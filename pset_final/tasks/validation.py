import datetime
import os

import great_expectations as ge
import luigi
from csci_utils.io.luigi import SuffixPreservingLocalTarget
from luigi import Task
from pset_final.tasks.data import DownloadTrainingDataTask


class ExpectationsValidationError(Exception):
    pass


class GreatExpectationValidationTask(Task):
    """A Task that executes a GreatExpectations Validation
    Suite on a specified data-set."""

    # Location for data to validate:
    data_dir = luigi.Parameter(default="data")
    data_sub_dir = luigi.Parameter(default="training")
    data_file = luigi.Parameter(default="wine_quality_1.parquet")

    # Data source name:
    datasource_name = luigi.Parameter(default="data_ds")

    # Location to store output:
    validation_dir = luigi.Parameter(default="data/validation")
    validation_sub_dir = luigi.Parameter(default="inference_data")

    def requires(self):
        return DownloadTrainingDataTask(s3_file=self.data_file)

    def output(self):
        return SuffixPreservingLocalTarget(
            os.path.join(
                self.validation_dir,
                self.datasource_name,
                self.data_dir,
                self.data_sub_dir,
                "_SUCCESS.txt",
            )
        )

    def run(self):
        """Executes a specified Great Expectations validation suite for a
        specified data source."""

        # Get a Great Expectations context:
        context = ge.data_context.DataContext()

        # Guard-statement: Check that GE data-source is valid:
        if self.datasource_name not in context.list_expectation_suite_names():
            raise ValueError("Unknown data_source: " + self.datasource_name)

        data_file_path = os.path.join(self.data_dir, self.data_sub_dir, self.data_file)
        # If you would like to validate a file on a filesystem:
        batch_kwargs = {"path": data_file_path, "datasource": self.datasource_name}

        batch = context.get_batch(batch_kwargs, self.datasource_name)

        # Create a run identifier that is meaningfully related to the pipeline execution:
        run_id = {
            "run_name": self.datasource_name
            + "-"
            + self.validation_sub_dir
            + "_validation",  # insert your own run_name here
            "run_time": datetime.datetime.now(datetime.timezone.utc),
        }

        # Validate data batch vs. expectation suite.  Using run_validation_operator
        # instead of batch.validate() to invoke data docs update operations.

        results = context.run_validation_operator(
            "action_list_operator", assets_to_validate=[batch], run_id=run_id
        )

        if not results["success"]:

            validation_results = results.list_validation_results()[0]

            # Output information about failured expectations to help with debugging:
            print(
                "Success validation percent: "
                + str(validation_results.statistics["success_percent"])
            )

            for val_result in validation_results.results:
                if not val_result.success:
                    expectation = val_result.expectation_config
                    print(
                        "Failed: ("
                        + expectation.kwargs["column"]
                        + ") "
                        + expectation.expectation_type
                    )

            raise ExpectationsValidationError(
                "Validation of the source data is not successful "
            )

        with self.output().open("w") as w:
            w.write(str(run_id))
