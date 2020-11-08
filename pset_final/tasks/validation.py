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

    # Location for data to validate:
    data_dir = luigi.Parameter(default="data")
    data_sub_dir = luigi.Parameter(default="training")
    data_file = luigi.Parameter(default="wine_quality_1.parquet")

    # Data source name:
    datasource_name = luigi.Parameter(default="sample_data")

    # Location to store output:
    validation_dir = luigi.Parameter(default="data/validation")
    validation_sub_dir = luigi.Parameter(default="downloaded_data")

    def requires(self):
        return DownloadTrainingDataTask()

    def output(self):
        return SuffixPreservingLocalTarget(
            os.path.join(self.validation_dir, self.datasource_name, "_SUCCESS.txt")
        )

    def run(self):
        context = ge.data_context.DataContext()

        if self.datasource_name not in context.list_expectation_suite_names():
            raise ValueError("Unknown data_source: " + self.datasource_name)

        data_file_path = os.path.join(self.data_dir, self.data_sub_dir, self.data_file)
        # If you would like to validate a file on a filesystem:
        batch_kwargs = {"path": data_file_path, "datasource": self.datasource_name}

        batch = context.get_batch(batch_kwargs, self.datasource_name)

        run_id = {
            "run_name": self.datasource_name
            + "-"
            + self.validation_sub_dir
            + "_validation",  # insert your own run_name here
            "run_time": datetime.datetime.now(datetime.timezone.utc),
        }

        results = context.run_validation_operator(
            "action_list_operator", assets_to_validate=[batch], run_id=run_id
        )

        if not results["success"]:
            raise ExpectationsValidationError(
                "Validation of the source data is not successful "
            )

        with self.output().open("w") as w:
            w.write(str(run_id))
