""" Main module for pset_final demonstration

Educational example can best be executed via:

    Build the model:
    > python -m pset_final --training --file wine_quality_1.parquet
    
    Confirm inference on the same data-set:
    > python -m pset_final --inference --file wine_quality_1.parquet

    Confirm failure on the new data-set:
    > python -m pset_final --inference --file wine_quality_2.parquet

"""

import argparse

import luigi

from pset_final.tasks.data import DownloadTrainingDataTask
from pset_final.tasks.inference import XGBoostInferenceTask
from pset_final.tasks.learn import XGBoostLearnerTask
from pset_final.tasks.validation import GreatExpectationValidationTask

# Specify arguments for pipeline execution
#   inference = run the inference (scoring) pipeline
#   training  = run the training (model build) pipeline

parser = argparse.ArgumentParser()
parser.add_argument(
    "-t", "--training", help="Execute training pipeline", action="store_true"
)

parser.add_argument(
    "-i", "--inference", help="Execute inference pipeline", action="store_true"
)

parser.add_argument(
    "-f",
    "--file",
    help="Wine quality data-set (e.g. wine_quality_1.parquet)",
    required=True,
)

parser.add_argument(
    "--skip-data-validation",
    help="Disable use of Great Expectations to validate data",
    action="store_true",
)

args = parser.parse_args()


def main():

    tasks = []

    if args.training:
        tasks.append(
            XGBoostLearnerTask(
                data_file=args.file, skip_validation=args.skip_data_validation
            )
        )

    if args.inference:
        tasks.append(
            XGBoostInferenceTask(
                data_file=args.file, skip_validation=args.skip_data_validation
            )
        )

    luigi.build(tasks, local_scheduler=True)


if __name__ == "__main__":
    main()
