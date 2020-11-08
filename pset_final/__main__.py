import luigi

from pset_final.tasks.data import DownloadTrainingDataTask
from pset_final.tasks.learn import XGBoostLearnerTask
from pset_final.tasks.validation import GreatExpectationValidationTask


def main():

    #    t = XGBoostLearnerTask(
    #        data_file="wine_quality_1.parquet",
    #        target_column="x",
    #        feature_columns="y",
    #        model_file="model.dat",
    #    )

    t = XGBoostLearnerTask()
    luigi.build([t], local_scheduler=True)


if __name__ == "__main__":
    main()
