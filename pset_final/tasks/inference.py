import os
import pickle

import luigi
import numpy as np
import pandas as pd
import xgboost as xgb
from luigi import LocalTarget, Task
from pset_final.tasks.data import DownloadTrainingDataTask
from pset_final.tasks.validation import GreatExpectationValidationTask
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


class XGBoostInferenceTask(Task):

    # Location for data to validate:
    data_dir = luigi.Parameter(default="data")
    data_sub_dir = luigi.Parameter(default="inference")
    data_file = luigi.Parameter(default="wine_quality_2.parquet")

    # Location of model:
    model_file = luigi.Parameter(default="saved_models/wine_quality.dat")

    # Location to store output:
    results_dir = luigi.Parameter(default="data")
    results_sub_dir = luigi.Parameter(default="scored_results")
    results_file = luigi.Parameter(default="scores.csv")

    skip_validation = luigi.BoolParameter(default=False)

    def requires(self):
        return {
            "data": DownloadTrainingDataTask(
                download_sub_path=self.data_sub_dir, s3_file=self.data_file
            ),
            "validation": GreatExpectationValidationTask(
                data_sub_dir=self.data_sub_dir,
                data_file=self.data_file,
                skip_validation=self.skip_validation,
            ),
        }

    def output(self):
        data_file = str(self.data_file).replace(".", "_")

        return LocalTarget(
            os.path.join(
                self.results_dir, self.results_sub_dir, data_file, self.results_file
            ),
            format=luigi.format.Nop,
        )

    def run(self):

        f = self.input()["data"].open("r")
        df = pd.read_parquet(f)

        # Numerical encoding of categorical feature:
        lbl = preprocessing.LabelEncoder()
        df["type"] = lbl.fit_transform(df["type"].astype(str))

        # Separate feature variables and target variable
        X = df.drop(["quality"], axis=1)

        # Normalize feature variables
        X = StandardScaler().fit_transform(X)

        loaded_model = pickle.load(
            open(os.path.join(self.data_dir, self.model_file), "rb")
        )

        # Created predicted values:
        y_pred = loaded_model.predict(X)

        # Output scores to csv:
        os.makedirs(os.path.dirname(self.output().path))
        np.savetxt(self.output().path, y_pred, delimiter=",")
