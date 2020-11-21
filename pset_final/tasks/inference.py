import os
import pickle

import luigi
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

    def requires(self):
        return {
            "data": DownloadTrainingDataTask(
                download_sub_path=self.data_sub_dir, s3_file=self.data_file
            ),
            "validation": GreatExpectationValidationTask(
                data_sub_dir=self.data_sub_dir, data_file=self.data_file
            ),
        }

    def output(self):
        return LocalTarget(
            os.path.join(self.results_dir, self.results_sub_dir, self.results_file),
            format=luigi.format.Nop,
        )

    def run(self):

        f = self.input()["data"].open("r")
        df = pd.read_parquet(f)

        # Numerical encoding of categorical feature:
        lbl = preprocessing.LabelEncoder()
        df["type"] = lbl.fit_transform(df["type"].astype(str))

        # Separate feature variables and target variable
        X = df.drop(["quality", "goodquality"], axis=1)
        y = df["goodquality"]

        # Normalize feature variables
        X = StandardScaler().fit_transform(X)

        loaded_model = pickle.load(open(self.model_file, "rb"))
        result = loaded_model.score(X)

        # with self.output().open("wb") as w:
        #     pickle.dump(model, w)
