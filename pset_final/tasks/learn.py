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


class XGBoostLearnerTask(Task):

    # Location for data to validate:
    data_dir = luigi.Parameter(default="data")
    data_sub_dir = luigi.Parameter(default="training")
    data_file = luigi.Parameter(default="wine_quality_1.parquet")

    # Location to store output:
    model_dir = luigi.Parameter(default="data")
    model_sub_dir = luigi.Parameter(default="saved_models")
    model_file = luigi.Parameter(default="wine_quality.dat")

    training_split = luigi.FloatParameter(default=0.25)

    def requires(self):
        return {
            "data": DownloadTrainingDataTask(s3_file=self.data_file),
            "validation": GreatExpectationValidationTask(data_file=self.data_file),
        }

    def output(self):
        return LocalTarget(
            os.path.join(self.model_dir, self.model_sub_dir, self.model_file),
            format=luigi.format.Nop,
        )

    def run(self):

        f = self.input()["data"].open("r")
        df = pd.read_parquet(f)

        # Create Classification version of target variable
        df["goodquality"] = [1 if x >= 7 else 0 for x in df["quality"]]

        # Numerical encoding of categorical feature:
        lbl = preprocessing.LabelEncoder()
        df["type"] = lbl.fit_transform(df["type"].astype(str))

        # Separate feature variables and target variable
        X = df.drop(["quality", "goodquality"], axis=1)
        y = df["goodquality"]

        # Normalize feature variables
        X = StandardScaler().fit_transform(X)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.training_split, random_state=0
        )

        model = xgb.XGBClassifier(random_state=1)
        model.fit(X_train, y_train)

        # TODO: Could add validation output

        with self.output().open("wb") as w:
            pickle.dump(model, w)
