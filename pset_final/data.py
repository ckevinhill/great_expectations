import luigi
from csci_utils.io.luigi import S3FileExistsTask


class S3TrainingDataTask(S3FileExistsTask):
    s3_path = luigi.Parameter(default="/pset_final/data/")
