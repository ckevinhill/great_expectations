import luigi
from csci_utils.io.luigi import S3DownloadFileTask


class DownloadTrainingDataTask(S3DownloadFileTask):
    s3_path = luigi.Parameter(default="pset_final/data")
    s3_file = luigi.Parameter(default="wine_quality_1.parquet")

    download_root_dir = luigi.Parameter(default="data")
    download_sub_path = luigi.Parameter(default="training")
