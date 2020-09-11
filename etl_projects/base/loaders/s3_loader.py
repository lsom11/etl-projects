import logging
import json
import boto3
from botocore.exceptions import ClientError
from etl_projects.platform.processors.spark import BaseSparkContext

class S3Loader:
    """
    Loads SparkDataFrames into S3
    """

    # todo:  check this value and argue the choice
    MAX_RECORDS_PER_FILE = 250000

    def load_table(
        self,
        df,
        database_name,
        table_name,
        format_options,
        database_location,
        partitions=[],
        **options
    ):

        if not df:
            raise ValueError("m=load_table, msg=Spark DataFrame is empty")
        s3_path = database_location + table_name
        df_writer = (
            df.write.mode("overwrite")
            .format(format_options)
            .option("maxRecordsPerFile", self.MAX_RECORDS_PER_FILE)
        )
        if partitions:
            df_writer = df_writer.partitionBy(*partitions)

        for op, val in options.items():
            df_writer = df_writer.option(op, val)

        df_writer.save(path=s3_path)


    @staticmethod
    def upload_file(json_data, bucket, file_name):
        s3_resource = boto3.resource('s3')
        s3_object = s3_resource.Object(bucket, f'{file_name}.json')

        s3_object.put(
            Body=(bytes(json.dumps(json_data).encode('UTF-8')))
        )
