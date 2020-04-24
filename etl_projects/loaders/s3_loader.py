from etl_projects.base.spark.base_spark import BaseSparkContext


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
