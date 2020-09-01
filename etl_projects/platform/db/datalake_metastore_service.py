class DatalakeMetastoreService:
    @staticmethod
    def get_db_info(source, bucket):

        # TODO: Forno is under new AWS accounts, then use new structure
        # Prod is temporarily under old AWS account and will be migrated soon, then this
        # if clause should be removed
        schema_suffix = ""

        spark_db_infos = {
            "db_raw_databricks": f"datalake_{source}_raw",
            "db_clean_databricks": f"datalake_{source}_clean",
            "db_enrich_databricks": f"datalake_{source}",
            "db_clean_staging_databricks": f"datalake_{source}_clean_staging",
        }

        athena_db_infos = {
            "db_raw_athena": f"datalake_{source}_raw{schema_suffix}",
            "db_clean_athena": f"datalake_{source}_clean{schema_suffix}",
            "db_enrich_athena": f"datalake_{source}{schema_suffix}",
            "db_clean_staging_athena": f"datalake_{source}_clean_staging{schema_suffix}",
        }

        s3_infos = {
            "db_raw_path": f"s3://{bucket}/raw/{source}/",
            "db_clean_path": f"s3://{bucket}/clean/{source}/",
            "db_enrich_path": f"s3://{bucket}/enrich/{source}/",
            "db_clean_staging_path": f"s3://{bucket}/clean_staging/{source}/",
        }

        db_infos = {}
        db_infos.update(spark_db_infos)
        db_infos.update(athena_db_infos)
        db_infos.update(s3_infos)
        return db_infos

    @staticmethod
    def get_layer_info(source, bucket, layer):
        """
        Return specified layer info
        :param source: source or context name in metastore
        :param bucket: datalake bucket in S3
        :param layer: raw, clean, enrich or clean_staging layers
        :return: specified layer info
        """
        db_info = DatalakeMetastoreService.get_db_info(source, bucket)

        database_name = db_info["db_" + layer + "_databricks"]
        database_location = db_info["db_" + layer + "_path"]
        athena_database_name = db_info["db_" + layer + "_athena"]

        return database_name, database_location, athena_database_name
