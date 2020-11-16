import logging

# internal
from .logging import create_default_logger
from .settings import Config, secrets
from .data_handling import SparkHandler, SparkProcessorPipeline


class Exporter:

    def __init__(self, spark_config, db_config, logger=None):
        """[summary]

        Args:
            spark_config ([dict]): SparkHandler config:
            {
                "spark_jars": [
                    "path/to/jar.jar"
                ]
            }
            db_config ([dict]): database connection config:
            {
                "format": "jdbc",
                "url": "jdbc:sqlserver://localhost:1433",
                "database": "database_name",
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }
            logger ([Logger], optional): package logger. Defaults to None.
        """
        # config
        self._config = Config(spark_config, db_config)

        # class name
        self._name = self.__class__.__name__

        # logger
        self._logger = logger if logger else create_default_logger()

        # spark handler
        self._spark_handler = SparkHandler(db_config=self._config.db_config, logger=self._logger, **self._config.spark_config)

        # spark processor
        self._spark_processor = SparkProcessorPipeline(logger=self._logger)

    def execute(self, table_task):
        """Execute a full ETL process on a table task:
        1. Get task params.
        2. Read table from database using jdbc driver and SparkHandler (SparkSession) and return a spark DataFrame.
        3. Process spark DataFrame according to defined task processing_args and the defined pipeline.
        4. Write Processed spark DataFrame to s3 path.

        Args:
            table_task ([dict]): user  defined task:
            {
            "task_name": "task_name",
            "table_name": "dbo.table_name",
            "index_col": "Id",
            "bucket_key": "buckt_name/key",
            "processing_args": {
                "pipeline": [
                    "PartitionColsProcessor",
                    "ColsDropperProcessor"
                ],
                "partition_col": "PartitionCol",
                "cols_to_rm": ["Id"]
            },
            "file_format": "parquet",
            "mode": "append",
            "partition_size: 100,
            "fetch_size: 100
        }

        Returns:
            [bool]: If execution succeed return True.
        """

        self._logger.info('{} - start etl task=[{}]'.format(self._name, table_task.get('task_name')))

        # table task params
        table_name = table_task.get('table_name')
        
        # index column
        index_col = table_task.get('index_col', 'Id')

        # bucket key
        bucket_key = table_task.get('bucket_key')

        # processing args
        processing_args = table_task.get('processing_args')

        # save format
        file_format = table_task.get('file_format', 'parquet')

        # read params
        partition_size = table_task.get('partition_size', 100*(10**3))
        fetch_size = table_task.get('fetch_size', 20*(10**3))

        # write mode
        write_mode = table_task.get('mode', 'append')

        # spark dataframe
        spark_df = self._spark_handler.read_table(table_name, index_col, partition_size, fetch_size)

        # spark dataframe with processing steps
        spark_processed_df, partition_cols = self._spark_processor.process_data(spark_df, **processing_args)

        # write spark dataframe
        self._spark_handler.write_df_to_s3(spark_processed_df, bucket_key, partition_cols=partition_cols, file_format=file_format, mode=write_mode)

        self._logger.info('{} - finished etl task=[{}]'.format(self._name, table_task.get('task_name')))

        return True

    def shutdown(self):
        """shutdown spark handler (SparkSession).
        """
        self._spark_handler.shutdown()
