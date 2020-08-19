import logging

# internal
from .logging import create_default_logger
from .settings import Config, secrets
from .data_handling import SparkHandler, SparkProcessorPipeline


class Exporter:

    def __init__(self, spark_config, db_config, logger=None):
        """[summary]

        Args:
            etl_tasks ([type]): [description]
            spark_config ([type]): [description]
            db_config ([type]): [description]
            logger ([type], optional): [description]. Defaults to None.
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
        """[summary]

        Args:
            table_task ([type]): [description]

        Returns:
            [type]: [description]
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

        # write mode
        write_mode = table_task.get('mode', 'append')

        # spark dataframe
        spark_df = self._spark_handler.read_table(table_name, index_col)

        # spark dataframe with processing steps
        spark_processed_df, partition_cols = self._spark_processor.process_data(spark_df, **processing_args)

        # write spark dataframe
        self._spark_handler.write_df_to_s3(spark_processed_df, bucket_key, partition_cols=partition_cols, file_format=file_format, mode=write_mode)

        self._logger.info('{} - finished etl task=[{}]'.format(self._name, table_task.get('task_name')))

        return True

    def shutdown(self):
        self._spark_handler.shutdown()

