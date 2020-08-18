import logging

# 3rd party
from pyspark.sql.functions import col, year, month, dayofmonth
from pyspark.sql.types import DateType, TimestampType


DATE_PARTITION_COLS = ['year', 'month', 'day']
DATE_TIME_TYPES = (DateType, TimestampType)

__all__ = ['SparkProcessorPipeline']


class PartitionColsProcessor:

    @classmethod
    def process_data(cls, df, partition_col=None, **kwargs):
        """[summary]

        Args:
            df ([type]): [description]
            partition_col ([type], optional): [description]. Defaults to None.

        Returns:
            [type]: [description]
        """
        partition_cols = None

        if partition_col is not None:
            partition_col_dtype = df.schema[partition_col].dataType

            # check if partition col is date type
            is_date_partition_col = isinstance(partition_col_dtype, DATE_TIME_TYPES)

            if is_date_partition_col:
                # add date partition cols
                df = cls._process_date_partition(df, partition_col)

            # partition_cols
            partition_cols = DATE_PARTITION_COLS if is_date_partition_col else list(partition_col)

        return df, partition_cols

    @staticmethod
    def _process_date_partition(df, partition_col):
        # year
        df = df.withColumn('year', year(col(partition_col)))

        # month
        df = df.withColumn('month', month(col(partition_col)))

        # day
        df = df.withColumn('day', dayofmonth(col(partition_col)))

        return df


class ColsDropperProcessor:
    @staticmethod
    def process_data(df, cols_to_rm=None, **kwargs):
        if cols_to_rm is not None:
            df = df.drop(*tuple(cols_to_rm))

        return df


processors = dict(
        PartitionColsProcessor=PartitionColsProcessor,
        ColsDropperProcessor=ColsDropperProcessor
    )


class SparkProcessorPipeline:
    def __init__(self, logger=None):
        self._processors = processors     
        
        self._logger = logger if logger else logging.getLogger(__name__.split('.')[0])

        self._name = self.__class__.__name__

    def process_data(self, df, pipeline, **kwargs):
        self._logger.info('{} - start pipeline: {}'.format(self._name, pipeline))

        partition_cols = None

        for step in pipeline:
            # step processor
            processor = self._processors.get(step)

            # dataframe
            df = processor.process_data(df, **kwargs)

            if processor.__name__ == 'PartitionColsProcessor':
                df, partition_cols = df

                self._logger.info('{} - step {} partition cols generated: {}'.format(self._name, step, partition_cols))

            self._logger.info('{} - step=[{}] ended'.format(self._name, step))

        self._logger.info('{} - pipeline was ended'.format(self._name))

        return df, partition_cols
