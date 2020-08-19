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
        """Given a spark Dataframe [df] and defined [partition_col], the Processor create new columns for future DataFrame
        write operation with partition columns schema.
        The processor check if the [partition_col] is a Datetime data dtype, and create ['year', 'month', 'day'] columns.
        Else, if it's not a Datetime column it will use the partition column as it is.

        Args:
            df ([DataFrame]): spark DataFrame.
            partition_col ([str], optional): Column to use as partition column. Defaults to None.

        Returns:
            [tuple: (DataFrame, list)]: return a tuple of the DataFrame after being processed and the created/configured 
                partition columns list. 
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
        """Static method to create new spark DataFrame column to Datetime column type.

        Args:
            df ([DataFrame]): input spark DataFrame.
            partition_col ([str]): spark DataFrame partition column name.

        Returns:
            [DataFrame]: processed spark DataFrame
        """
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
        """Given columns to drop, the Processor drop the columns from the spark DataFrame.

        Args:
            df ([DataFrame]): input spark DataFrame.
            cols_to_rm ([list], optional): list of the columns names. Defaults to None.

        Returns:
            [DataFrame]: spark DataFrame after column are removed.
        """
        if cols_to_rm is not None:
            df = df.drop(*tuple(cols_to_rm))

        return df


processors = dict(
        PartitionColsProcessor=PartitionColsProcessor,
        ColsDropperProcessor=ColsDropperProcessor
    )


class SparkProcessorPipeline:
    def __init__(self, logger=None):
        """Processors Pipeline, which will process spark DataFrame due to give pipeline steps.

        Args:
            logger ([Logger], optional): Logger instance. Defaults to None.
        """
        # possible pipeline processors mapping
        self._processors = processors     
        
        self._logger = logger if logger else logging.getLogger(__name__.split('.')[0])

        self._name = self.__class__.__name__

    def process_data(self, df, pipeline, **kwargs):
        """Iterate input pipeline and perform Processors steps.

        Args:
            df ([DataFrame]): input spark DataFrame
            pipeline ([list]): list of the pipeline Processors steps.

        Returns:
            [tuple: (DataFrame, list)]: return a tuple of the DataFrame after being processed and the created/configured 
                partition columns list (can be None).
        """
        self._logger.info('{} - start pipeline: {}'.format(self._name, pipeline))

        partition_cols = None

        for step in pipeline:
            # step processor
            processor = self._processors.get(step)

            # dataframe
            df = processor.process_data(df, **kwargs)

            # special processor
            if processor.__name__ == 'PartitionColsProcessor':
                df, partition_cols = df

                self._logger.info('{} - step {} partition cols generated: {}'.format(self._name, step, partition_cols))

            self._logger.info('{} - step=[{}] ended'.format(self._name, step))

        self._logger.info('{} - pipeline was ended'.format(self._name))

        return df, partition_cols
