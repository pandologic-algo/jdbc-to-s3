import logging
import time

# 3rd party
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

# internal
from ..settings import secrets


class SparkHandler:
    def __init__(self, partition_size=5*(10**6), fetch_size=20000, spark_jars=None, db_config=None, logger=None):
        """SparkHandler contains simple API to spark cluster. 
        It can initialize spark cluster by creating a SparkSession.
        The 3 public class functionalities are:
        - read_table : read a table given a table_name and index column. 
        - write_df_to_s3 : write spark df to s3 path
        - shutdown : shutdown the SparkSession

        Args:
            partition_size ([int], optional): size of each read partition from the database. Defaults to 5*(10**6). From that parameter combined with other 
                we determine the numPartitions of SparkSession JDBC read.
            fetch_size ([int], optional): SparkSession JDBC read fetchsize parameter. Defaults to 20000.
            spark_jars ([list], optional): paths to jdbc jars. Defaults to None.
            db_config ([dict], optional): database connetion parameters. Defaults to None.
            logger ([Logger], optional): logger instance for logging package process. Defaults to None.
        """
        self._name = self.__class__.__name__

        self._partition_size = partition_size

        self._fetch_size = fetch_size

        self._logger = logger if logger else logging.getLogger(__name__.split('.')[0])

        self._spark_jars = spark_jars

        self._db_config = db_config

        # spark session
        self._spark_session = self._init_spark_session()

    def read_table(self, table_name, index_col):
        """Public method for reading a table using SparkSession. The reading spark params are derived from user configuration and table status:
        - partitionColumn: user config
        - lowerBound: read from database table according to [index_col]="partitionColumn"
        - upperBound: read from database table according to [index_col]="partitionColumn"
        - numPartitions: derived from "lowerBound", "upperBound" and [self._partition_size] values.
        - fetchsize: user config
        More info on these params can be found here - https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html.
        Args:
            table_name ([str]): table database name, inluded with the table schema (e.g.: "dbo.test"). 
                Also possible to pass a sub-query, e.g: "(select top 1000 * from dbo.test) as t"
            index_col ([str]): named index column for spark parallelism.

        Returns:
            [DataFrame]: spark DataFrame
        """
        # defaults
        df = None

        min_index, max_index = self._spark_read_table_index_min_max(index_col, table_name)

        # check if session exists, if not resession
        self._resession_spark_session()

        # reader
        reader = self._init_db_spark_session_reader()

        # num of partitions
        num_partitions = max(int((max_index - min_index + 1)/self._partition_size), 1)

        try:
            df = reader\
                .option("dbtable", table_name) \
                .option("partitionColumn", index_col) \
                .option("lowerBound", str(min_index)) \
                .option("upperBound", str(max_index)) \
                .option("numPartitions", str(num_partitions)) \
                .option("fetchsize", str(self._fetch_size)) \
                .load()
        except Exception as ex:
            self._logger.exception('{} - error in read table=[{}]: {}'.format(self._name, table_name, ex))

            self._close_spark_session()

            raise Exception('Could not read Dataframe')

        self._logger.info('{} - table=[{}] lazy read was done'.format(self._name, table_name))

        return df

    def write_df_to_s3(self, df, bucket_key, partition_cols=None, file_format='parquet', mode='append'):
        """Write spark Dataframe to s3 path.

        Args:
            df ([DataFrame]): spark DataFrame read from database.
            bucket_key ([str]): s3 save path. E.g.: "<bucket_name>/<key_in_bucket>"
            partition_cols ([list], optional): List of the DataFrame column to be use for partitioning when saving. Defaults to None.
            file_format ([str], optional): file format for saaving the DataFrame. Defaults to 'parquet'.
            mode ([str], optional): spark DataFrame write mode. Defaults to 'append'.

        Raises:
            Exception: if write wasn't successfull raise general Exception.
        """
        self._logger.info('{} - start writing DataFrame to bucket_key=[{}] in format=[{}], mode=[{}] and partition_cols=[{}]'.format(self._name,
            bucket_key,
            file_format, 
            mode,
            partition_cols))
        
        # check if session exists, if not resession
        self._resession_spark_session()

        # writer
        writer = df.write.mode(mode).format(file_format)
        
        # partition cols
        if partition_cols is not None:
            writer = writer.partitionBy(*partition_cols)
        
        try:
            writer.save("s3a://{}".format(bucket_key))

        except Exception as ex:
            self._logger.exception('{} - error in write table: {}'.format(self._name, ex))

            self._close_spark_session()

            raise Exception('Could not write Dataframe to s3 path')

        self._logger.info('{} - finished writing DataFrame'.format(self._name))

    def shutdown(self):
        """Public method of shutdowning SparkSession
        """
        self._close_spark_session()
        
    def _init_spark_session(self):
        """Initialize the SparkSession. Including passing to session JDBC jars and AWS access key and token for the s3 writing operation.
        The AWS params are passed from the secrets module and loaded from the env vars. Make sure the user related to those key has the right 
        s3 saving permissions.

        Raises:
            Exception: If session couldn't be created.

        Returns:
            [SparkSession]: SparkSession object. 
        """
        try:
            spark_session = SparkSession\
                .builder\

            if self._spark_jars:
                # add spark jars
                spark_jars_concated = ','.join(self._spark_jars)
                spark_session = spark_session\
                    .config('spark.jars', spark_jars_concated)
            
            # AWS cardentials
            spark_session = spark_session\
                .config('spark.hadoop.fs.s3a.access.key', secrets.get('AWS_ACCESS_KEY_ID'))\
                .config('spark.hadoop.fs.s3a.secret.key', secrets.get('AWS_SECRET_KEY_ID'))\
            
            # create session
            spark_session = spark_session.getOrCreate()

        except Exception as ex:
            self._logger.exception('{} - could not init spark session: {}'.format(self._name, ex))

            self._close_spark_session()

            raise Exception('Could not load spark session')

        return spark_session

    def _init_db_spark_session_reader(self):
        """Create an abstract spark "reader" by population reader database params, jdbc driver class and format read.

        Raises:
            Exception: If couldn't initialize "reader".

        Returns:
            [DataFrameReader]: a partly configure spark DataFrameReader reader.
        """
        try:
            reader = self._spark_session.read \
                .format(self._db_config.get('format')) \
                .option("url", self._db_config.get('url')) \
                .option("user", secrets.get('DB_USER')) \
                .option("password", secrets.get('DB_PASSWORD')) \
                .option("driver", self._db_config.get('driver'))

        except Exception as ex:
            self._logger.exception('{} - could not init spark reader') 

            self._close_spark_session()

            raise Exception('Could not init spark reader')

        return reader

    def _close_spark_session(self):
        """Close spark session.
        """
        try:
            self._spark_session.stop()
        except Exception as ex:
            self._logger.exception('{} - could not stop spark session: {}'.format(self._name, ex))

    def _resession_spark_session(self, attempts=5):
        """If session is lost, try resession. It first checks that session is lost. If so, create new session and populate _spark_session SparkHandler
        instance variables.

        Args:
            attempts (int, optional): Number of retries to resession. Defaults to 5.
        """
        if self._spark_session._jsc.sc().isStopped():
            # try
            for attempt in range(1, attempts + 1):
                try:
                    self._logger.info('{} - spark session is stopped creating new session'.format(self._name))

                    # create new session
                    self._spark_session = self._init_spark_session()

                    self._logger.info('{} - new spark session was created'.format(self._name))

                    break

                except Exception as ex:
                    self._logger.exception('{} - attempt {} could resession spark session: {}'.format(self._name, attempt, ex))

                    if attempt < attempts:
                        # sleep
                        time.sleep(60)
                    else:

                        raise Exception('Could not resession spark session after {} attempts'.format(attempts))
        
    def _spark_read_table_index_min_max(self, index_col, table_name):
        """Get database table min and max index values. The values are read from the database  using the JDBC connection.
        These values are used later for reading the table and determine the "lowerBound" and "upperBound" spark JDBC read params.

        Args:
            index_col ([str]): table index column name.
            table_name ([str]): database table name.

        Returns:
            [tuple]: min, max values
        """
        # check if session exists, if not resession
        self._resession_spark_session()

        # reader
        reader = self._init_db_spark_session_reader()
        
        # qeury
        qry = 'select minIndex=min({0}), maxIndex=max({0}) from {1}'.format(index_col, table_name)
        
        try:
            dict_index_min_max = reader\
                .option("query", qry)\
                .load()\
                .collect()[0]\
                .asDict()
            
            min_index, max_index = dict_index_min_max.get('minIndex'), dict_index_min_max.get('maxIndex')

        except Exception as ex:
            self._logger.exception('{} - error in read table=[{}] index min-max values: {}'.format(self._name, table_name, ex))

            self._close_spark_session()

            raise Exception('Could not get table index min-max values')

        self._logger.info('{} - table=[{}] bounderies: min_index=[{}] and max_index=[{}]'.format(self._name,
            table_name,
            min_index,
            max_index))

        return min_index, max_index
