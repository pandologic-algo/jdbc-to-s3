import logging

# 3rd party
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

# internal
from ..settings import secrets


class SparkHandler:
    def __init__(self, partition_size=5*(10**6), fetch_size=20000, spark_jars=None, db_config=None, logger=None):
        """[summary]

        Args:
            partition_size ([type], optional): [description]. Defaults to 5*(10**6).
            fetch_size (int, optional): [description]. Defaults to 20000.
            spark_jars ([type], optional): [description]. Defaults to None.
            db_config ([type], optional): [description]. Defaults to None.
            logger ([type], optional): [description]. Defaults to None.
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
        """[summary]

        Args:
            table_name ([type]): [description]
            index_col ([type]): [description]

        Returns:
            [type]: [description]
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

        self._logger.info('{} - table=[{}] lazy read was done'.format(self._name, table_name))

        return df

    def write_df_to_s3(self, df, bucket_key, partition_cols=None, file_format='parquet', mode='append'):
        self._logger.info('{} - start writing DataFrame to bucket_key=[{}] in format=[{}], mode=[{}] and partition_cols=[{}]'.format(self._name,
            bucket_key,
            file_format, 
            mode,
            partition_cols))
        
        # check if session exists, if not resession
        self._resession_spark_session()

        writer = df.write.mode(mode).format(file_format)
        
        if partition_cols is not None:
            writer = writer.partitionBy(*partition_cols)
        
        try:
            writer.save("s3a://{}".format(bucket_key))
        except Exception as ex:
            self._logger.exception('{} - error in write table: {}'.format(self._name, ex))

        self._logger.info('{} - finished writing DataFrame'.format(self._name))

    def _init_spark_session(self):
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

        return spark_session

    def _init_db_spark_session_reader(self):
        reader = self._spark_session.read \
            .format(self._db_config.get('format')) \
            .option("url", self._db_config.get('url')) \
            .option("user", secrets.get('DB_USER')) \
            .option("password", secrets.get('DB_PASSWORD')) \
            .option("driver", self._db_config.get('driver')) 

        return reader

    def close_spark_session(self):
        """[summary]
        """
        try:
            self._spark_session.stop()
        except Exception as ex:
            self._logger.exception('{} - could not stop spark session: {}'.format(self._name, ex))

    def _resession_spark_session(self, attempts=5):
        """[summary]

        Args:
            attempts (int, optional): [description]. Defaults to 5.
        """
        if self._spark_session._jsc.sc().isStopped():
            for attempt in range(1, attempts + 1):
                try:
                    self._logger.info('{} - spark session is stopped creating new session'.format(self._name))

                    self._spark_session = self._init_spark_session()

                    self._logger.info('{} - new spark session was created'.format(self._name))

                    break

                except Exception as ex:
                    self._logger.exception('{} - attempt {} could resession spark session: {}'.format(self._name, attempt, ex))

                    if attempt < attempts:
                        time.sleep(60)
        
    def _spark_read_table_index_min_max(self, index_col, table_name):
        """[summary]

        Args:
            index_col ([type]): [description]
            table_name ([type]): [description]

        Returns:
            [type]: [description]
        """
        # defaults
        min_index, max_index = None, None

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

        self._logger.info('{} - table=[{}] bounderies: min_index=[{}] and max_index=[{}]'.format(self._name,
            table_name,
            min_index,
            max_index))

        return min_index, max_index

    

