# Introduction
The Following package include a simplier API to pyspark and Spark Framework.
The API include 3 simple operations:
1. __Read__ - a simple functionality for handling the read operation from an external database. The read operation is done using a JDBC driver and a SparkSession found in the package SparkHandler class.
2. __Write__ - support write to S3 path, found in the package SparkHandler class.
3. __Export__ - combined the 2 previous operations to a pipeline found in the package Exporter class.


Further, the package has a processors module which include Processors
which can perform basic processing steps on spark dataframe before saving to s3 path.

# Getting Started


## 1. Installation process

stable:

`pip install git+https://github.com/pandologic-algo/spark-jdbc-handler.git`

dev:

`pip install git+https://github.com/pandologic-algo/spark-jdbc-handler.git@dev`

specific version:

```pip install git+https://github.com/pandologic-algo/spark-jdbc-handler.git@v<x.x.x>```

## 2. Config

### 2.1 SparkHandler
config params:
- __partition_size__ ([int], optional): size of each read partition from the database. Defaults to 5*(10**6). From that parameter combined with other we determine the numPartitions of SparkSession JDBC read.
- __fetch_size__ ([int], optional): SparkSession JDBC read fetchsize parameter. Defaults to 20000.
- __spark_jars__ ([list], optional): paths to jdbc jars. Defaults to None.
- __db_config__ ([dict], optional): database connetion parameters. Defaults to None.
- __logger__ ([Logger], optional): logger instance for logging package process. Defaults to None.

### 2.2 Exporter
config params:
- __spark_config__ ([dict]): SparkHandler config:
```
{
    "spark_jars": [
        "path/to/jar.jar"
    ],
    "partition_size": 5000000,
    "fetch_size": 20000
}
```
__db_config__ ([dict]): database connection config:
```
{
    "format": "jdbc",
    "url": "jdbc:sqlserver://localhost:1433",
    "database": "database_name",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
```
__logger__ ([Logger], optional): package logger. Defaults to None.

### 2.3 task config
A task is sent to Exporter and executed by execute() method.
Task sturcture:
```
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
    "mode": "append"
}
```

### 2.4 Secrets config
Secrets can be injected from the env vars or by creating in the cwd path .env file (dotenv package will load them).

The needed env vars for the secrets:
``` 
DB_USER=db_user
DB_PASSWORD=db_password
AWS_ACCESS_KEY_ID=access_key
AWS_SECRET_KEY_ID=secret_key
```

## 4. Use
Using a SparkHandler:
``` python
from spark_jdbc_handler.data_handling import SparkHandler

# handler
spark_handler = SparkHandler(partition_size=5*(10**6), fetch_size=20000,   spark_jars=None, db_config=None, logger=None)

# read table
df = spark_handler.read_table(table_name='some_table', index_col='Id')

# write to s3 
spark_handler.write_df_to_s3(df=df, bucket_key='my-bucket/key',         partition_cols=['CompanyId'], file_format='parquet', mode='append')

# shutdown handler
spark_handler.shutdown()
```

Using SparkProcessorPipeline:
SparkProcessorPipeline has processors which contains Processors steps.
Current Supported  Proccessors:
1. _PartitionColsProcessor_ - add columns to dataframe to be the future partition columns. Specifically, knows to detect if input column is datetime and create "year", "month" and "day" column for future write partition.
2. _ColsDropperProcessor_ - drop columns from dataframe.

``` python
from spark_jdbc_handler.data_handling import SparkProcessorPipeline

processor = SparkProcessorPipeline()

# params
pipeline = ["PartitionColsProcessor", "ColsDropperProcessor"]
partition_col = "PartitionCol"
cols_to_rm = ["Id"]

# process df, but not executed yet as spark has lazy execution mechanism
processed_df = processor.process_data(df, pipeline=pipeline,    partition_col=partition_col,cols_to_rm=cols_to_rm)

```

Using Exporter:
``` python
from spark_jdbc_handler.etl import Exporter

spark_config = {
    "spark_jars": [
        "jars/mssql-jdbc-6.1.0.jre8.jar"
    ],
    "partition_size": 5000000,
    "fetch_size": 20000
}

db_config = {
    "format": "jdbc",
    "url": "jdbc:sqlserver://localhost:1433",
    "database": "test",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

task = {
    "task_name": "task",
    "table_name": "dbo.test",
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
    "mode": "append"
}

# exporter 
exporter = Exporter(spark_config=spark_config, db_config=db_config)

# execute task
is_success = False
try:
    is_success = exporter.execute(etl_task)
except:
    pass
print('succeed={}'.format(is_success))

# shutdown handler
exporter.shutdown()
```


## 5. Future

- Extend Processors types.