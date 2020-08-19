import os
import sys
import json

# add current path
sys.path.append(os.getcwd())

# 3rd party
import pytest

# internal 
from spark_jdbc_handler.etl import Exporter

CONFIG_PATH = 'utils/config.json'


def _load_config(path):
    # load config file
    with open(path, 'r') as fp:
        config = json.load(fp)
        
    return config


@pytest.fixture
def my_etl():
    config = _load_config(CONFIG_PATH) 

    spark_config = config.get('spark_config')
    db_config = config.get('db_config')

    return Exporter(spark_config=spark_config, db_config=db_config)


def test_run(my_etl):
    config = _load_config(CONFIG_PATH) 

    etl_task = config.get('etl_tasks')[0]

    assert my_etl.execute(etl_task) == True
