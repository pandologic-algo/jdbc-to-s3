import os
import sys
import json

# add current path
sys.path.append(os.getcwd())

# 3rd party
import pytest

# internal 
from jdbc_to_s3 import Exporter

CONFIG_PATH = 'utils/config.json'


def _load_config(path):
    # load config file
    with open(path, 'r') as fp:
        config = json.load(fp)
        
    return config


@pytest.fixture
def my_etl():
    config = _load_config(CONFIG_PATH) 

    return Exporter(**config)


def test_run(my_etl):
    assert my_etl.run() == True
