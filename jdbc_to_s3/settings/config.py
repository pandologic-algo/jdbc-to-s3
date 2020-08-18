import os
import json


class Config:
    def __init__(self, spark_config, db_config):
        # db config
        self.db_config = db_config
        
        # spark config
        self.spark_config = spark_config
