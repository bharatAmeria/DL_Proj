import sys

from src.exception import CustomException
from src.logger import logging

import os
from dotenv import load_dotenv
import pymongo
import certifi

load_dotenv()
ca = certifi.where()


class MongoDBClient:
    """
    Class Name :   export_data_into_feature_store
    Description :   This method exports the dataframe from mongodb feature store as dataframe

    Output      :   connection to mongodb database
    On Failure  :   raises an exception
    """
    client = None

    def __init__(self, database_name=os.getenv('DATABASE_NAME')) -> None:
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv('MONGODB_URL_KEY')
                if mongo_db_url is None:
                    raise Exception(f"Environment key: {os.getenv('MONGODB_URL_KEY')} is not set.")
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name
            logging.info("MongoDB connection successful")
        except Exception as e:
            raise CustomException(e, sys)
