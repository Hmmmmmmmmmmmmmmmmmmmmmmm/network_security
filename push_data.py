import os, sys, json
from dotenv import load_dotenv
from pathlib import Path
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
if not MONGO_DB_URL:
    raise ValueError("MONGO_DB_URL is not set")
import certifi
ca = certifi.where()

import pandas as pd
import numpy as np
import pymongo
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger

log = get_logger(__name__)

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            log.error("Exception!!")
            raise NetworkSecurityException(e,sys)

    def csv_to_json(self,file_path):
        '''
        Converts csv to json and returns
        a list of said json data
        '''
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            log.error("Exception!!")
            raise NetworkSecurityException(e,sys)

    def insert_data_mongodb(self,records,database,collection):
        '''
        Pushes data to MongoDB
        '''
        try:
            self.database=database
            self.collection=collection
            self.records=records

            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[self.database]

            self.collection=self.database[self.collection]
            self.collection.insert_many(self.records)
            return(len(self.records))
        except Exception as e:
            log.error("Exception!!")
            raise NetworkSecurityException(e,sys)



if __name__ == '__main__':
    log.info("Entering Data Pushing Stage")


    BASE_DIR = Path(__file__).resolve().parent
    FILE_PATH = BASE_DIR / "Network_Data" / "phisingData.csv"
    DATABASE = "Aftab_MLProject2"
    Collection = "NetworkData"
    network_obj = NetworkDataExtract()
    records=network_obj.csv_to_json(file_path=FILE_PATH)
    print(records)
    no_of_records = network_obj.insert_data_mongodb(records,DATABASE,Collection)
    log.info(f"Function Ran, records pushed: {no_of_records}")
    print(no_of_records)

