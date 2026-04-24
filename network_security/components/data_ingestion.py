from email import header
import os, sys
from textwrap import indent

from network_security.entity import artifact_entity
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger

log = get_logger()

# config of the Data Ingestion Config
from network_security.entity.config_entity import DataIngestionConfig
from network_security.entity.artifact_entity import DataIngestionArtifact

import pandas as pd
import numpy as np
from typing import List
import pymongo
from sklearn.model_selection import train_test_split

from dotenv import load_dotenv

load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")

class DataIngestion:

    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            log.info("Entered DataIngestion Constructor")
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e,sys) from e

    def export_collection_as_dataframe(self)->pd.DataFrame:
        """
        Read Data from MongoDB
        """
        try:
            log.info("Exporting collection as a Dataframe")
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            collection = self.mongo_client[database_name][collection_name]
            df = pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"],axis=1)
            df.replace({"na":np.nan}, inplace=True)
            if df.isna().any().any():
                log.warning("Missing values detected in dataset")
            log.info("Fetched collection successfully")
            return df
        except Exception as e:
            raise NetworkSecurityException(e,sys) from e

    def export_data_into_feature_store(self, dataframe: pd.DataFrame):
        try:
            log.info("Exporting Data into feature_store")
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            # Creating folder:
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            return dataframe

        except Exception as e:
            raise NetworkSecurityException(e,sys) from e

    def split_data_as_train_test_library(self, dataframe:pd.DataFrame):
        try:
            log.info(f"Performing train test split on the dataframe with ratio; {self.data_ingestion_config.train_test_split_ratio}")
            train_set, test_set = train_test_split(
                dataframe, test_size = self.data_ingestion_config.train_test_split_ratio
            )
            log.info(f"Train test split done, exporting files to given path")
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path,exist_ok=True)
            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index = False, header=True
            )
            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index = False, header=True
            )
            log.info(f"Exported Training and Testing files")
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

    def initiate_data_ingestion(self) ->DataIngestionArtifact:
        try:
            log.info("Initiating Data Ingestion")
            dataframe = self.export_collection_as_dataframe()
            dataframe = self.export_collection_as_dataframe()
            self.split_data_as_train_test_library(dataframe=dataframe)
            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )
            return data_ingestion_artifact
        except Exception as e:
            log.error("Failed during data ingestion", exc_info=True)
            raise NetworkSecurityException(e, sys) from e