# Currently made for testing purposes

import os, sys
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger

from network_security.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig
from network_security.entity.artifact_entity import DataIngestionArtifact
from network_security.components.data_ingestion import DataIngestion


log = get_logger(__name__)

if __name__ == "__main__":
    try:
        log.info("Testing Data Ingestion segment")
        # datetime = datetime.now()
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config)
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        print(data_ingestion_artifact)
        log.info(f"Data Ingestion Artifact Successfully Ran:\n{data_ingestion_artifact}")
    except Exception as e:
        log.error("Failed during data ingestion", exc_info=True)
        raise NetworkSecurityException(e, sys) from e