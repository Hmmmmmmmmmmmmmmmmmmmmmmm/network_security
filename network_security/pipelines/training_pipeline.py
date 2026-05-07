import mlflow.sklearn
from typing import Any, Dict, Tuple
import os, sys
from network_security.components import model_trainer
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger

log = get_logger(__name__)

from network_security.components.data_ingestion import DataIngestion
from network_security.components.data_validation import DataValidation
from network_security.components.data_transformation import DataTransformation
from network_security.components.model_trainer import ModelTrainer
from network_security.components.model_selector import ModelSelector

from network_security.entity.config_entity import(
    TrainingPipelineConfig,
    DataIngestionConfig,
    DataValidationConfig,
    DataTransformationConfig,
    ModelTrainerConfig,
    ModelSelectorConfig
)

from network_security.entity.artifact_entity import(
    DataIngestionArtifact,
    DataValidationArtifact,
    DataTransformationArtifact,
    ModelTrainerArtifact,
    BestModelArtifact
)
import dagshub

class TrainingPipeline:
    def __init__(self):
        self.training_pipeline_config = TrainingPipelineConfig()

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            log.info("Initializing Data Ingestion")
            # datetime = datetime.now()
            data_ingestion_config = DataIngestionConfig(
                training_pipeline_config=self.training_pipeline_config
            )
            data_ingestion = DataIngestion(
                data_ingestion_config=data_ingestion_config
            )
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            log.info(f"Data Ingestion Artifact Created:\n{data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            log.error("Skill Issue", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

    def start_data_validation(self, data_ingestion_artifact:DataIngestionArtifact) -> DataValidationArtifact:
        try:
            log.info(f"Initializing Data Validation")
            data_validation_config = DataValidationConfig(
                training_pipeline_config=self.training_pipeline_config
            )
            data_validation = DataValidation(
                data_validation_config=data_validation_config,
                data_ingestion_artifact=data_ingestion_artifact
            )
            data_validation_artifact = data_validation.initiate_data_validation()
            log.info(f"Data validation Artifact Created:\n{data_validation_artifact}")
            return data_validation_artifact
        except Exception as e:
            log.error("Skill Issue", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

    def start_data_transformation(self, data_validation_artifact: DataValidationArtifact) -> DataTransformationArtifact:
        try:
            log.info("Initializing Data Transformation")
            data_transformation_config = DataTransformationConfig(
                training_pipeline_config=self.training_pipeline_config,
            )
            data_transformation = DataTransformation(
                data_transformation_config=data_transformation_config,
                data_validation_artifact=data_validation_artifact,
            )
            data_transformation_artifact = data_transformation.initiate_data_transformation()
            log.info(f"Data Transformation Artifact Created:\n{data_transformation_artifact}")
            return data_transformation_artifact
        except Exception as e:
            log.error("Skill Issue", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

    def start_model_trainer(self, data_transformation_artifact: DataTransformationArtifact) -> Tuple[ModelTrainer, ModelTrainerArtifact]:
        """
        Returns (model_trainer, model_trainer_artifact) — the instance is passed
        downstream to ModelSelector so training delegation stays in ModelTrainer.
        """
        try:
            log.info("Initializing Model Trainer")
            model_trainer_config = ModelTrainerConfig(
                training_pipeline_config=self.training_pipeline_config
            )
            model_trainer = ModelTrainer(
                model_trainer_config=model_trainer_config,
                data_transformation_artifact=data_transformation_artifact,
            )
            model_trainer_artifact = model_trainer.initiate_model_trainer()
            log.info(f"Model Trainer Artifact Created:\n{model_trainer_artifact}")
            return model_trainer, model_trainer_artifact
        except Exception as e:
            log.error("Skill Issue", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

    def start_model_selector(self, model_trainer:ModelTrainer, model_trainer_artifact: ModelTrainerArtifact) -> BestModelArtifact:
        try:
            log.info("Initializing Model Selector")
            model_selector_config = ModelSelectorConfig(
                training_pipeline_config=self.training_pipeline_config
            )
            model_selector = ModelSelector(
                model_selector_config  = model_selector_config,
                model_trainer          = model_trainer,
                model_trainer_artifact = model_trainer_artifact,
            )
            best_model_artifact = model_selector.initiate_model_selector()
            log.info(f"Model Selector Artifact Created:\n{best_model_artifact}")
            return best_model_artifact
        except Exception as e:
            log.error("Skill Issue", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

    # Entry Point::
    def run_pipeline(self):
        try:
            log.info("Initializing Training Pipeline")

            # DagShub init lives here — one call covers the whole process
            dagshub.init(
                repo_owner="Hmmmmmmmmmmmmmmmmmmmmmmm",
                repo_name="network_security",
                mlflow=True,
            )
            data_ingestion_artifact    = self.start_data_ingestion()
            data_validation_artifact   = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            data_transformation_artifact = self.start_data_transformation(data_validation_artifact=data_validation_artifact)
            # instance + artifact both flow forward
            model_trainer, model_trainer_artifact = self.start_model_trainer(data_transformation_artifact=data_transformation_artifact)
            best_model_artifact = self.start_model_selector(model_trainer=model_trainer,model_trainer_artifact=model_trainer_artifact)
            log.info("Training Pipeline completed successfully")
            return best_model_artifact


            log.info(f"Pipeline Ran Successfully:\n")
        except Exception as e:
            log.error("Pipeline Failed! aka: \"Skill Issue\"", exc_info=True)
            raise NetworkSecurityException(e, sys) from e



# ghjksdf



