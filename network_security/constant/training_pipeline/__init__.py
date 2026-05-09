import os, sys
import numpy as np
import pandas as pd

"""
Defining common constant variable for training pipeline
"""
TARGET_COLUMN = "Result"
PIPELINE_NAME:str = "NetworkSecurity"
ARTIFACT_DIR:str = "Artifacts"
FILE_NAME:str = "phishingData.csv"

TRAIN_FILE_NAME:str = "train.csv"
TEST_FILE_NAME:str = "test.csv"

SCHEMA_FILE_PATH = os.path.join("data_schema", "schema.yaml")

DRIFT_THRESHOLD: float = 0.05

PREPROCESSING_OBJECT_FILE_NAME: str = "preprocessor.pkl"

MODEL_FILE_NAME: str = "model.pkl"

SAVED_MODEL_DIR = os.path.join("saved_models")

FINAL_MODEL_DIR: str = "Final_Model"
FINAL_MODEL_PATH = os.path.join(FINAL_MODEL_DIR,"best_model.pkl")

PREDICTED_OUTPUT_DIR: str = "predicted_output"
PREDICTED_OUTPUT_FILE_NAME: str = "output.csv"
DEFAULT_TEST_FILE = os.path.join("valid_data","test.csv")

TRAINING_BUCKET_NAME = "networksecurity"

"""
Data Ingestion related constant starts with DATA_INGESTION VAR NAME
"""

DATA_INGESTION_COLLECTION_NAME:str = "NetworkData"
DATA_INGESTION_DATABASE_NAME:str = "Aftab_MLProject2"
DATA_INGESTION_DIR_NAME:str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR:str = "feature_store"
DATA_INGESTION_INGESTED_DIR:str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO:float = 0.2


"""
Data Validation related constant start with DATA_VALIDATION VAR NAME
"""

DATA_VALIDATION_DIR_NAME: str = "data_validation"
DATA_VALIDATION_VALID_DIR: str = "validated"
DATA_VALIDATION_INVALID_DIR: str = "invalid"
DATA_VALIDATION_DRIFT_REPORT_DIR: str = "drift_report"
DATA_VALIDATION_DRIFT_REPORT_FILE_NAME: str = "report.yaml"

"""
Data Transformation related constant start with DATA TRANSFORMATION VAR NAME
"""

DATA_TRANSFORMATION_DIR_NAME: str = "data_transformation"
DATA_TRANSFORMATION_TRANSFORMED_DATA_DIR: str = "transformed"
DATA_TRANSFORMATION_TRANSFORMED_OBJECT_DIR: str = "transformed_object"

# KNN imputer to replace nan value
DATA_TRANSFORMATION_IMPUTER_PARAMS: dict = {
    "missing_values": np.nan,
    "n_neighbors": 3,
    "weights": "uniform",
}

"""
Model Trainer related constant start with MODE TRAINER VAR NAME
"""

MODEL_TRAINER_DIR_NAME: str = "model_trainer"
MODEL_TRAINER_TRAINED_MODEL_DIR: str = "trained_model"
MODEL_TRAINER_TRAINED_MODEL_NAME: str = "model.pkl"
MODEL_TRAINER_EXPECTED_SCORE: float = 0.6
MODEL_TRAINER_OVER_FITTING_UNDER_FITTING_THRESHOLD: float = 0.05
MODEL_TRAINER_PARAM_GRID_YAML_FILE_NAME: str = "param_grid.yaml"
MODEL_TRAINER_PARAM_GRID_YAML_FILE_PATH = os.path.join("Model_parameters",MODEL_TRAINER_PARAM_GRID_YAML_FILE_NAME)


"""
Model Selector related constant start with MODE SELECTOR VAR NAME
"""
MODEL_SELECTOR_EXPERIMENT_NAME: str = "NetworkSecurityModel"
MODEL_SELECTOR_METRIC_NAME: str     = "test_f1"          # matches log_metrics("test", ...) → test_f1
MODEL_SELECTOR_DIR_NAME: str        = "model_selector"
MODEL_SELECTOR_BEST_MODEL_DIR: str  = "best_model"
MODEL_SELECTOR_BEST_MODEL_FILE_NAME: str = "best_model.pkl"
MODEL_SELECTOR_METADATA_FILE_NAME: str   = "best_model_metadata.json"