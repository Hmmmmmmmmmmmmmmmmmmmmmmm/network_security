from dataclasses import dataclass, field
from typing import Any, Dict, Optional

@dataclass
class DataIngestionArtifact:
    trained_file_path:str
    test_file_path:str


@dataclass
class DataValidationArtifact:
    validation_status: bool
    valid_train_file_path: str
    valid_test_file_path: str
    invalid_train_file_path: str
    invalid_test_file_path: str
    drift_report_file_path: str

@dataclass
class DataTransformationArtifact:
    transformed_object_file_path: str
    transformed_train_file_path: str
    transformed_test_file_path: str


@dataclass
class ClassificationMetricArtifact:
    accuracy_score:float
    f1_score: float
    precision_score: float
    recall_score: float
    roc_auc_score: float
    average_precision_score: float
@dataclass
class ModelTrainerArtifact:
    trained_model_file_path: str
    train_metric_artifact: ClassificationMetricArtifact
    test_metric_artifact: ClassificationMetricArtifact

@dataclass
class BestModelArtifact:
    experiment_name:          str
    run_id:                   str
    model_name:               str            # from MLflow tag  "model_name"
    model_class_name:         str            # model.__class__.__name__
    metric_name:              str
    metric_value:             float
    mlflow_model_uri:         str
    selected_model_file_path: str
    preprocessor_file_path:   Optional[str]
    params:                   Dict[str, str] = field(default_factory=dict)
    tags:                     Dict[str, str] = field(default_factory=dict)