import mlflow.sklearn
from typing import Any, Dict
from network_security.entity.artifact_entity import DataTransformationArtifact, ModelTrainerArtifact, ClassificationMetricArtifact
from network_security.entity.config_entity import ModelTrainerConfig

from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger

log = get_logger(__name__)

from network_security.utils.main_utils.utils import load_numpy_array_data, read_yaml, save_object, load_object
from network_security.utils.ml_utils.metrics.classification_metrics import get_classification_score
from network_security.utils.ml_utils.model.estimator import NetworkModel, get_classification_metrics
from network_security.constant.training_pipeline import MODEL_TRAINER_EXPECTED_SCORE
from network_security.utils.ml_utils.model.estimator import tune_models, evaluate_classifiers
import pandas as pd
import numpy as np
import os, sys

# MODELS IMPORT (Sklearn only no custom models):
# Models
from catboost import CatBoostClassifier
from xgboost import XGBClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import (
    AdaBoostClassifier,
    GradientBoostingClassifier,
    RandomForestClassifier
)
from sklearn.linear_model import (
    LogisticRegression,
    RidgeClassifier,
    SGDClassifier
)
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import RandomizedSearchCV
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
import mlflow
# import dagshub
# dagshub.init(repo_owner='Hmmmmmmmmmmmmmmmmmmmmmmm', repo_name='network_security', mlflow=True)

class ModelTrainer:
    def __init__(self, model_trainer_config: ModelTrainerConfig,
                 data_transformation_artifact: DataTransformationArtifact):
        try:
            log.info("Entered ModelTrainer Constructor")
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifact = data_transformation_artifact
            self.preprocessor_path = self.data_transformation_artifact.transformed_object_file_path
        except Exception as e:
            # log.error("Failed during data ingestion", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

    def log_metrics(self, prefix: str, metrics: ClassificationMetricArtifact):
        mlflow.log_metric(f"{prefix}_accuracy", metrics.accuracy_score)
        mlflow.log_metric(f"{prefix}_f1", metrics.f1_score)
        mlflow.log_metric(f"{prefix}_precision", metrics.precision_score)
        mlflow.log_metric(f"{prefix}_recall", metrics.recall_score)
        mlflow.log_metric(f"{prefix}_roc_auc", metrics.roc_auc_score)
        mlflow.log_metric(f"{prefix}_avg_precision", metrics.average_precision_score)

    @staticmethod
    def get_models() -> dict:
        """
        Single source of truth for all candidate models.
        Imported by ModelSelector to avoid registry duplication.
        """
        return {
            "Random Forest":               RandomForestClassifier(),
            "Logistic Regression":         LogisticRegression(max_iter=1000),
            "Ridge Classifier":            RidgeClassifier(),
            "SGD Classifier (ElasticNet)": SGDClassifier(loss="log_loss", penalty="elasticnet"),
            "KNN":                         KNeighborsClassifier(),
            "SVC":                         SVC(probability=True),
            "Decision Tree":               DecisionTreeClassifier(),
            "Gradient Boosting":           GradientBoostingClassifier(),
            "AdaBoost":                    AdaBoostClassifier(estimator=DecisionTreeClassifier()),
            "XGBoost":                     XGBClassifier(eval_metric="logloss"),
            "CatBoost":                    CatBoostClassifier(verbose=0),
        }

    def train_model(self, train_array):
        try:
            log.info("Training models")
            X_train_full, X_val, y_train_full, y_val = train_test_split(
                train_array[:, :-1],
                train_array[:, -1],
                test_size=0.2,
                random_state=42,
                stratify=train_array[:, -1]
            )
            models = ModelTrainer.get_models()

            results_df = evaluate_classifiers(
                X_train=X_train_full,
                y_train=y_train_full,
                X_test=X_val,
                y_test=y_val,
                models=models
            )
            top_models = results_df.head(5)["Model"].tolist()
            log.info(f"Top 5 models selected for tuning: {top_models}")
            params_file_path = self.model_trainer_config.param_grid_file_path
            config = read_yaml(params_file_path)
            param_grids = config.get("param_grids", {}) # YAML FILE INTAKE HERE WITH PARAMS!!!
            tuned_results, best_models = tune_models(
                X_train_full, y_train_full,
                X_val, y_val,
                models,
                param_grids,
                top_models,
                n_iter=100,
                do_GridSearch = True
            )
            best_model_name = max(
                tuned_results,
                key=lambda x: tuned_results[x]["metrics"].f1_score
            )
            best_model = best_models[best_model_name]
            # Retrain on full training data
            best_model.fit(train_array[:, :-1], train_array[:, -1])
            best_score = tuned_results[best_model_name]["metrics"].f1_score
            log.info(f"Validation F1 Score of best model ({best_model_name}): {best_score}")
            if best_score < MODEL_TRAINER_EXPECTED_SCORE:
                log.info(f"No good model found [F1 < {MODEL_TRAINER_EXPECTED_SCORE}]")
                raise NetworkSecurityException(f"No good model found [F1 < {MODEL_TRAINER_EXPECTED_SCORE}]",sys)
            summary = {
                "best_model": best_model_name,
                "f1_score": best_score,
                "metrics": tuned_results[best_model_name]["metrics"],
                "best_params": tuned_results[best_model_name]["best_params"],
            }
            log.info(summary)
            return best_model_name, best_model, tuned_results[best_model_name]["best_params"]


        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

    def train_single_model(self, model_name: str, raw_params: Dict[str, str]) -> Any:
        """
        Train a single named model with given params on the full training data.
        Called by ModelSelector to retrain with historical best params —
        keeps all training logic inside ModelTrainer.
        """
        models = ModelTrainer.get_models()
        if model_name not in models:
            raise ValueError(
                f"Model '{model_name}' not found in ModelTrainer registry. "
                f"Available: {list(models.keys())}"
            )

        # Cast string params from MLflow using the YAML grid as type reference
        config      = read_yaml(self.model_trainer_config.param_grid_file_path)
        param_grids = config.get("param_grids", {})
        grid        = param_grids.get(model_name, {})
        params      = {}

        for k, v_str in raw_params.items():
            if k in grid and grid[k]:
                ref = grid[k][0]
                try:
                    if isinstance(ref, bool):    params[k] = str(v_str).lower() == "true"
                    elif isinstance(ref, int):   params[k] = int(v_str)
                    elif isinstance(ref, float): params[k] = float(v_str)
                    else:                        params[k] = v_str
                except (ValueError, TypeError):  params[k] = v_str
            else:
                try:    params[k] = int(v_str)
                except:
                    try:    params[k] = float(v_str)
                    except: params[k] = v_str

        log.info("train_single_model — %s  params: %s", model_name, params)

        # Fresh instance from registry with historical params
        model = models[model_name].__class__(**params)

        train_arr = load_numpy_array_data(
            self.data_transformation_artifact.transformed_train_file_path
        )
        model.fit(train_arr[:, :-1], train_arr[:, -1])
        log.info("train_single_model — training complete: %s", model.__class__.__name__)
        return model

    def initiate_model_trainer(self):
        try:
            log.info("Initiating Model Training")
            train_file_path = self.data_transformation_artifact.transformed_train_file_path
            test_file_path = self.data_transformation_artifact.transformed_test_file_path
            # loading arrays
            train_arr = load_numpy_array_data(train_file_path)
            test_arr = load_numpy_array_data(test_file_path)
            x_train, y_train, x_test, y_test = (
                train_arr[:,:-1],
                train_arr[:,-1],
                test_arr[:,:-1],
                test_arr[:,-1],
            )
            model_name, model, params = self.train_model(train_arr)

            # Train metrics
            y_train_pred = model.predict(x_train)
            y_train_score = model.predict_proba(x_train) if hasattr(model, "predict_proba") else None

            classification_train_metrics: ClassificationMetricArtifact
            classification_train_metrics, _, _ = get_classification_metrics(
                y_true=y_train,
                y_pred=y_train_pred,
                y_score=y_train_score,
                model_name="Train"
            )

            # Test metrics
            y_test_pred = model.predict(x_test)
            y_test_score = model.predict_proba(x_test) if hasattr(model, "predict_proba") else None

            classification_test_metrics: ClassificationMetricArtifact
            classification_test_metrics, _, _ = get_classification_metrics(
                y_true=y_test,
                y_pred=y_test_pred,
                y_score=y_test_score,
                model_name="Test"
            )

            # Tracking MLFlow!
            # self.track_mlflow(
            #     "train",model, classification_train_metrics
            # )
            # self.track_mlflow(
            #     "test",model, classification_test_metrics
            # )
            mlflow.set_experiment("NetworkSecurityModel")
            with mlflow.start_run(run_name=model_name):
                # Log params
                # mlflow.log_param("model_name", model_name)
                mlflow.set_tag("model_name", model_name)
                mlflow.set_tag("pipeline", "NetworkSecurity")

                # mlflow.log_params(params)
                for k, v in params.items():
                    mlflow.log_param(k, v)

                # Train metrics
                self.log_metrics("train", classification_train_metrics)
                # Test metrics
                self.log_metrics("test", classification_test_metrics)
                # Log model ONCE
                mlflow.sklearn.log_model(model, "model")
                preprocessor_local_path = str(self.preprocessor_path)
                log.info("Logging preprocessor artifact from: %s", preprocessor_local_path)
                mlflow.log_artifact(
                    local_path=preprocessor_local_path,
                    artifact_path="preprocessor"
                )


            preprocessor = load_object(file_path=self.data_transformation_artifact.transformed_object_file_path)

            model_dir_path = os.path.dirname(self.model_trainer_config.trained_model_file_path)
            os.makedirs(model_dir_path,exist_ok=True)

            Network_Model = NetworkModel(
                model=model,
                preprocessor=preprocessor
            )

            save_object(self.model_trainer_config.trained_model_file_path, obj=Network_Model)

            model_trainer_artifact = ModelTrainerArtifact(
                trained_model_file_path=self.model_trainer_config.trained_model_file_path,
                train_metric_artifact=classification_train_metrics,
                test_metric_artifact=classification_test_metrics
            )
            log.info(f"Model Trainer artifact: {model_trainer_artifact}")

            return model_trainer_artifact

        except Exception as e:
            log.error("Failed during Model Trainer", exc_info=True)
            raise NetworkSecurityException(e, sys) from e
