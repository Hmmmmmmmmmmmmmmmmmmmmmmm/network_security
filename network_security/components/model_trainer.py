from network_security.entity.artifact_entity import DataTransformationArtifact, ModelTrainerArtifact
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

class ModelTrainer:
    def __init__(self, model_trainer_config: ModelTrainerConfig,
                 data_transformation_artifact: DataTransformationArtifact):
        try:
            log.info("Entered ModelTrainer Constructor")
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifact = data_transformation_artifact
        except Exception as e:
            # log.error("Failed during data ingestion", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

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
            models = {
                "Random Forest": RandomForestClassifier(),
                "Logistic Regression": LogisticRegression(max_iter=1000),
                "Ridge Classifier": RidgeClassifier(),
                "SGD Classifier (ElasticNet)": SGDClassifier(
                    loss="log_loss",
                    penalty="elasticnet"
                ),
                "KNN": KNeighborsClassifier(),
                "SVC": SVC(probability=True),
                "Decision Tree": DecisionTreeClassifier(),
                "Gradient Boosting": GradientBoostingClassifier(),
                "AdaBoost": AdaBoostClassifier(
                    estimator=DecisionTreeClassifier()
                ),
                "XGBoost": XGBClassifier(eval_metric="logloss"),
                "CatBoost": CatBoostClassifier(verbose=0)
            }

            results_df = evaluate_classifiers(
                X_train=X_train_full,
                y_train=y_train_full,
                X_test=X_val,
                y_test=y_val,
                models=models
            )
            top_models = results_df.head(5)["Model"].tolist()
            params_file_path = self.model_trainer_config.param_grid_file_path
            config = read_yaml(params_file_path)
            param_grids = config.get("param_grids", {}) # YAML FILE INTAKE HERE WITH PARAMS!!!
            tuned_results, best_models = tune_models(
                X_train_full, y_train_full,
                X_val, y_val,
                models,
                param_grids,
                top_models
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
            return best_model


        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

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
            model = self.train_model(train_arr)

            # Train metrics
            y_train_pred = model.predict(x_train)
            y_train_score = model.predict_proba(x_train) if hasattr(model, "predict_proba") else None

            classification_train_metrics, _, _ = get_classification_metrics(
                y_true=y_train,
                y_pred=y_train_pred,
                y_score=y_train_score,
                model_name="Train"
            )

            # Tracking MLFlow!
            # Test metrics
            y_test_pred = model.predict(x_test)
            y_test_score = model.predict_proba(x_test) if hasattr(model, "predict_proba") else None

            classification_test_metrics, _, _ = get_classification_metrics(
                y_true=y_test,
                y_pred=y_test_pred,
                y_score=y_test_score,
                model_name="Test"
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
