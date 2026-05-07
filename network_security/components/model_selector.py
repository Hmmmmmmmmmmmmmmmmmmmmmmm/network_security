import json
import os
import pickle
import sys
from typing import Any, Dict, Optional, Tuple

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

from network_security.components.model_trainer import ModelTrainer
from network_security.entity.artifact_entity import BestModelArtifact, ModelTrainerArtifact
from network_security.entity.config_entity import ModelSelectorConfig
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger
from network_security.utils.main_utils.utils import save_object
from network_security.utils.ml_utils.model.estimator import NetworkModel

log = get_logger(__name__)


class ModelSelector:
    """
    Orchestrates best-model selection — does NOT train.
    All training is delegated to ModelTrainer.train_single_model().

    Phase 1 — compare current run f1 vs historical best from MLflow.
    Phase 2a — current wins  : load model + preprocessor from local NetworkModel pkl.
    Phase 2b — historical wins: delegate retrain to ModelTrainer with historical params.
    """

    def __init__(
        self,
        model_selector_config:  ModelSelectorConfig,
        model_trainer:          ModelTrainer,           # owns training + config + transform artifact
        model_trainer_artifact: ModelTrainerArtifact,  # output of model_trainer.initiate_model_trainer()
    ):
        try:
            log.info("Entered ModelSelector Constructor")
            self.cfg          = model_selector_config
            self.model_trainer = model_trainer
            self.trainer_art  = model_trainer_artifact
            self.client       = MlflowClient()
            os.makedirs(self.cfg.best_model_dir,  exist_ok=True)
            os.makedirs(self.cfg.final_model_dir, exist_ok=True)
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

    # ── MLflow ───────────────────────────────────────────────────────────────

    def _get_best_run(self):
        experiment = mlflow.get_experiment_by_name(self.cfg.experiment_name)
        if experiment is None:
            raise ValueError(
                f"MLflow experiment not found: '{self.cfg.experiment_name}'. "
                "Ensure ModelTrainer ran and dagshub/mlflow are pointing at the same URI."
            )
        runs = self.client.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string="attributes.status = 'FINISHED'",
        )
        valid_runs = [r for r in runs if self.cfg.metric_name in r.data.metrics]
        if not valid_runs:
            raise ValueError(
                f"No finished runs contain metric '{self.cfg.metric_name}' "
                f"in experiment '{self.cfg.experiment_name}'."
            )
        best_run = max(
            valid_runs,
            key=lambda r: (r.data.metrics[self.cfg.metric_name], r.info.end_time or 0),
        )
        log.info(
            "Best historical run — id=%s  %s=%.6f  model=%s",
            best_run.info.run_id,
            self.cfg.metric_name,
            best_run.data.metrics[self.cfg.metric_name],
            best_run.data.tags.get("model_name", "unknown"),
        )
        return best_run

    # ── Model loading ────────────────────────────────────────────────────────

    def _load_local_bundle(self) -> Tuple[Any, Any, str]:
        """Unpack model + preprocessor from current run's NetworkModel pkl."""
        local_path = self.trainer_art.trained_model_file_path
        log.info("Loading current-run model from: %s", local_path)
        with open(local_path, "rb") as fh:
            network_model = pickle.load(fh)
        log.info("Local model loaded: %s", network_model.model.__class__.__name__)
        return network_model.model, network_model.preprocessor, local_path

    def _retrain_with_historical_params(
        self, model_name: str, raw_params: Dict[str, str]
    ) -> Tuple[Any, Any]:
        """
        Delegate training to ModelTrainer — no training logic here.
        Preprocessor always from current run (fit on current data).
        """
        model = self.model_trainer.train_single_model(model_name, raw_params)

        with open(self.trainer_art.trained_model_file_path, "rb") as fh:
            network_model = pickle.load(fh)

        return model, network_model.preprocessor

    # ── Save helpers ─────────────────────────────────────────────────────────

    def _save_bundle(self, model: Any, preprocessor: Any) -> str:
        """
        Save as NetworkModel — preserves the predict() interface
        (preprocessor.transform → model.predict) for batch prediction.
        """
        network_model = NetworkModel(model=model, preprocessor=preprocessor)
        save_object(file_path=self.cfg.best_model_file_path, obj=network_model)
        log.info("NetworkModel saved to: %s", self.cfg.best_model_file_path)
        return self.cfg.best_model_file_path

    def _save_bundle_previous_iter(self, model: Any, preprocessor: Optional[Any]) -> str:
        bundle = {"model": model, "preprocessor": preprocessor}
        save_object(file_path=self.cfg.best_model_file_path, obj=bundle)
        log.info("Model bundle saved to: %s", self.cfg.best_model_file_path)
        return self.cfg.best_model_file_path

    def _write_metadata(self, artifact: BestModelArtifact, source: str) -> None:
        payload = {
            "experiment_name":          artifact.experiment_name,
            "run_id":                   artifact.run_id,
            "model_name":               artifact.model_name,
            "model_class_name":         artifact.model_class_name,
            "metric_name":              artifact.metric_name,
            "metric_value":             artifact.metric_value,
            "selection_source":         source,
            "mlflow_model_uri":         artifact.mlflow_model_uri,
            "selected_model_file_path": artifact.selected_model_file_path,
            "preprocessor_file_path":   artifact.preprocessor_file_path,
            "params":                   artifact.params,
            "tags":                     artifact.tags,
        }
        with open(self.cfg.metadata_file_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        log.info("Metadata written to: %s", self.cfg.metadata_file_path)

    # ── Entry point ──────────────────────────────────────────────────────────

    def initiate_model_selector(self) -> BestModelArtifact:
        try:
            log.info("Initiating Model Selection")

            best_run      = self._get_best_run()
            historical_f1 = best_run.data.metrics[self.cfg.metric_name]
            current_f1    = self.trainer_art.test_metric_artifact.f1_score

            log.info(
                "F1 comparison — current run: %.6f  |  historical best: %.6f",
                current_f1, historical_f1,
            )

            if current_f1 >= historical_f1:
                log.info("Current run is best — loading from local artifact")
                model, preprocessor, prep_path = self._load_local_bundle()
                source = "current_run_local"
            else:
                hist_model_name = best_run.data.tags.get("model_name")
                hist_params     = best_run.data.params
                log.info(
                    "Historical run is better — delegating retrain of '%s' to ModelTrainer",
                    hist_model_name,
                )
                if not hist_params:
                    log.warning("No logged params on historical run — falling back to current run")
                    model, preprocessor, prep_path = self._load_local_bundle()
                    source = "current_run_fallback_no_params"
                else:
                    model, preprocessor = self._retrain_with_historical_params(
                        hist_model_name, hist_params
                    )
                    prep_path = self.trainer_art.trained_model_file_path
                    source    = "retrained_historical_params"

            run_id        = best_run.info.run_id
            selected_path = self._save_bundle(model, preprocessor)

            artifact = BestModelArtifact(
                experiment_name          = self.cfg.experiment_name,
                run_id                   = run_id,
                model_name               = best_run.data.tags.get("model_name", "unknown"),
                model_class_name         = model.__class__.__name__,
                metric_name              = self.cfg.metric_name,
                metric_value             = max(current_f1, historical_f1),
                mlflow_model_uri         = f"runs:/{run_id}/model",
                selected_model_file_path = selected_path,
                preprocessor_file_path   = prep_path,
                params                   = dict(best_run.data.params),
                tags                     = dict(best_run.data.tags),
            )

            self._write_metadata(artifact, source)
            log.info(
                "Model Selection complete — %s  source=%s  %s=%.6f",
                artifact.model_class_name, source,
                artifact.metric_name, artifact.metric_value,
            )
            return artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys) from e


# import json
# import os
# import pickle
# import sys
# from typing import Any, Dict, Optional, Tuple

# import mlflow
# import mlflow.sklearn
# from mlflow.tracking import MlflowClient

# from network_security.components.model_trainer import ModelTrainer
# from network_security.entity.artifact_entity import (
#     BestModelArtifact,
#     DataTransformationArtifact,
#     ModelTrainerArtifact,
# )
# from network_security.entity.config_entity import ModelSelectorConfig, ModelTrainerConfig
# from network_security.exception.exception import NetworkSecurityException
# from network_security.logging.logger import get_logger
# from network_security.utils.main_utils.utils import load_numpy_array_data, read_yaml, save_object

# log = get_logger(__name__)


# class ModelSelector:
#     """
#     Two-phase best-model selection:
#       Phase 1 — compare current run f1 vs historical best from MLflow.
#       Phase 2a — current wins  : load model + preprocessor from local NetworkModel pkl.
#       Phase 2b — historical wins: retrain locally using historical params (no remote download).

#     Models dict and param grid types are sourced from ModelTrainer / YAML to avoid duplication.
#     """

#     def __init__(
#         self,
#         model_selector_config:        ModelSelectorConfig,
#         model_trainer_config:         ModelTrainerConfig,
#         model_trainer_artifact:       ModelTrainerArtifact,
#         data_transformation_artifact: DataTransformationArtifact,
#     ):
#         try:
#             log.info("Entered ModelSelector Constructor")
#             self.cfg           = model_selector_config
#             self.trainer_cfg   = model_trainer_config
#             self.trainer_art   = model_trainer_artifact
#             self.transform_art = data_transformation_artifact
#             self.client        = MlflowClient()

#             # Shared model registry — single source of truth from ModelTrainer
#             self._models = ModelTrainer.get_models()

#             # Param grid from YAML — used for type-safe casting of MLflow string params
#             grid_config        = read_yaml(self.trainer_cfg.param_grid_file_path)
#             self._param_grids  = grid_config.get("param_grids", {})

#             os.makedirs(self.cfg.best_model_dir,  exist_ok=True)
#             os.makedirs(self.cfg.final_model_dir, exist_ok=True)
#         except Exception as e:
#             raise NetworkSecurityException(e, sys) from e

#     # ── MLflow ───────────────────────────────────────────────────────────────

#     def _get_best_run(self):
#         experiment = mlflow.get_experiment_by_name(self.cfg.experiment_name)
#         if experiment is None:
#             raise ValueError(
#                 f"MLflow experiment not found: '{self.cfg.experiment_name}'. "
#                 "Ensure ModelTrainer ran and dagshub/mlflow are pointing at the same URI."
#             )
#         runs = self.client.search_runs(
#             experiment_ids=[experiment.experiment_id],
#             filter_string="attributes.status = 'FINISHED'",
#         )
#         valid_runs = [r for r in runs if self.cfg.metric_name in r.data.metrics]
#         if not valid_runs:
#             raise ValueError(
#                 f"No finished runs contain metric '{self.cfg.metric_name}' "
#                 f"in experiment '{self.cfg.experiment_name}'."
#             )
#         best_run = max(
#             valid_runs,
#             key=lambda r: (r.data.metrics[self.cfg.metric_name], r.info.end_time or 0),
#         )
#         log.info(
#             "Best historical run — id=%s  %s=%.6f  model=%s",
#             best_run.info.run_id,
#             self.cfg.metric_name,
#             best_run.data.metrics[self.cfg.metric_name],
#             best_run.data.tags.get("model_name", "unknown"),
#         )
#         return best_run

#     # ── Param casting ────────────────────────────────────────────────────────

#     def _cast_params(self, model_name: str, raw: Dict[str, str]) -> Dict[str, Any]:
#         """
#         Cast MLflow string params to proper types using the YAML param grid as
#         the type reference — same grid ModelTrainer used during tuning.
#         Falls back to int → float → str auto-detection when a param isn't in the grid.
#         """
#         grid = self._param_grids.get(model_name, {})
#         cast = {}

#         for k, v_str in raw.items():
#             if k in grid and grid[k]:
#                 # Infer type from the first value in the grid list
#                 ref = grid[k][0]
#                 try:
#                     if isinstance(ref, bool):
#                         cast[k] = v_str.lower() == "true"
#                     elif isinstance(ref, int):
#                         cast[k] = int(v_str)
#                     elif isinstance(ref, float):
#                         cast[k] = float(v_str)
#                     else:
#                         cast[k] = v_str
#                 except (ValueError, TypeError):
#                     cast[k] = v_str
#             else:
#                 # Auto-detect: int → float → string
#                 try:    cast[k] = int(v_str)
#                 except:
#                     try:    cast[k] = float(v_str)
#                     except: cast[k] = v_str

#         log.info("Params after casting for %s: %s", model_name, cast)
#         return cast

#     # ── Model loading ────────────────────────────────────────────────────────

#     def _load_local_bundle(self) -> Tuple[Any, Any, str]:
#         """Unpack model + preprocessor from current run's NetworkModel pkl."""
#         local_path = self.trainer_art.trained_model_file_path
#         log.info("Loading current-run model from: %s", local_path)
#         with open(local_path, "rb") as fh:
#             network_model = pickle.load(fh)
#         log.info("Local model loaded: %s", network_model.model.__class__.__name__)
#         return network_model.model, network_model.preprocessor, local_path

#     def _retrain_with_historical_params(
#         self, model_name: str, raw_params: Dict[str, str]
#     ) -> Tuple[Any, Any]:
#         """
#         Rebuild the historical best model from ModelTrainer.get_models(),
#         apply type-safe params from YAML grid, retrain on current training data.
#         Preprocessor always comes from the current run (fit on current data).
#         """
#         if model_name not in self._models:
#             log.warning(
#                 "model_name='%s' not in ModelTrainer.get_models() — falling back to current run.",
#                 model_name,
#             )
#             model, preprocessor, _ = self._load_local_bundle()
#             return model, preprocessor

#         params = self._cast_params(model_name, raw_params)

#         # Get a fresh instance of the model class from the shared registry
#         model = self._models[model_name].__class__(**params)

#         log.info("Retraining %s with historical params: %s", model_name, params)
#         train_arr = load_numpy_array_data(self.transform_art.transformed_train_file_path)
#         model.fit(train_arr[:, :-1], train_arr[:, -1])
#         log.info("Retrain complete: %s", model.__class__.__name__)

#         # Preprocessor from current run's NetworkModel
#         with open(self.trainer_art.trained_model_file_path, "rb") as fh:
#             network_model = pickle.load(fh)

#         return model, network_model.preprocessor

#     # ── Save helpers ─────────────────────────────────────────────────────────

#     def _save_bundle(self, model: Any, preprocessor: Optional[Any]) -> str:
#         bundle = {"model": model, "preprocessor": preprocessor}
#         save_object(file_path=self.cfg.best_model_file_path, obj=bundle)
#         log.info("Model bundle saved to: %s", self.cfg.best_model_file_path)
#         return self.cfg.best_model_file_path

#     def _write_metadata(self, artifact: BestModelArtifact, source: str) -> None:
#         payload = {
#             "experiment_name":          artifact.experiment_name,
#             "run_id":                   artifact.run_id,
#             "model_name":               artifact.model_name,
#             "model_class_name":         artifact.model_class_name,
#             "metric_name":              artifact.metric_name,
#             "metric_value":             artifact.metric_value,
#             "selection_source":         source,
#             "mlflow_model_uri":         artifact.mlflow_model_uri,
#             "selected_model_file_path": artifact.selected_model_file_path,
#             "preprocessor_file_path":   artifact.preprocessor_file_path,
#             "params":                   artifact.params,
#             "tags":                     artifact.tags,
#         }
#         with open(self.cfg.metadata_file_path, "w", encoding="utf-8") as fh:
#             json.dump(payload, fh, indent=2, default=str)
#         log.info("Metadata written to: %s", self.cfg.metadata_file_path)

#     # ── Entry point ──────────────────────────────────────────────────────────

#     def initiate_model_selector(self) -> BestModelArtifact:
#         try:
#             log.info("Initiating Model Selection")

#             best_run      = self._get_best_run()
#             historical_f1 = best_run.data.metrics[self.cfg.metric_name]
#             current_f1    = self.trainer_art.test_metric_artifact.f1_score

#             log.info(
#                 "F1 comparison — current run: %.6f  |  historical best: %.6f",
#                 current_f1, historical_f1,
#             )

#             if current_f1 >= historical_f1:
#                 log.info("Current run is best — loading from local artifact")
#                 model, preprocessor, prep_path = self._load_local_bundle()
#                 source = "current_run_local"
#             else:
#                 hist_model_name = best_run.data.tags.get("model_name")
#                 hist_params     = best_run.data.params

#                 log.info(
#                     "Historical run is better — retraining '%s' with params: %s",
#                     hist_model_name, hist_params,
#                 )

#                 if not hist_params:
#                     log.warning("Historical run has no logged params — falling back to current run")
#                     model, preprocessor, prep_path = self._load_local_bundle()
#                     source = "current_run_fallback_no_params"
#                 else:
#                     model, preprocessor = self._retrain_with_historical_params(
#                         hist_model_name, hist_params
#                     )
#                     prep_path = self.trainer_art.trained_model_file_path
#                     source    = "retrained_historical_params"

#             run_id        = best_run.info.run_id
#             selected_path = self._save_bundle(model, preprocessor)
#             model_uri     = f"runs:/{run_id}/model"

#             artifact = BestModelArtifact(
#                 experiment_name          = self.cfg.experiment_name,
#                 run_id                   = run_id,
#                 model_name               = best_run.data.tags.get("model_name", "unknown"),
#                 model_class_name         = model.__class__.__name__,
#                 metric_name              = self.cfg.metric_name,
#                 metric_value             = max(current_f1, historical_f1),
#                 mlflow_model_uri         = model_uri,
#                 selected_model_file_path = selected_path,
#                 preprocessor_file_path   = prep_path,
#                 params                   = dict(best_run.data.params),
#                 tags                     = dict(best_run.data.tags),
#             )

#             self._write_metadata(artifact, source)
#             log.info(
#                 "Model Selection complete — %s  source=%s  %s=%.6f",
#                 artifact.model_class_name, source,
#                 artifact.metric_name, artifact.metric_value,
#             )
#             return artifact

#         except Exception as e:
#             raise NetworkSecurityException(e, sys) from e


# # import json
# # import os
# # import pickle
# # import sys
# # from typing import Any, Dict, Optional, Tuple

# # import mlflow
# # import mlflow.sklearn
# # import numpy as np
# # from mlflow.tracking import MlflowClient

# # from catboost import CatBoostClassifier
# # from xgboost import XGBClassifier
# # from sklearn.ensemble import AdaBoostClassifier, GradientBoostingClassifier, RandomForestClassifier
# # from sklearn.linear_model import LogisticRegression, RidgeClassifier, SGDClassifier
# # from sklearn.neighbors import KNeighborsClassifier
# # from sklearn.svm import SVC
# # from sklearn.tree import DecisionTreeClassifier

# # from network_security.entity.artifact_entity import (
# #     BestModelArtifact,
# #     DataTransformationArtifact,
# #     ModelTrainerArtifact,
# # )
# # from network_security.entity.config_entity import ModelSelectorConfig
# # from network_security.exception.exception import NetworkSecurityException
# # from network_security.logging.logger import get_logger
# # from network_security.utils.main_utils.utils import load_numpy_array_data, save_object

# # """
# # Decision flow at runtime
# # current_f1 >= historical_best_f1?
# #     YES → load NetworkModel.pkl directly          (source: current_run_local)
# #     NO  → historical params logged?
# #               YES → rebuild model class + retrain  (source: retrained_historical_params)
# #               NO  → fall back to current run       (source: current_run_fallback_no_params)
# # """

# # log = get_logger(__name__)

# # # ── Must mirror ModelTrainer's model dict keys exactly ──────────────────────
# # MODEL_REGISTRY: Dict[str, Any] = {
# #     "Random Forest":               RandomForestClassifier,
# #     "Logistic Regression":         LogisticRegression,
# #     "Ridge Classifier":            RidgeClassifier,
# #     "SGD Classifier (ElasticNet)": SGDClassifier,
# #     "KNN":                         KNeighborsClassifier,
# #     "SVC":                         SVC,
# #     "Decision Tree":               DecisionTreeClassifier,
# #     "Gradient Boosting":           GradientBoostingClassifier,
# #     "AdaBoost":                    AdaBoostClassifier,
# #     "XGBoost":                     XGBClassifier,
# #     "CatBoost":                    CatBoostClassifier,
# # }

# # # MLflow stores all params as strings — these sets drive the type casting
# # _INT_PARAMS   = {"n_estimators", "max_depth", "depth", "iterations",
# #                  "min_samples_split", "min_samples_leaf", "n_neighbors", "max_iter"}
# # _FLOAT_PARAMS = {"learning_rate", "subsample", "colsample_bytree",
# #                  "min_child_weight", "gamma", "reg_alpha", "reg_lambda", "l2_leaf_reg"}


# # class ModelSelector:
# #     """
# #     Two-phase best-model selection:
# #       1. Compare current run's f1 against the historical best from MLflow.
# #       2a. Current run wins  → load model + preprocessor from local NetworkModel pkl.
# #       2b. Historical wins   → retrain locally using historical params + current preprocessor.
# #     No remote artifact downloads — avoids DagShub upload reliability issues.
# #     """

# #     def __init__(
# #         self,
# #         model_selector_config:       ModelSelectorConfig,
# #         model_trainer_artifact:      ModelTrainerArtifact,
# #         data_transformation_artifact: DataTransformationArtifact,
# #     ):
# #         try:
# #             log.info("Entered ModelSelector Constructor")
# #             self.cfg          = model_selector_config
# #             self.trainer_art  = model_trainer_artifact
# #             self.transform_art = data_transformation_artifact
# #             self.client       = MlflowClient()
# #             os.makedirs(self.cfg.best_model_dir,  exist_ok=True)
# #             os.makedirs(self.cfg.final_model_dir, exist_ok=True)
# #         except Exception as e:
# #             raise NetworkSecurityException(e, sys) from e

# #     # ── MLflow helpers ───────────────────────────────────────────────────────

# #     def _get_best_run(self):
# #         experiment = mlflow.get_experiment_by_name(self.cfg.experiment_name)
# #         if experiment is None:
# #             raise ValueError(
# #                 f"MLflow experiment not found: '{self.cfg.experiment_name}'. "
# #                 "Ensure ModelTrainer ran and dagshub/mlflow are pointing at the same URI."
# #             )
# #         runs = self.client.search_runs(
# #             experiment_ids=[experiment.experiment_id],
# #             filter_string="attributes.status = 'FINISHED'",
# #         )
# #         valid_runs = [r for r in runs if self.cfg.metric_name in r.data.metrics]
# #         if not valid_runs:
# #             raise ValueError(
# #                 f"No finished runs contain metric '{self.cfg.metric_name}' "
# #                 f"in experiment '{self.cfg.experiment_name}'."
# #             )
# #         best_run = max(
# #             valid_runs,
# #             key=lambda r: (r.data.metrics[self.cfg.metric_name], r.info.end_time or 0),
# #         )
# #         log.info(
# #             "Best historical run — id=%s  %s=%.6f  model=%s",
# #             best_run.info.run_id,
# #             self.cfg.metric_name,
# #             best_run.data.metrics[self.cfg.metric_name],
# #             best_run.data.tags.get("model_name", "unknown"),
# #         )
# #         return best_run

# #     # ── Param casting ────────────────────────────────────────────────────────

# #     def _cast_params(self, raw: Dict[str, str]) -> Dict[str, Any]:
# #         """Convert MLflow string params to int / float / str as appropriate."""
# #         result = {}
# #         for k, v in raw.items():
# #             if k in _INT_PARAMS:
# #                 try:    result[k] = int(v)
# #                 except: result[k] = v
# #             elif k in _FLOAT_PARAMS:
# #                 try:    result[k] = float(v)
# #                 except: result[k] = v
# #             else:
# #                 # auto-detect: try int → float → keep string
# #                 try:    result[k] = int(v)
# #                 except:
# #                     try:    result[k] = float(v)
# #                     except: result[k] = v
# #         return result

# #     # ── Model loading ────────────────────────────────────────────────────────

# #     def _load_local_bundle(self) -> Tuple[Any, Any, str]:
# #         """Unpack model + preprocessor from the current run's NetworkModel pkl."""
# #         local_path = self.trainer_art.trained_model_file_path
# #         log.info("Loading current-run model from: %s", local_path)
# #         with open(local_path, "rb") as fh:
# #             network_model = pickle.load(fh)
# #         log.info("Local model: %s", network_model.model.__class__.__name__)
# #         return network_model.model, network_model.preprocessor, local_path

# #     def _retrain_with_historical_params(
# #         self, model_name: str, raw_params: Dict[str, str]
# #     ) -> Tuple[Any, Any]:
# #         """
# #         Rebuild the historical best model class with its logged params,
# #         retrain on current run's full training data, return (model, preprocessor).
# #         Preprocessor is always taken from the current run — it was fit on current data.
# #         """
# #         if model_name not in MODEL_REGISTRY:
# #             log.warning(
# #                 "model_name='%s' not in MODEL_REGISTRY — falling back to current run model.",
# #                 model_name,
# #             )
# #             model, preprocessor, _ = self._load_local_bundle()
# #             return model, preprocessor

# #         params       = self._cast_params(raw_params)
# #         model_class  = MODEL_REGISTRY[model_name]

# #         # CatBoost needs verbose=0 to stay quiet
# #         if model_class is CatBoostClassifier:
# #             params.setdefault("verbose", 0)

# #         log.info("Retraining %s with historical params: %s", model_name, params)
# #         model = model_class(**params)

# #         train_arr = load_numpy_array_data(self.transform_art.transformed_train_file_path)
# #         model.fit(train_arr[:, :-1], train_arr[:, -1])
# #         log.info("Retrained %s on full training data", model_name)

# #         # Load preprocessor from current run's NetworkModel
# #         local_path = self.trainer_art.trained_model_file_path
# #         with open(local_path, "rb") as fh:
# #             network_model = pickle.load(fh)

# #         return model, network_model.preprocessor

# #     # ── Save helpers ─────────────────────────────────────────────────────────

# #     def _save_bundle(self, model: Any, preprocessor: Optional[Any]) -> str:
# #         bundle = {"model": model, "preprocessor": preprocessor}
# #         save_object(file_path=self.cfg.best_model_file_path, obj=bundle)
# #         log.info("Model bundle saved to: %s", self.cfg.best_model_file_path)
# #         return self.cfg.best_model_file_path

# #     def _write_metadata(self, artifact: BestModelArtifact, source: str) -> None:
# #         payload = {
# #             "experiment_name":          artifact.experiment_name,
# #             "run_id":                   artifact.run_id,
# #             "model_name":               artifact.model_name,
# #             "model_class_name":         artifact.model_class_name,
# #             "metric_name":              artifact.metric_name,
# #             "metric_value":             artifact.metric_value,
# #             "selection_source":         source,           # ← audit trail
# #             "mlflow_model_uri":         artifact.mlflow_model_uri,
# #             "selected_model_file_path": artifact.selected_model_file_path,
# #             "preprocessor_file_path":   artifact.preprocessor_file_path,
# #             "params":                   artifact.params,
# #             "tags":                     artifact.tags,
# #         }
# #         with open(self.cfg.metadata_file_path, "w", encoding="utf-8") as fh:
# #             json.dump(payload, fh, indent=2, default=str)
# #         log.info("Metadata written to: %s", self.cfg.metadata_file_path)

# #     # ── Entry point ──────────────────────────────────────────────────────────

# #     def initiate_model_selector(self) -> BestModelArtifact:
# #         try:
# #             log.info("Initiating Model Selection")

# #             best_run       = self._get_best_run()
# #             historical_f1  = best_run.data.metrics[self.cfg.metric_name]
# #             current_f1     = self.trainer_art.test_metric_artifact.f1_score

# #             log.info(
# #                 "F1 comparison — current run: %.6f  |  historical best: %.6f",
# #                 current_f1, historical_f1,
# #             )

# #             if current_f1 >= historical_f1:
# #                 # ── Current run is best or tied ──────────────────────────────
# #                 log.info("Current run is best — loading from local artifact")
# #                 model, preprocessor, prep_path = self._load_local_bundle()
# #                 source   = "current_run_local"
# #                 run_id   = best_run.info.run_id   # may differ; log the true best's id

# #             else:
# #                 # ── Historical run is better — retrain with its params ───────
# #                 hist_model_name = best_run.data.tags.get("model_name")
# #                 hist_params     = best_run.data.params

# #                 log.info(
# #                     "Historical run is better — retraining '%s' with params: %s",
# #                     hist_model_name, hist_params,
# #                 )

# #                 if not hist_params:
# #                     log.warning("Historical run has no logged params — falling back to current run model")
# #                     model, preprocessor, prep_path = self._load_local_bundle()
# #                     source = "current_run_fallback_no_params"
# #                 else:
# #                     model, preprocessor = self._retrain_with_historical_params(
# #                         hist_model_name, hist_params
# #                     )
# #                     prep_path = self.trainer_art.trained_model_file_path
# #                     source    = "retrained_historical_params"

# #                 run_id = best_run.info.run_id

# #             selected_path = self._save_bundle(model, preprocessor)
# #             model_uri     = f"runs:/{run_id}/model"

# #             artifact = BestModelArtifact(
# #                 experiment_name          = self.cfg.experiment_name,
# #                 run_id                   = run_id,
# #                 model_name               = best_run.data.tags.get("model_name", "unknown"),
# #                 model_class_name         = model.__class__.__name__,
# #                 metric_name              = self.cfg.metric_name,
# #                 metric_value             = max(current_f1, historical_f1),
# #                 mlflow_model_uri         = model_uri,
# #                 selected_model_file_path = selected_path,
# #                 preprocessor_file_path   = prep_path,
# #                 params                   = dict(best_run.data.params),
# #                 tags                     = dict(best_run.data.tags),
# #             )

# #             self._write_metadata(artifact, source)

# #             log.info(
# #                 "Model Selection complete — %s  source=%s  %s=%.6f",
# #                 artifact.model_class_name,
# #                 source,
# #                 artifact.metric_name,
# #                 artifact.metric_value,
# #             )
# #             return artifact

# #         except Exception as e:
# #             raise NetworkSecurityException(e, sys) from e









# # # import json
# # # import os
# # # import pickle
# # # import sys
# # # from typing import Any, Optional, Tuple

# # # import mlflow
# # # import mlflow.sklearn
# # # from mlflow.tracking import MlflowClient

# # # from network_security.entity.artifact_entity import BestModelArtifact
# # # from network_security.entity.config_entity import ModelSelectorConfig
# # # from network_security.exception.exception import NetworkSecurityException
# # # from network_security.logging.logger import get_logger
# # # from network_security.utils.main_utils.utils import save_object
# # # from network_security.entity.artifact_entity import BestModelArtifact, ModelTrainerArtifact

# # # log = get_logger(__name__)


# # # class ModelSelector:
# # #     """
# # #     Queries the MLflow experiment that ModelTrainer logged to,
# # #     picks the run with the highest metric_name, and saves a
# # #     {model, preprocessor} bundle alongside a JSON metadata file.
# # #     """

# # #     def __init__(
# # #             self,
# # #             model_selector_config: ModelSelectorConfig,
# # #             model_trainer_artifact: ModelTrainerArtifact
# # #     ):
# # #         try:
# # #             log.info("Entered ModelSelector Constructor")
# # #             self.cfg    = model_selector_config
# # #             self.model_trainer_artifact = model_trainer_artifact
# # #             self.client = MlflowClient()
# # #             os.makedirs(self.cfg.best_model_dir, exist_ok=True)
# # #             os.makedirs(self.cfg.final_model_dir, exist_ok=True)

# # #         except Exception as e:
# # #             raise NetworkSecurityException(e, sys) from e

# # #     # ------------------------------------------------------------------
# # #     # Private helpers
# # #     # ------------------------------------------------------------------

# # #     def _get_best_run(self):
# # #         """Return the MLflow run with the highest value of cfg.metric_name."""
# # #         experiment = mlflow.get_experiment_by_name(self.cfg.experiment_name)
# # #         if experiment is None:
# # #             raise ValueError(
# # #                 f"MLflow experiment not found: '{self.cfg.experiment_name}'. "
# # #                 "Ensure ModelTrainer ran and dagshub/mlflow are pointing at the same URI."
# # #             )

# # #         runs = self.client.search_runs(
# # #             experiment_ids=[experiment.experiment_id],
# # #             filter_string="attributes.status = 'FINISHED'",  # skip crashed runs
# # #         )
# # #         valid_runs = [r for r in runs if self.cfg.metric_name in r.data.metrics]

# # #         if not valid_runs:
# # #             raise ValueError(
# # #                 f"No finished runs contain metric '{self.cfg.metric_name}' "
# # #                 f"in experiment '{self.cfg.experiment_name}'."
# # #             )

# # #         # Primary sort: metric value (higher = better).
# # #         # Tie-break: most recently finished run.
# # #         best_run = max(
# # #             valid_runs,
# # #             key=lambda r: (
# # #                 r.data.metrics[self.cfg.metric_name],
# # #                 r.info.end_time or 0,
# # #             ),
# # #         )
# # #         log.info(
# # #             "Best run selected — id=%s  %s=%.6f",
# # #             best_run.info.run_id,
# # #             self.cfg.metric_name,
# # #             best_run.data.metrics[self.cfg.metric_name],
# # #         )
# # #         return best_run

# # #     def _load_model_online(self, run_id: str) -> Tuple[Any, str]:
# # #         """
# # #         Bad code id issues and what not aka SKILL ISSUE
# # #         """
# # #         model_uri = f"runs:/{run_id}/model"
# # #         log.info("Downloading model from DagShub — this may take a moment: %s", model_uri)
# # #         model = mlflow.sklearn.load_model(model_uri)
# # #         log.info("Model download complete: %s", model.__class__.__name__)
# # #         return model, model_uri

# # #     def _load_preprocessor_online(self, run_id: str) -> Tuple[Optional[Any], Optional[str]]:
# # #         """
# # #         Downloads the 'preprocessor' artifact folder logged by ModelTrainer
# # #         (mlflow.log_artifact(..., artifact_path="preprocessor")) and unpickles
# # #         the first .pkl file found inside it.
# # #         Returns (preprocessor_object, local_path) or (None, None) if absent.
# # #         """
# # #         try:
# # #             artifact_dir = self.client.download_artifacts(
# # #                 run_id=run_id, path="preprocessor"
# # #             )
# # #             if not os.path.isdir(artifact_dir):
# # #                 log.warning("Preprocessor artifact directory missing for run_id=%s", run_id)
# # #                 return None, None

# # #             pkl_files = [f for f in os.listdir(artifact_dir) if f.lower().endswith(".pkl")]
# # #             if not pkl_files:
# # #                 log.warning("No .pkl file found in preprocessor artifact for run_id=%s", run_id)
# # #                 return None, None

# # #             preprocessor_path = os.path.join(artifact_dir, pkl_files[0])
# # #             with open(preprocessor_path, "rb") as fh:
# # #                 preprocessor = pickle.load(fh)

# # #             log.info("Preprocessor loaded from: %s", preprocessor_path)
# # #             return preprocessor, preprocessor_path

# # #         except Exception:
# # #             log.warning(
# # #                 "Could not load preprocessor for run_id=%s — continuing without it.",
# # #                 run_id, exc_info=True,
# # #             )
# # #             return None, None

# # #     def _load_model(self, run_id: str) -> Tuple[Any, str]:
# # #         """
# # #         Load model from the locally saved NetworkModel bundle produced by ModelTrainer.
# # #         Avoids downloading from DagShub — remote artifacts may be incomplete for older runs.
# # #         """
# # #         local_path = self.model_trainer_artifact.trained_model_file_path
# # #         log.info("Loading model from local artifact: %s", local_path)

# # #         with open(local_path, "rb") as fh:
# # #             network_model = pickle.load(fh)

# # #         # NetworkModel bundles both model and preprocessor — extract just the model here.
# # #         # Preprocessor is pulled separately in _load_preprocessor.
# # #         model = network_model.model
# # #         model_uri = f"runs:/{run_id}/model"   # kept for metadata/auditing only

# # #         log.info("Model loaded locally: %s", model.__class__.__name__)
# # #         return model, model_uri

# # #     def _load_preprocessor(self, run_id: str) -> Tuple[Optional[Any], Optional[str]]:
# # #         """
# # #         Load preprocessor from the locally saved NetworkModel bundle.
# # #         """
# # #         try:
# # #             local_path = self.model_trainer_artifact.trained_model_file_path
# # #             with open(local_path, "rb") as fh:
# # #                 network_model = pickle.load(fh)

# # #             preprocessor = network_model.preprocessor
# # #             if preprocessor is None:
# # #                 log.warning("Preprocessor is None inside NetworkModel for run_id=%s", run_id)
# # #                 return None, None

# # #             log.info("Preprocessor loaded from local NetworkModel bundle")
# # #             return preprocessor, local_path

# # #         except Exception:
# # #             log.warning(
# # #                 "Could not load preprocessor from local bundle for run_id=%s — continuing without it.",
# # #                 run_id, exc_info=True,
# # #             )
# # #             return None, None

# # #     def _save_bundle(self, model: Any, preprocessor: Optional[Any]) -> str:
# # #         """Persist {model, preprocessor} dict to best_model_file_path."""
# # #         bundle = {"model": model, "preprocessor": preprocessor}
# # #         save_object(file_path=self.cfg.best_model_file_path, obj=bundle)
# # #         log.info("Model bundle saved to: %s", self.cfg.best_model_file_path)
# # #         return self.cfg.best_model_file_path

# # #     def _write_metadata(self, artifact: BestModelArtifact) -> None:
# # #         """Dump artifact fields to JSON for easy inspection / auditing."""
# # #         payload = {
# # #             "experiment_name":          artifact.experiment_name,
# # #             "run_id":                   artifact.run_id,
# # #             "model_name":               artifact.model_name,
# # #             "model_class_name":         artifact.model_class_name,
# # #             "metric_name":              artifact.metric_name,
# # #             "metric_value":             artifact.metric_value,
# # #             "mlflow_model_uri":         artifact.mlflow_model_uri,
# # #             "selected_model_file_path": artifact.selected_model_file_path,
# # #             "preprocessor_file_path":   artifact.preprocessor_file_path,
# # #             "params":                   artifact.params,
# # #             "tags":                     artifact.tags,
# # #         }
# # #         with open(self.cfg.metadata_file_path, "w", encoding="utf-8") as fh:
# # #             json.dump(payload, fh, indent=2, default=str)
# # #         log.info("Metadata written to: %s", self.cfg.metadata_file_path)

# # #     # ------------------------------------------------------------------
# # #     # Public entry point  (mirrors initiate_* convention)
# # #     # ------------------------------------------------------------------

# # #     def initiate_model_selector(self) -> BestModelArtifact:
# # #         try:
# # #             log.info("Initiating Model Selection")

# # #             best_run   = self._get_best_run()
# # #             run_id     = best_run.info.run_id

# # #             model, model_uri          = self._load_model(run_id)
# # #             preprocessor, prep_path   = self._load_preprocessor(run_id)
# # #             selected_path             = self._save_bundle(model, preprocessor)

# # #             artifact = BestModelArtifact(
# # #                 experiment_name          = self.cfg.experiment_name,
# # #                 run_id                   = run_id,
# # #                 model_name               = best_run.data.tags.get("model_name", "unknown"),
# # #                 model_class_name         = model.__class__.__name__,
# # #                 metric_name              = self.cfg.metric_name,
# # #                 metric_value             = float(best_run.data.metrics[self.cfg.metric_name]),
# # #                 mlflow_model_uri         = model_uri,
# # #                 selected_model_file_path = selected_path,
# # #                 preprocessor_file_path   = prep_path,
# # #                 params                   = dict(best_run.data.params),
# # #                 tags                     = dict(best_run.data.tags),
# # #             )

# # #             self._write_metadata(artifact)

# # #             log.info(
# # #                 "Model Selection complete — %s  (run_id=%s  %s=%.6f)",
# # #                 artifact.model_class_name,
# # #                 artifact.run_id,
# # #                 artifact.metric_name,
# # #                 artifact.metric_value,
# # #             )
# # #             return artifact

# # #         except Exception as e:
# # #             raise NetworkSecurityException(e, sys) from e