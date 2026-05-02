from network_security.constant.training_pipeline import MODEL_FILE_NAME, SAVED_MODEL_DIR
from network_security.entity.artifact_entity import ClassificationMetricArtifact

import os, sys

from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger

log = get_logger(__name__)




# ML libraries:
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
    ConfusionMatrixDisplay
)
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import RandomizedSearchCV, GridSearchCV
from sklearn.preprocessing import label_binarize
# ML_functions:
def evaluate_classifiers(
    X_train, y_train,
    X_test, y_test,
    models: dict,
    verbose=True,
    logs=True,
    save_results=False
) -> pd.DataFrame:

    results = []
    is_binary = len(np.unique(y_train)) == 2

    for name, model in models.items():
        if verbose:
            print(f"Training {name}...")
        if logs:
            log.info(f"Evaluating models: Training {name}...")

        # Train
        model.fit(X_train, y_train)

        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)

        # Try to get probabilities
        y_proba = None
        if hasattr(model, "predict_proba"):
            y_proba = model.predict_proba(X_test)
        elif hasattr(model, "decision_function"):
            y_proba = model.decision_function(X_test)

        # Base metrics
        result = {
            "Model": name,
            "Train Accuracy": accuracy_score(y_train, y_train_pred),
            "Test Accuracy": accuracy_score(y_test, y_test_pred),
            "Precision": precision_score(y_test, y_test_pred, average="weighted", zero_division=0),
            "Recall": recall_score(y_test, y_test_pred, average="weighted", zero_division=0),
            "F1 Score": f1_score(y_test, y_test_pred, average="weighted", zero_division=0)
        }

        # ROC-AUC & PR-AUC
        try:
            if y_proba is not None:
                if is_binary:
                    # Use probability of positive class
                    y_score = y_proba[:, 1] if y_proba.ndim > 1 else y_proba
                    result["ROC-AUC"] = roc_auc_score(y_test, y_score)
                    result["PR-AUC"] = average_precision_score(y_test, y_score)
                else:
                    # Multiclass
                    result["ROC-AUC"] = roc_auc_score(y_test, y_proba, multi_class="ovr", average="weighted")
                    result["PR-AUC"] = average_precision_score(
                        pd.get_dummies(y_test),
                        y_proba,
                        average="weighted"
                    )
            else:
                result["ROC-AUC"] = np.nan
                result["PR-AUC"] = np.nan
        except Exception as e:
            result["ROC-AUC"] = np.nan
            result["PR-AUC"] = np.nan
            if logs:
                log.warning(f"{name}: AUC calculation failed - {e}")

        results.append(result)

    # DataFrame
    results_df = pd.DataFrame(results)

    # Sort (you can change this depending on priority)
    results_df.sort_values(by="F1 Score", ascending=False, inplace=True)
    results_df.reset_index(drop=True, inplace=True)

    # Save results
    if save_results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = os.path.join(PROJECT_ROOT, "artifacts", "reports")
        os.makedirs(report_dir, exist_ok=True)

        csv_path = os.path.join(report_dir, f"classifier_scores_{timestamp}.csv")
        results_df.to_csv(csv_path, index=False)

        if logs:
            log.info(f"Saved classifier scores to {csv_path}")

        # Plot
        plt.figure(figsize=(10, 6))
        sns.barplot(data=results_df, x="Test Accuracy", y="Model")
        plt.title("Classifier Comparison (Accuracy)")
        plt.tight_layout()

        plot_path = os.path.join(report_dir, f"classifier_comparison_{timestamp}.png")
        plt.savefig(plot_path)
        plt.close()

        if logs:
            log.info(f"Saved classifier comparison plot to {plot_path}")

    return results_df

def get_classification_metrics(
    y_true,
    y_pred,
    y_score=None,   # probabilities or decision scores
    model_name=None,
    verbose=True,
    plot=False,
    save_dir=None
):
    """
    Compute classification metrics (binary + multiclass safe)

    returns: artifact, classification matrix, result array

    result array:
    [Accuracy, Precision, Recall, F1 Score, ROC-AUC, PR-AUC]

    artifact: ClassificationMetricArtifact(
        accuracy_score=results.get("Accuracy", np.nan),
        f1_score=results.get("F1 Score", np.nan),
        precision_score=results.get("Precision", np.nan),
        recall_score=results.get("Recall", np.nan),
        roc_auc_score=results.get("ROC-AUC", np.nan),
        average_precision_score=results.get("PR-AUC", np.nan)
    )
    """

    results = {}

    # Detect binary vs multi-class
    is_binary = len(np.unique(y_true)) == 2

    # Basic metrics
    results["Accuracy"] = accuracy_score(y_true, y_pred)
    results["Precision"] = precision_score(y_true, y_pred, average="weighted", zero_division=0)
    results["Recall"] = recall_score(y_true, y_pred, average="weighted", zero_division=0)
    results["F1 Score"] = f1_score(y_true, y_pred, average="weighted", zero_division=0)

    # AUC metrics
    try:
        if y_score is not None:
            if is_binary:
                # handle shape
                if isinstance(y_score, np.ndarray) and y_score.ndim > 1:
                    y_score_ = y_score[:, 1]
                else:
                    y_score_ = y_score

                results["ROC-AUC"] = roc_auc_score(y_true, y_score_)
                results["PR-AUC"] = average_precision_score(y_true, y_score_)

            else:# multi-class
                results["ROC-AUC"] = roc_auc_score(
                    y_true,
                    y_score,
                    multi_class="ovr",
                    average="weighted"
                )

                # one-hot encode y_true
                classes = np.unique(y_true)
                y_true_oh = label_binarize(y_true, classes=classes)

                results["PR-AUC"] = average_precision_score(
                    y_true_oh,
                    y_score,
                    average="weighted"
                )
        else:
            results["ROC-AUC"] = np.nan
            results["PR-AUC"] = np.nan

    except Exception as e:
        results["ROC-AUC"] = np.nan
        results["PR-AUC"] = np.nan

    # Confusion Matrix
    cm = confusion_matrix(y_true, y_pred)

    if plot:
        disp = ConfusionMatrixDisplay(confusion_matrix=cm)
        disp.plot(cmap="Blues")
        title = f"Confusion Matrix - {model_name}" if model_name else "Confusion Matrix"
        plt.title(title)
        plt.tight_layout()

        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            path = os.path.join(save_dir, f"cm_{model_name}.png")
            plt.savefig(path)
            plt.close()
        plt.close()

    # Verbose print
    if verbose:
        print(f"\n Metrics for {model_name}:")
        for k, v in results.items():
            print(f"{k}: {v:.4f}" if isinstance(v, float) else f"{k}: {v}")

    artifact = ClassificationMetricArtifact(
        accuracy_score=results.get("Accuracy", np.nan),
        f1_score=results.get("F1 Score", np.nan),
        precision_score=results.get("Precision", np.nan),
        recall_score=results.get("Recall", np.nan),
        roc_auc_score=results.get("ROC-AUC", np.nan),
        average_precision_score=results.get("PR-AUC", np.nan),
    )
    return artifact, cm, results

def tune_models(
    X_train, y_train,
    X_test, y_test,
    models: dict,
    param_grids: dict,
    top_models: list,
    n_iter: int = 100,
    verbose=True,
    logs=True,
    plot = False,
    report_dir = None,
    csv_path = None,
    do_GridSearch = False
):
    tuned_results = {}
    best_models = {}

    log.info("Tuning Models (Hyperparameter Tuning)")

    for name in top_models:
        try:
            if verbose:
                print(f"\nTuning {name}...")
            if logs:
                log.info(f"Tuning {name}...")

            model = models[name]
            params = param_grids.get(name)

            if params is None:
                if verbose:
                    print(f"No param grid for {name}, skipping...")
                continue

            # search = RandomizedSearchCV(
            #     estimator=model,
            #     param_distributions=params,
            #     n_iter=n_iter,
            #     scoring="f1_weighted",
            #     cv=5,
            #     n_jobs=-1,
            #     verbose=1,
            #     random_state=42
            # )
            if do_GridSearch:
                if verbose:
                    print("Using GridSearchCV...")
                search = GridSearchCV(
                    estimator=model,
                    param_grid=params,
                    scoring="f1_weighted",
                    cv=5,
                    n_jobs=-1,
                    verbose=1
                )
            else:
                if verbose:
                    print("Using RandomizedSearchCV...")
                search = RandomizedSearchCV(
                    estimator=model,
                    param_distributions=params,
                    n_iter=n_iter,
                    scoring="f1_weighted",
                    cv=5,
                    n_jobs=-1,
                    verbose=1,
                    random_state=42
                )

            search.fit(X_train, y_train)

            best_model = search.best_estimator_

            # Predictions
            y_pred = best_model.predict(X_test)

            # Probabilities / scores
            y_score = None
            if hasattr(best_model, "predict_proba"):
                y_score = best_model.predict_proba(X_test)
            elif hasattr(best_model, "decision_function"):
                y_score = best_model.decision_function(X_test)

            metrics,_,_ = get_classification_metrics(
                y_test,
                y_pred,
                y_score=y_score,
                model_name=name,
                verbose=True,
                plot=False
            )

            tuned_results[name] = {
                "best_params": search.best_params_,
                "metrics": metrics
            }

            best_models[name] = best_model

        except Exception as e:
            log.error(f"Error tuning {name}: {e}", exc_info=True)
            if verbose:
                print(f"Error tuning {name}: {e}")
            continue
    # os.makedirs(report_dir, exist_ok=True)

    tuned_list = []
    for model, data in tuned_results.items():
        metrics = data["metrics"]
        row = {
            "Model": model,
            "Accuracy": metrics.accuracy_score,
            "F1 Score": metrics.f1_score,
            "Precision": metrics.precision_score,
            "Recall": metrics.recall_score,
            "ROC-AUC": metrics.roc_auc_score,
            "PR-AUC": metrics.average_precision_score,
            "Best Params": str(data["best_params"])
        }
        tuned_list.append(row)

    tuned_df = pd.DataFrame(tuned_list)
    tuned_df.sort_values(by="F1 Score", ascending=False, inplace=True)
    # if csv_path:
    #     tuned_df.to_csv(csv_path, index=False)
    #     if logs:
    #         log.info(f"Saved tuned model scores to {csv_path}")
    # if plot:
    #     plt.figure(figsize=(10, 6))
    #     sns.barplot(data=tuned_df, x="F1 Score", y="Model")
    #     plt.title("Tuned Model Comparison (F1 Score)")
    #     plt.tight_layout()

    #     plot_path = os.path.join(report_dir, f"tuned_model_comparison.png")
    #     plt.savefig(plot_path)
    #     plt.close()
    #     if logs:
    #         log.info(f"Saved tuned comparison plot to {plot_path}")

    return tuned_results, best_models

class NetworkModel:
    def __init__(self, preprocessor, model):
        try:
            self.preprocessor = preprocessor
            self.model = model
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

    def predict(self, X):
        try:
            x_transform = self.preprocessor.transform(X)
            y_hat = self.model.predict(x_transform)
            return y_hat
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e
