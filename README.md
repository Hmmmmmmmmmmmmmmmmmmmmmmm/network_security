# NetGuard ML — Network Security Threat Detection

An end-to-end machine learning pipeline for network intrusion detection. Ingests live traffic data from MongoDB, trains and compares 11 classifier models with automated hyperparameter tuning, tracks every experiment on DagShub via MLflow, and serves real-time threat predictions through a FastAPI web application.

**Best recorded F1 Score: 0.9792 (XGBoost)**

---

## Features

- **Full ML pipeline** — data ingestion → validation → transformation → training → model selection, orchestrated as a single pipeline class
- **11 models evaluated** per run: Random Forest, XGBoost, CatBoost, AdaBoost, Gradient Boosting, Decision Tree, KNN, SVC, Logistic Regression, Ridge Classifier, SGD Classifier
- **GridSearchCV hyperparameter tuning** on the top 5 models per run, with YAML-configured parameter grids
- **Cross-run model selection** — after each run, the selector queries all historical MLflow runs on DagShub and retrains with the best-ever parameters if they outperform the current run. No run's insights are wasted.
- **Live training dashboard** — browser UI that polls training progress every 2 seconds, showing which models are being evaluated and tuned in real time
- **Experiment tracking** — every run logged to DagShub (metrics, params, tags, preprocessor artifact)
- **KNN imputation** — missing value handling via KNNImputer (k=3) fit during transformation and bundled with the model
- **NetworkModel wrapper** — preprocessor and model bundled into a single object with a `predict()` interface, simplifying inference to one call
- **FastAPI serving** — `/train` dashboard, `/predict` upload interface, auto-generated `/docs`

---

## Architecture

```
MongoDB Atlas
     │
     ▼
Data Ingestion ──► Data Validation ──► Data Transformation
                                              │
                                              ▼
                                    Model Trainer
                                    ├─ Evaluate 11 models
                                    ├─ Select Top 5
                                    ├─ GridSearchCV tuning
                                    └─ Retrain best on full data
                                              │
                                              ▼
                                    Model Selector
                                    ├─ Query DagShub MLflow (all runs)
                                    ├─ Compare current vs historical best
                                    ├─ If historical wins → retrain with
                                    │  its params (via ModelTrainer)
                                    └─ Save NetworkModel → Final_Model/
                                              │
                                        FastAPI App
                                    ├─ GET  /        → Landing page
                                    ├─ GET  /train   → Training dashboard
                                    ├─ POST /train/start → Run pipeline
                                    ├─ GET  /train/status → Live progress
                                    ├─ GET  /predict  → Upload form
                                    └─ POST /predict  → Results table
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| ML | scikit-learn, XGBoost, CatBoost |
| Experiment tracking | MLflow, DagShub |
| Data store | MongoDB Atlas |
| API | FastAPI, Uvicorn |
| Templating | Jinja2 |
| Data processing | pandas, NumPy |
| Environment | python-dotenv, certifi |

---

## Project Structure

```
network_security/
├── components/
│   ├── data_ingestion.py
│   ├── data_validation.py
│   ├── data_transformation.py
│   ├── model_trainer.py        # Training + train_single_model()
│   └── model_selector.py       # Cross-run selection logic
├── constant/
│   └── training_pipeline.py    # All path/name constants
├── entity/
│   ├── artifact_entity.py      # Dataclasses for pipeline outputs
│   └── config_entity.py        # Dataclasses for pipeline configs
├── exception/
│   └── exception.py
├── logging/
│   └── logger.py
├── pipelines/
│   └── training_pipeline.py    # Orchestrates all components
└── utils/
    ├── main_utils/utils.py
    └── ml_utils/
        ├── metrics/
        └── model/estimator.py  # NetworkModel, evaluate_classifiers, tune_models

templates/
├── index.html                  # Landing page
├── train.html                  # Live training dashboard
├── predict.html                # Upload / default file form
└── table.html                  # Prediction results

Final_Model/
└── best_model.pkl              # NetworkModel (preprocessor + model)

app.py                          # FastAPI application
param_grids.yaml                # Hyperparameter search spaces
```

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/your-username/network-security-ml.git
cd network-security-ml
pip install -r requirements.txt
```

### 2. Set environment variables

Create a `.env` file in the project root:

```env
MONGO_DB_URL=mongodb+srv://username:password@cluster.mongodb.net/
DAGSHUB_REPO_OWNER=your-dagshub-username
DAGSHUB_REPO_NAME=network_security
```

### 3. Run

```bash
python app.py
```

Visit `http://localhost:8000`.

---

## Environment Variables

| Variable | Description |
|---|---|
| `MONGO_DB_URL` | MongoDB Atlas connection string |
| `DAGSHUB_REPO_OWNER` | DagShub username |
| `DAGSHUB_REPO_NAME` | DagShub repository name |

Never commit `.env` to version control. It is already in `.gitignore`.

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Landing page |
| `GET` | `/train` | Live training dashboard |
| `POST` | `/train/start` | Start training pipeline (non-blocking) |
| `GET` | `/train/status` | Current training state (JSON) |
| `GET` | `/predict` | Prediction upload form |
| `POST` | `/predict` | Run inference; accepts `file` (CSV upload) or `use_default=true` |
| `GET` | `/docs` | Swagger UI — auto-generated API reference |
| `GET` | `/redoc` | ReDoc — clean API reference |

---

## Training Pipeline — Stage by Stage

**Data Ingestion** — Pulls records from MongoDB Atlas, exports to CSV, performs an 80/20 train/test split.

**Data Validation** — Checks column count against schema. Generates a drift report YAML. Fails fast if schema is violated.

**Data Transformation** — Fits a `KNNImputer(n_neighbors=3)` pipeline on training data. Saves as `preprocessor.pkl`. Exports transformed arrays as `.npy` files.

**Model Training** — Evaluates all 11 models on a validation split. Selects top 5 by F1 score. Runs GridSearchCV on those 5 using parameter grids from `param_grids.yaml`. Retrains the best-tuned model on the full training data. Logs all metrics, params, and tags to MLflow/DagShub.

**Model Selection** — Queries DagShub for all historical finished runs. Compares the current run's test F1 against the global best. If the current run wins, loads the local model. If a historical run was better and its params are logged, delegates retraining to `ModelTrainer.train_single_model()`. Saves a `NetworkModel` bundle to `Final_Model/best_model.pkl`.

---

## Model Performance

| Metric | Value |
|---|---|
| Best F1 Score | 0.9792 |
| Best Model | XGBoost |
| Test Accuracy | 97.9% |
| ROC-AUC | 0.9980 |

---
