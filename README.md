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
- **CI/CD** — GitHub Actions pipeline builds and pushes Docker image on every push to `main`; deploys to AWS EC2 and syncs artifacts to S3

---


## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA LAYER                              │
│  MongoDB Atlas ──► Raw traffic features (11,055 samples)   │
│  AWS S3        ──► Artifacts, trained models, logs          │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   ML PIPELINE                               │
│                                                             │
│  Data Ingestion ──► Validation ──► Transformation           │
│                                         │                   │
│                                    Model Trainer            │
│                                    ├─ 11 models evaluated   │
│                                    ├─ Top 5 selected        │
│                                    ├─ GridSearchCV tuning   │
│                                    └─ Best retrained        │
│                                         │                   │
│                                    Model Selector           │
│                                    ├─ Query DagShub MLflow  │
│                                    ├─ Compare all runs      │
│                                    └─ Save best NetworkModel│
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   SERVING LAYER (FastAPI)                   │
│                                                             │
│  GET  /           ──► Landing page                          │
│  GET  /train      ──► Live training dashboard               │
│  POST /train/start──► Start pipeline (non-blocking)         │
│  GET  /train/status──► Live progress (polled every 2s)      │
│  GET  /predict    ──► Upload form + active model info       │
│  POST /predict    ──► Inference → results table             │
│  GET  /model/info ──► Active model metadata (JSON)          │
│  GET  /download/model ──► Download best_model.pkl           │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   CI/CD (GitHub Actions)                    │
│                                                             │
│  Push to main ──► Build Docker image                        │
│              ──► Push to Docker Hub                         │
│              ──► SSH into EC2, pull & restart container     │
│              ──► Sync artifacts to S3                       │
└─────────────────────────────────────────────────────────────┘
```

---

## Dataset

**File:** `Network_Data/phishingData.csv`  
Academically derived dataset (UCI ML Repository, Mohammad et al.) where each row represents one URL. Raw web properties — URL structure, DNS records, HTML/JS content, and external reputation signals — encoded into a ternary scheme: `{-1, 0, 1}`, where `-1` = phishing indicator, `0` = suspicious, `1` = legitimate.

| Stat | Value |
|---|---|
| Total samples | 11,055 |
| Features | 30 |
| Phishing URLs | 4,898 |
| Legitimate URLs | 6,157 |
| Target column | `Result` (`1` = legitimate, `-1` = phishing) |

---






## Tech Stack

| Layer | Technology |
|---|---|
| ML | scikit-learn, XGBoost, CatBoost |
| Experiment tracking | MLflow, DagShub |
| Data store | MongoDB Atlas |
| Artifact store | AWS S3 |
| Deployment | AWS EC2, Docker |
| CI/CD | GitHub Actions |
| API | FastAPI, Uvicorn |
| Templating | Jinja2 |
| Data processing | pandas, NumPy |

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
│   ├── artifact_entity.py
│   └── config_entity.py
├── pipelines/
│   └── training_pipeline.py
└── utils/

templates/                      # Jinja2 HTML templates
Final_Model/
├── best_model.pkl              # NetworkModel (preprocessor + model)
└── model_metadata.json         # Active model info (served by /model/info)

.github/workflows/
└── main.yaml                   # CI/CD: build → push → deploy

app.py
param_grids.yaml
requirements_docker.txt
Dockerfile
```

---


## Quick Start (Local)

### 1. Clone and install

```bash
git clone https://github.com/your-username/network-security-ml.git
cd network-security-ml
pip install -r requirements.txt
```

### 2. Set environment variables

Create a `.env` file in the project root:

```env
# .env
MONGO_DB_URL=mongodb+srv://username:password@cluster.mongodb.net/
DAGSHUB_REPO_OWNER=your-dagshub-username
DAGSHUB_REPO_NAME=network_security
```

### 3. Run

```bash
python app.py
# → http://localhost:8000
```

---


## Quick Start (Docker)

### Pull and run pre-built image

```bash
docker pull yourdockerhubusername/netguard-ml:latest
```

Create `.env`:
```env
MONGO_DB_URL=mongodb+srv://...
DAGSHUB_REPO_OWNER=your-username
DAGSHUB_REPO_NAME=network_security
TRAINING_DISABLED=true
```

Run:
```bash
docker run --env-file .env -p 8000:8000 yourdockerhubusername/netguard-ml:latest
```

The `best_model.pkl` is bundled in the image — prediction works immediately. Training is disabled by default in the Docker image.

### Build locally

```bash
docker build -t netguard-ml:latest .
docker run --env-file .env -p 8000:8000 -e TRAINING_DISABLED=true netguard-ml:latest
```

---


## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `MONGO_DB_URL` | Yes | MongoDB Atlas connection string |
| `DAGSHUB_REPO_OWNER` | Yes (training) | DagShub username |
| `DAGSHUB_REPO_NAME` | Yes (training) | DagShub repository name |
| `MLFLOW_TRACKING_USERNAME` | If private repo | DagShub username (token auth) |
| `MLFLOW_TRACKING_PASSWORD` | If private repo | DagShub access token |
| `TRAINING_DISABLED` | No | Set `true` to disable training endpoint (used in Docker/Render) |
| `AWS_ACCESS_KEY_ID` | CI/CD only | AWS credentials for S3 + EC2 deployment |
| `AWS_SECRET_ACCESS_KEY` | CI/CD only | AWS credentials |
| `AWS_REGION` | CI/CD only | e.g. `us-east-1` |
| `EC2_HOST` | CI/CD only | Public IP or DNS of EC2 instance |
| `EC2_SSH_KEY` | CI/CD only | Private key for EC2 SSH (stored as GitHub secret) |
| `DOCKER_HUB_USERNAME` | CI/CD only | Docker Hub username |
| `DOCKER_HUB_TOKEN` | CI/CD only | Docker Hub access token |

**To activate CI/CD**: add the above secrets to your GitHub repo under  
`Settings → Secrets and variables → Actions`.

---

## CI/CD Pipeline (GitHub Actions)

On every push to `main`, `.github/workflows/main.yaml` runs:

```
Push to main
    │
    ▼
[Job: build-push]
├── Checkout code
├── Log in to Docker Hub
├── Build Docker image
└── Push to Docker Hub (yourusername/netguard-ml:latest)
    │
    ▼
[Job: deploy] (runs after build-push)
├── SSH into EC2 instance
├── docker pull yourusername/netguard-ml:latest
├── Stop old container
├── Start new container (with env vars)
└── Sync Artifacts/ → s3://your-bucket/artifacts/
```

The `main.yaml` is already configured. To activate it, add the required secrets listed above to your GitHub repo. No other changes needed.

---

## Model Performance

| Metric | Value |
|---|---|
| Best F1 Score | 0.9792 |
| Best Model | XGBoost |
| Test Accuracy | 97.9% |
| ROC-AUC | 0.9980 |
| Average Precision | 0.9983 |

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Landing page |
| `GET` | `/train` | Live training dashboard |
| `POST` | `/train/start` | Start pipeline (non-blocking, returns immediately) |
| `GET` | `/train/status` | Training state JSON (polled every 2s) |
| `GET` | `/train/is-enabled` | Returns `{"enabled": bool}` — false when TRAINING_DISABLED=true |
| `GET` | `/predict` | Prediction form (shows active model info) |
| `POST` | `/predict` | Run inference; `file` upload or `use_default=true` |
| `GET` | `/model/info` | Active model metadata JSON |
| `GET` | `/download/model` | Download `Final_Model/best_model.pkl` |
| `GET` | `/docs` | Swagger UI |
| `GET` | `/redoc` | ReDoc |

---

## Training Pipeline — Stage by Stage

**Data Ingestion** — Pulls records from MongoDB Atlas, exports to CSV, 80/20 train/test split.

**Data Validation** — Schema check (column count). Drift report generated as YAML. Fails fast on schema violation.

**Data Transformation** — `KNNImputer(n_neighbors=3)` fit on training data. Exported as `.npy` arrays.

**Model Training** — 11 models evaluated. Top 5 by F1 selected. GridSearchCV on those 5 using `param_grids.yaml`. Best retrained on full data. All metrics/params/tags logged to DagShub via MLflow.

**Model Selection** — Queries DagShub for all historical finished runs. If a previous run had better test F1, delegates retraining with its params to `ModelTrainer.train_single_model()`. Saves `NetworkModel` to `Final_Model/best_model.pkl` and metadata to `Final_Model/model_metadata.json`.

---

## Limitations

- Dataset is feature-engineered, not raw packet captures
- Training and inference share the same application container
- No authentication or rate limiting on API endpoints
- No async job queue (Celery/Redis) for distributed training
- Designed for research and portfolio demonstration

## Future Improvements

- Separate inference and training services
- WebSocket-based training dashboard (replace polling)
- Real-time URL classification (requires web scraper + pipeline)
- Redis + Celery for distributed background training
- Model registry and rollback support via MLflow Model Registry


## Yap
PS: This project was made to see whats this MLOps and actual code which can be thrown in deployment and can be upgraded without the whole thing going down as it is call "*Modularity*". Started with the course I was following, then I ventured a bit here and there in terms of hyperparameter-tuning (do not do this on a bad machine: speaking from painful experience) and then messing around with FastAPI and everything. I tried to keep it as Human written as possible but when it came to Landing page lets just say Claude knew exactly what I wanted. Hope if you are reading you go through this one and do hit me up regarding how the project was and what could have done better. I wonder if I can claim to know ML after this.

Signing off to touch some grass
Yours truly,
Hmmmmmmmmmmmmmmmmmmmmmmm
