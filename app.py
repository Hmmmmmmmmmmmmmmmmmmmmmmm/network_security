import asyncio
import logging
import os
import re
import sys
import threading
from datetime import datetime
from typing import Optional

import certifi
import pymongo
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.templating import Jinja2Templates
from uvicorn import run as app_run

load_dotenv()
ca = certifi.where()
mongo_db_url = os.getenv("MONGO_DB_URL")

from network_security.constant.training_pipeline import (
    DATA_INGESTION_COLLECTION_NAME,
    DATA_INGESTION_DATABASE_NAME,
    DEFAULT_TEST_FILE,
    FINAL_MODEL_PATH,
    PREDICTED_OUTPUT_DIR,
    PREDICTED_OUTPUT_FILE_NAME,
)
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger
from network_security.pipelines.training_pipeline import TrainingPipeline
from network_security.utils.main_utils.utils import load_object

log = get_logger(__name__)

import json
from fastapi import HTTPException
from fastapi.responses import FileResponse

# ── MongoDB ──────────────────────────────────────────────────────────────────
client   = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
database = client[DATA_INGESTION_DATABASE_NAME]
collection = database[DATA_INGESTION_COLLECTION_NAME]

# ── FastAPI ──────────────────────────────────────────────────────────────────
app = FastAPI(title="NetGuard ML", description="Network Security Threat Detection API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  LIVE TRAINING STATE                                                     ║
# ╚══════════════════════════════════════════════════════════════════════════╝

_state_lock = threading.Lock()

def _fresh_state() -> dict:
    return {
        "running":          False,
        "done":             False,
        "success":          False,
        "error":            None,
        "started_at":       None,
        "finished_at":      None,
        # pipeline stage tracking
        "stage":            "idle",
        "stages_done":      [],
        # model tracking
        "models_evaluated": [],
        "top_5":            [],
        "tuning":           [],
        "best_model":       None,
        "best_f1":          None,
        "best_params":      None,
        # final selection
        "final_model":      None,
        "final_f1":         None,
        "selection_source": None,
        # raw log feed
        "logs":             [],
    }

training_state: dict = _fresh_state()


# ── Log capture handler ───────────────────────────────────────────────────────

class _LogCaptureHandler(logging.Handler):
    """
    Injected into specific named loggers at startup.
    Appends structured entries to training_state["logs"] and parses
    key messages to update the structured fields (stage, models, etc.).
    Thread-safe via _state_lock.
    """

    # Stage sentinel phrases mapped to stage key
    _STAGE_MAP = [
        ("Initiating Data Ingestion",       "data_ingestion"),
        ("Initiating Data Validation",      "data_validation"),
        ("Initializing Data Validation",    "data_validation"),
        ("Initiating Data Transformation",  "data_transformation"),
        ("Initializing Data Transformation","data_transformation"),
        ("Initiating Model Training",       "model_training"),
        ("Initiating Model Selection",      "model_selection"),
        ("Training Pipeline completed",     "done"),
    ]

    _STAGE_ORDER = [
        "data_ingestion", "data_validation", "data_transformation",
        "model_training", "model_selection", "done",
    ]

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        entry = {
            "time":    datetime.fromtimestamp(record.created).strftime("%H:%M:%S"),
            "level":   record.levelname,
            "module":  record.name.split(".")[-1],
            "message": msg,
        }
        with _state_lock:
            training_state["logs"].append(entry)
            self._parse(msg)

    def _parse(self, msg: str) -> None:
        # ── Stage transitions ──────────────────────────────────────────
        for phrase, stage_key in self._STAGE_MAP:
            if phrase in msg:
                prev = training_state["stage"]
                if prev != stage_key and prev in self._STAGE_ORDER:
                    prev_idx = self._STAGE_ORDER.index(prev)
                    if prev_idx >= 0 and prev not in training_state["stages_done"]:
                        training_state["stages_done"].append(prev)
                training_state["stage"] = stage_key
                break

        # ── Models evaluated ──────────────────────────────────────────
        m = re.search(r"Evaluating models: Training (.+?)\.\.\.", msg)
        if m:
            name = m.group(1).strip()
            if name not in training_state["models_evaluated"]:
                training_state["models_evaluated"].append(name)

        # ── Top 5 selected ────────────────────────────────────────────
        m = re.search(r"Top 5 models selected for tuning: (\[.+?\])", msg)
        if m:
            try:
                import ast
                training_state["top_5"] = ast.literal_eval(m.group(1))
            except Exception:
                pass

        # ── Tuning ────────────────────────────────────────────────────
        m = re.search(r"Tuning (.+?)\.\.\.", msg)
        if m:
            name = m.group(1).strip()
            if name not in training_state["tuning"]:
                training_state["tuning"].append(name)

        # ── Best model from tuning ────────────────────────────────────
        m = re.search(r"Validation F1 Score of best model \((.+?)\): ([\d.]+)", msg)
        if m:
            training_state["best_model"] = m.group(1)
            training_state["best_f1"]    = float(m.group(2))

        # ── Final selection result ─────────────────────────────────────
        m = re.search(
            r"Model Selection complete.+?(\w+)\s+source=(\S+)\s+\S+=([0-9.]+)", msg
        )
        if m:
            training_state["final_model"]      = m.group(1)
            training_state["selection_source"] = m.group(2)
            training_state["final_f1"]         = float(m.group(3))


# Attach the capture handler to the loggers we care about.
# Works because logging.getLogger() always returns the same object
# for a given name, even if get_logger() hasn't been called yet.
_capture_handler = _LogCaptureHandler()
_capture_handler.setLevel(logging.INFO)

_WATCHED_LOGGERS = [
    "network_security.components.model_trainer",
    "network_security.components.model_selector",
    "network_security.utils.ml_utils.model.estimator",
    "network_security.pipelines.training_pipeline",
    "network_security.components.data_ingestion",
    "network_security.components.data_validation",
    "network_security.components.data_transformation",
]

for _name in _WATCHED_LOGGERS:
    logging.getLogger(_name).addHandler(_capture_handler)


# ── Background training runner ────────────────────────────────────────────────

def _run_pipeline_sync() -> None:
    """
    Runs entirely in a thread-pool worker.
    Never called on the event loop thread — keeps FastAPI responsive during training.
    """
    import dagshub
    dagshub.init(
        repo_owner=os.getenv("DAGSHUB_REPO_OWNER"),
        repo_name=os.getenv("DAGSHUB_REPO_NAME"),
        mlflow=True,
    )
    pipeline = TrainingPipeline()
    pipeline.run_pipeline()


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  ROUTES                                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════╝

# ── Pages ─────────────────────────────────────────────────────────────────────

@app.get("/", tags=["pages"])
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={})


@app.get("/predict", tags=["pages"])
async def predict_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="predict.html",
        context={"default_test_file": DEFAULT_TEST_FILE},
    )


# ── Training ──────────────────────────────────────────────────────────────────

@app.get("/train", tags=["training"])
async def train_page(request: Request):
    """Renders the live training dashboard."""
    return templates.TemplateResponse(request=request, name="train.html", context={})


TRAINING_DISABLED = os.getenv("TRAINING_DISABLED", "false").lower() == "true"
FINAL_MODEL_METADATA_PATH = os.path.join("Final_Model", "model_metadata.json")

# Add after existing routes:

@app.get("/train/is-enabled", tags=["training"])
async def train_is_enabled():
    # if TRAINING_DISABLED:
    #     return {
    #         "message": "Training disabled on this deployment",
    #         "started": False
    #     }
    return {"enabled": not TRAINING_DISABLED}

# In /train/start POST — add this check at the very top of the function:
# if TRAINING_DISABLED:
#     return {"message": "Training disabled on this deployment", "started": False}

@app.get("/model/info", tags=["prediction"])
async def model_info():
    if os.path.exists(FINAL_MODEL_METADATA_PATH):
        with open(FINAL_MODEL_METADATA_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

@app.get("/download/model", tags=["prediction"])
async def download_model():
    if not os.path.exists(FINAL_MODEL_PATH):
        raise HTTPException(status_code=404, detail="Model not found. Run training first.")
    return FileResponse(
        path=FINAL_MODEL_PATH,
        filename="best_model.pkl",
        media_type="application/octet-stream"
    )

@app.post("/train/start", tags=["training"])
async def train_start():
    """
    Kicks off the training pipeline in a thread-pool worker.
    Returns immediately (202 Accepted). Client polls /train/status for progress.
    """
    if TRAINING_DISABLED:                                            # ← add
        return {"message": "Training disabled on this deployment", "started": False}  # ← add

    global training_state

    with _state_lock:
        if training_state["running"]:
            return {"message": "Training already in progress", "started": False}
        # Reset state for this run
        training_state = _fresh_state()
        training_state["running"]    = True
        training_state["started_at"] = datetime.now().isoformat()

    async def _run():
        global training_state
        try:
            await run_in_threadpool(_run_pipeline_sync)
            with _state_lock:
                training_state.update({
                    "running":     False,
                    "done":        True,
                    "success":     True,
                    "stage":       "done",
                    "finished_at": datetime.now().isoformat(),
                })
        except Exception as exc:
            with _state_lock:
                training_state.update({
                    "running":     False,
                    "done":        True,
                    "success":     False,
                    "error":       str(exc),
                    "finished_at": datetime.now().isoformat(),
                })

    asyncio.create_task(_run())
    log.info("Training pipeline started via /train/start")
    return {"message": "Training started", "started": True}


@app.get("/train/status", tags=["training"])
async def train_status():
    """Polled by train.html every 2 seconds to update the live dashboard."""
    with _state_lock:
        # Return a snapshot — shallow copy is safe for JSON-serialisable dict
        return dict(training_state)


# ── Prediction ────────────────────────────────────────────────────────────────

@app.post("/predict", tags=["prediction"])
async def predict_route(
    request:     Request,
    use_default: bool                   = Form(False),
    file:        Optional[UploadFile]   = File(None),
):
    try:
        log.info("Prediction route called")

        if use_default:
            log.info("Using default test file: %s", DEFAULT_TEST_FILE)
            if not os.path.exists(DEFAULT_TEST_FILE):
                raise FileNotFoundError(f"Default file not found: {DEFAULT_TEST_FILE}")
            X_new = pd.read_csv(DEFAULT_TEST_FILE)
        elif file is not None and file.filename:
            X_new = pd.read_csv(file.file)
        else:
            return templates.TemplateResponse(
                request=request,
                name="predict.html",
                context={
                    "error": "Please upload a CSV file or choose the default test file.",
                    "default_test_file": DEFAULT_TEST_FILE,
                },
                status_code=400,
            )

        log.info("Input CSV loaded — %d rows", len(X_new))
        network_model = load_object(FINAL_MODEL_PATH)
        y_pred = network_model.predict(X_new)
        X_new["predicted_column"] = y_pred

        representational_html = X_new.to_html(classes="table", index=False, border=0)

        # Save timestamped CSV
        timestamp        = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
        output_dir       = os.path.join(PREDICTED_OUTPUT_DIR, timestamp)
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, PREDICTED_OUTPUT_FILE_NAME)
        X_new.to_csv(output_file_path, index=False)
        log.info("Predictions saved to %s", output_file_path)

        return templates.TemplateResponse(
            request=request,
            name="table.html",
            context={"table": representational_html},
        )

    except Exception as exc:
        log.error("Prediction failed", exc_info=True)
        raise NetworkSecurityException(exc, sys) from exc


# ── Status/ Misc ───────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app_run(app=app, host="0.0.0.0", port=8000)
