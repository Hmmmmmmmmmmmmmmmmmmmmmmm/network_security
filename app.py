import sys
import os
import certifi
import pymongo
ca = certifi.where()
from dotenv import load_dotenv
load_dotenv()
mongo_db_url = os.getenv("MONGO_DB_URL")
print(mongo_db_url)

from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger

log = get_logger(__name__)

from network_security.pipelines.training_pipeline import TrainingPipeline
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, File, UploadFile, Request
from uvicorn import run as app_run
from fastapi.responses import Response
from starlette.responses import RedirectResponse
import pandas as pd
from network_security.utils.main_utils.utils import load_object

# client setup in case fresh data ingestion called/ necessary:
client = pymongo.MongoClient(mongo_db_url, tlsCAFile = ca)

from network_security.constant.training_pipeline import(
    DATA_INGESTION_COLLECTION_NAME,
    DATA_INGESTION_DATABASE_NAME
)

database = client[DATA_INGESTION_DATABASE_NAME]
collection = database[DATA_INGESTION_COLLECTION_NAME]

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials = True,
    allow_methods = ["*"],
    allow_headers = ["*"],
)

@app.get("/",tags=["authentication"])
async def index():
    return RedirectResponse(url="/docs")
@app.get("/train")
async def train_route():
    try:
        log.info("Training Route Called via FastAPI")
        log.info("El Psy Congroo")
        train_pipeline = TrainingPipeline()
        train_pipeline.run_pipeline()
        log.info("Training successful")
    except Exception as e:
        log.error("Pipeline Failed! aka: \"Skill Issue\"", exc_info=True)
        raise NetworkSecurityException(e, sys) from e


if __name__ == "__main__":
    app_run(app=app, host="localhost", port=8000)



