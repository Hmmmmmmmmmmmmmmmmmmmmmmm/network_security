import sys
import os
import certifi
import pymongo
ca = certifi.where()
from dotenv import load_dotenv
load_dotenv()
mongo_db_url = os.getenv("MONGO_DB_URL")
# print(mongo_db_url)

from datetime import datetime
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger

log = get_logger(__name__)

from network_security.pipelines.training_pipeline import TrainingPipeline
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, File, UploadFile, Request,Form
from uvicorn import run as app_run
from fastapi.responses import Response
from starlette.responses import RedirectResponse
import pandas as pd
from typing import Optional
from network_security.utils.main_utils.utils import load_object, save_object

# client setup in case fresh data ingestion called/ necessary:
client = pymongo.MongoClient(mongo_db_url, tlsCAFile = ca)

from network_security.constant.training_pipeline import(
    DATA_INGESTION_COLLECTION_NAME,
    DATA_INGESTION_DATABASE_NAME,
    FINAL_MODEL_PATH,
    PREDICTED_OUTPUT_DIR,
    PREDICTED_OUTPUT_FILE_NAME,
    DEFAULT_TEST_FILE
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

from fastapi.templating import Jinja2Templates
templates = Jinja2Templates(directory="templates")

@app.get("/", tags=["pages"])
async def index(request: Request):
    # return templates.TemplateResponse(
    #     "index.html",
    #     {"request": request}
    # )
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={}
    )

# @app.get("/",tags=["authentication"])
# async def index():
#     return RedirectResponse(url="/docs")


@app.get("/predict", tags=["pages"])
async def predict_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="predict.html",
        context={
            "default_test_file": DEFAULT_TEST_FILE
        }
    )


@app.get("/train")
async def train_route():
    try:
        log.info("Training Route Called via FastAPI")
        log.info("Commence operation Training - El Psy Congroo")
        train_pipeline = TrainingPipeline()
        train_pipeline.run_pipeline()
        log.info("Training successful")
        return Response("Training Successful")
    except Exception as e:
        log.error("Pipeline Failed! aka: \"Skill Issue\"", exc_info=True)
        raise NetworkSecurityException(e, sys) from e

@app.post("/predict")
async def predict_route(
    request: Request,
    use_default: bool = Form(False),
    file: Optional[UploadFile] = File(None),
):
    try:
        log.info("Prediction Route Called via FastAPI")
        log.info("Commence operation Prediction - El Psy Congroo")
        if use_default:
            log.info(f"Using default test file: {DEFAULT_TEST_FILE}")
            if not os.path.exists(DEFAULT_TEST_FILE):
                raise FileNotFoundError(f"Default file not found: {DEFAULT_TEST_FILE}")
            X_new = pd.read_csv(DEFAULT_TEST_FILE)
        else:
            if file is None:
                return templates.TemplateResponse(
                    request=request,
                    name="predict.html",
                    context={
                        "error": "Upload a CSV file or choose the default test file.",
                        "default_test_file": DEFAULT_TEST_FILE,
                    },
                    status_code=400,
                )
            X_new = pd.read_csv(file.file)
        log.info("Input CSV loaded")
        network_model = load_object(FINAL_MODEL_PATH)
        log.info("Network_Model loaded")
        y_pred = network_model.predict(X_new)
        X_new["predicted_column"] = y_pred
        representational_html = X_new.to_html(classes="table", index=False)
        timestamp = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
        output_dir = os.path.join(PREDICTED_OUTPUT_DIR, timestamp)
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, PREDICTED_OUTPUT_FILE_NAME)
        X_new.to_csv(output_file_path, index=False)
        return templates.TemplateResponse(
            request=request,
            name="table.html",
            context={
                "table": representational_html
            }
        )

    except Exception as e:
        log.error("Prediction failed", exc_info=True)
        raise NetworkSecurityException(e, sys) from e

if __name__ == "__main__":
    app_run(app=app, host="localhost", port=8000)






# # @app.post("/predict")
# # async def predict_route(request: Request, file: UploadFile=File(...)):
# @app.post("/predict")
# async def predict_route(
#     request: Request,
#     use_default: bool = Form(False),
#     file: Optional[UploadFile] = File(None),
# ):
#     try:
#         log.info("Prediction Route Called via FastAPI")
#         log.info("Commence operation Prediction - El Psy Congroo")
#         X_new = pd.read_csv(file.file)
#         print(X_new.iloc[0])
#         log.info("X_new file(to be predicted input variable) loaded")
#         network_model = load_object(FINAL_MODEL_PATH)
#         log.info("Network_Model loaded")
#         y_pred = network_model.predict(X_new)
#         print(y_pred)
#         X_new['predicted_column'] = y_pred
#         print(X_new['predicted_column'])
#         log.info("Prediction Completed, proceeding to present output via html")
#         representational_html = X_new.to_html(classes = 'table for-output')
#         timestamp = datetime.now()
#         timestamp = timestamp.strftime("%m_%d_%Y_%H_%M_%S")
#         output_file_path = os.path.join(PREDICTED_OUTPUT_DIR,timestamp)
#         os.makedirs(output_file_path, exist_ok=True)
#         output_file_path = os.path.join(output_file_path,PREDICTED_OUTPUT_FILE_NAME)
#         X_new.to_csv(output_file_path)

#         # return Response("Training Successful"), templates.TemplateResponse("table.html", {
#         #     "request": request,
#         #     "table": representational_html
#         # })
#         # return templates.TemplateResponse("table.html", {
#         #     "request": request,
#         #     "table": representational_html
#         # })
#         return templates.TemplateResponse(
#             request=request,
#             name="table.html",
#             context={
#                 "table": representational_html
#             }
#         )
#     except Exception as e:
#         log.error("Pipeline Failed! aka: \"Skill Issue\"", exc_info=True)
#         raise NetworkSecurityException(e, sys) from e
