import os,sys
from pymongo.mongo_client import MongoClient
from dotenv import load_dotenv
from pathlib import Path
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
if not MONGO_DB_URL:
    raise ValueError("MONGO_DB_URL is not set")
import certifi
ca = certifi.where()


# uri = "mongodb+srv://<@username>:<@password>@cluster0.ztemfsl.mongodb.net/?appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(MONGO_DB_URL)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)