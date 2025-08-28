import os
import uvicorn
from fastapi import FastAPI
from Persister.src.connector import MongoConnector

ADDRESS = os.getenv("MONGO_ADDRESS","mongodb://localhost:27017/")
COLL_ANTI = "tweets_antisemitic"
COLL_NOT_ANTI = "tweets_not_antisemitic"
DB_NAME = "my_ready_data"

app = FastAPI()

@app.get("/antisemitic")
def get_collection():
    conn = MongoConnector(ADDRESS, DB_NAME, COLL_ANTI)
    try:
        return list(conn.coll.find({}))
    except Exception as e:
        return f'Error: {e}'


@app.get("/not_antisemitic")
def get_collection():
    conn = MongoConnector(ADDRESS, DB_NAME, COLL_NOT_ANTI)
    try:
        return list(conn.coll.find({}))
    except Exception as e:
        return f'Error: {e}'
