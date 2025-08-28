import os
import uvicorn
from fastapi import FastAPI
from Persister.src.connector import MongoConnector

ADDRESS = os.getenv("MONGO_ADDRESS","mongodb://localhost:27017/")
COLL_ANTI = "tweets_antisemitic"
COLL_NOT_ANTI = "tweets_not_antisemitic"
DB_NAME = "my_ready_data"

app = FastAPI()

def serialize_doc(doc):
    doc["_id"] = str(doc["_id"])
    return doc

@app.get("/antisemitic")
def get_antisemitic():
    conn = MongoConnector(ADDRESS, DB_NAME, COLL_ANTI)
    try:
        docs = list(conn.coll.find({}))
        return [serialize_doc(docs) for doc in docs]
    except Exception as e:
        return {"error": str(e)}

@app.get("/not_antisemitic")
def get_not_antisemitic():
    conn = MongoConnector(ADDRESS, DB_NAME, COLL_NOT_ANTI)
    try:
        docs = list(conn.coll.find({}))
        return [serialize_doc(docs) for doc in docs]
    except Exception as e:
        return {"error": str(e)}

if __name__ == '__main__':
    uvicorn.run("server:app",host="127.0.0.1",port=8000)