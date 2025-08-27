from pymongo import MongoClient
import pandas as pd
import os

# MongoDB connection parameters from environment variables
user = os.getenv("USER", "IRGC_NEW")
password = os.getenv("PASS", "iran135")
db_name = os.getenv("DB_NAME", "IranMalDB")
coll_name = os.getenv("COLLECTION_NAME", "tweets")
mongo_url = os.getenv("MONGO_URL", "mongodb+srv://IRGC_NEW:iran135@cluster0.6ycjkak.mongodb.net/")

# Class to fetch data from MongoDB
class DataFetcher:
    def __init__(self):
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]
        self.collection = self.db[coll_name]
        self.place_holder = 0

# Fetches 100 tweets from the collection, skipping already fetched ones
    def get_100_tweets(self):
        data = list(self.collection.find().sort("CreateDate", 1).skip(self.place_holder).limit(100))
        data_list = []
        for doc in data:
            doc['_id'] = str(doc['_id'])
            data_list.append(doc)
        df = pd.DataFrame(data_list)
        self.place_holder += 100
        return df