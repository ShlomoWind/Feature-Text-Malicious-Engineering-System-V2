from pymongo import MongoClient

class MongoConnector:
    def __init__(self,url,db_name,col_name):
        self.client = MongoClient(url)
        self.db = self.client[db_name]
        self.coll = self.db[col_name]