import time
from utils.consumer import Consumer
from connector import MongoConnector

SERVER_ADDRESS = "localhost:9092"
MONGO_ADDRESS = "mongodb://localhost:27017/"
DB_NAME = "my_ready_data"
COLLECTION_ANTISEMITIC = "tweets_antisemitic"
COLLECTION_NOT_ANTISEMITIC = "tweets_not_antisemitic"
TOPIC_ENRICHED_ANTISEMITIC = "enriched_preprocessed_tweets_antisemitic"
TOPIC_ENRICHED_NOT_ANTISEMITIC = "enriched_preprocessed_tweets_not_antisemitic"

connect = MongoConnector(MONGO_ADDRESS,DB_NAME)

class Manager:
    def __init__(self):
        self.consumer_antisemitic = Consumer(TOPIC_ENRICHED_ANTISEMITIC, SERVER_ADDRESS)
        self.consumer_not_antisemitic = Consumer(TOPIC_ENRICHED_NOT_ANTISEMITIC, SERVER_ADDRESS)
        self.anti_coll = connect.db[COLLECTION_ANTISEMITIC]
        self.not_anti_coll = connect.db[COLLECTION_NOT_ANTISEMITIC]


    def run(self):
        while True:
            try:
                messages_antisemitic = self.consumer_antisemitic.consume()
                for msg in messages_antisemitic:
                    self.anti_coll.insert_one(msg)
                    print(f"inserted to mongo: {msg}")

                messages_not_antisemitic = self.consumer_not_antisemitic.consume()
                for msg in messages_not_antisemitic:
                    self.not_anti_coll.insert_one(msg)
                    print(f"inserted to mongo: {msg}")

                time.sleep(0.5)
            except Exception as e:
                print(f"Error occurred: {e}")
                time.sleep(1)