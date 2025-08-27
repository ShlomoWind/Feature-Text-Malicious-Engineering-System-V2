import json
import time

from Preprocessor.src.consumer import Consumer
from Retriever.src.publisher import Producer
from Preprocessor.src.processor import Preprocessor

SERVER_ADDRESS = "localhost:9092"
TOPIC_PROCESSED_ANTISEMITIC = "preprocessed_tweets_antisemitic"
TOPIC_PROCESSED_NOT_ANTISEMITIC = "preprocessed_tweets_not_antisemitic"
TOPIC_RAW_ANTISEMITIC = "raw_tweets_antisemitic"
TOPIC_RAW_NOT_ANTISEMITIC = "raw_tweets_not_antisemitic"

producer = Producer(SERVER_ADDRESS)


class Manager:
    def __init__(self):
        self.consumer_antisemitic = Consumer(TOPIC_RAW_ANTISEMITIC,SERVER_ADDRESS)
        self.consumer_not_antisemitic = Consumer(TOPIC_RAW_NOT_ANTISEMITIC,SERVER_ADDRESS)

    def run(self):
        while True:
            try:
                messages_antisemitic = self.consumer_antisemitic.consume()
                for msg in messages_antisemitic:
                    print("Processing antisemitic message...")
                    processed = Preprocessor(msg).process()
                    producer.publish(json.dumps(processed), TOPIC_PROCESSED_ANTISEMITIC)
                    print(f"Published: {processed}")

                messages_not_antisemitic = self.consumer_not_antisemitic.consume()
                for msg in messages_not_antisemitic:
                    print("Processing non-antisemitic message...")
                    processed = Preprocessor(msg).process()
                    producer.publish(json.dumps(processed), TOPIC_PROCESSED_NOT_ANTISEMITIC)
                    print(f"Published: {processed}")

                time.sleep(0.5)
            except Exception as e:
                print(f"Error occurred: {e}")
                time.sleep(1)