import json
import time

from Preprocessor.src.consumer import Consumer
from Retriever.src.publisher import Producer
from enricher import DataEnricher

SERVER_ADDRESS = "localhost:9092"
TOPIC_PROCESSED_ANTISEMITIC = "preprocessed_tweets_antisemitic"
TOPIC_PROCESSED_NOT_ANTISEMITIC = "preprocessed_tweets_not_antisemitic"
TOPIC_ENRICHED_ANTISEMITIC = "enriched_preprocessed_tweets_antisemitic"
TOPIC_ENRICHED_NOT_ANTISEMITIC = "enriched_preprocessed_tweets_not_antisemitic"

prod = Producer(SERVER_ADDRESS)

class Manager:
    def __init__(self):
        self.consumer_antisemitic = Consumer(TOPIC_PROCESSED_ANTISEMITIC,SERVER_ADDRESS)
        self.consumer_not_antisemitic = Consumer(TOPIC_PROCESSED_NOT_ANTISEMITIC,SERVER_ADDRESS)

    def run(self):
        while True:
            try:
                messages_antisemitic = self.consumer_antisemitic.consume()
                for msg in messages_antisemitic:
                    enriched = DataEnricher(msg).enriched()
                    prod.publish(json.dumps(enriched), TOPIC_ENRICHED_ANTISEMITIC)
                    print(f"Published: {enriched}")

                messages_not_antisemitic = self.consumer_not_antisemitic.consume()
                for msg in messages_not_antisemitic:
                    enriched = DataEnricher(msg).enriched()
                    prod.publish(json.dumps(enriched), TOPIC_ENRICHED_NOT_ANTISEMITIC)
                    print(f"Published: {enriched}")

                time.sleep(0.5)
            except Exception as e:
                print(f"Error occurred: {e}")
                time.sleep(1)