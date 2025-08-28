from Retriever.src.publisher import Producer
from fetcher import DataFetcher
import time
import os

# Kafka broker address and topics
address = os.getenv("KAFKA_BROKER", "localhost:9092")
topic_1 = "raw_tweets_antisemitic"
topic_2 = "raw_tweets_not_antisemitic"

# Initialize producer and data fetcher
producer = Producer(address)
fetcher = DataFetcher()

# Continuously fetch and publish tweets
while True:
    df = fetcher.get_100_tweets()
    for idx, row in df.iterrows():
        if row['Antisemitic'] == 1:
            producer.publish(row.to_json(), topic_1)
        else:
            producer.publish(row.to_json(), topic_2)
    time.sleep(60)
