from kafka import KafkaConsumer

# Kafka Consumer class
class Consumer:
    def __init__(self,topic,server_address):
        self.topic = topic
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[server_address],
            value_deserializer=lambda x: x.decode('utf-8'),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000)

# Consumes messages from the topic
    def consume(self):
        messages = []
        for message in self.consumer:
            messages.append(message.value)
        return messages