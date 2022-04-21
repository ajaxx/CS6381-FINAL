from src.Utilities import alpaca
from src.Utilities import kafka_helper

from kafka import KafkaConsumer

class Sub:

    consumer: KafkaConsumer
    k_utility: kafka_helper.Kafka

    topic: str

    def __init__(self, topic: str):
        
        self.topic = topic

        self.k_utility = kafka_helper.Kafka('10.0.0.1', '2181')
        self.consumer = self.k_utility.get_consumer(self.topic)

    def recieve(self):

        for message in self.consumer:
            print(message)