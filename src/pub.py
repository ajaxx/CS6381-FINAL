from src.Utilities import alpaca
from src.Utilities import kafka_helper

from kafka import KafkaProducer

class Sub:

    k_utility: kafka_helper.Kafka
    producer: KafkaProducer

    topic: str

    def __init__(self, topic: str):
        
        self.topic = topic

        self.k_utility = kafka_helper.Kafka('10.0.0.1', '2181')
        self.producer = self.k_utility.get_producer(self.topic)

    