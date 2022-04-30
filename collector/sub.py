from Utilities import kafka_helper

from kafka import KafkaConsumer
from time import sleep

class Sub:

    consumer: KafkaConsumer
    k_utility: kafka_helper.Kafka

    topic: str

    def __init__(self, topic: str):
        
        self.topic = topic

        self.k_utility = kafka_helper.Kafka('10.0.0.1', '2181')
        self.wait_for_topic_creation()

        self.consumer = self.k_utility.get_consumer(self.topic)

    def wait_for_topic_creation(self):

        while True:

            if self.k_utility.topic_exists(self.topic):
                break

            else:
                sleep(1)
                print("waiting for topic to be created")


    def recieve(self):

        for message in self.consumer:
            print(message)