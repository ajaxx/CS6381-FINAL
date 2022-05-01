from Utilities import kafka_helper

from kafka import KafkaProducer

class Pub:

    k_utility: kafka_helper.Kafka
    producer: KafkaProducer

    topic: str

    def __init__(self, zookeeper_host: str = '127.0.0.1', zookeeper_port: int = 2181):
        self.k_utility = kafka_helper.Kafka(zookeeper_host, str(zookeeper_port))
        self.producer = self.k_utility.get_producer()
    
    def publish(self, topic: str, data: str):
        
        if not self.k_utility.topic_exists(topic):
            self.k_utility.create_topic(topic, 1, 1)

        print(f"publishing {data} to {topic}")
        self.producer.send(topic, str.encode(str(data)))

    
