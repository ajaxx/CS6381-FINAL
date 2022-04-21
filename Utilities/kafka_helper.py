import os
from kafka import KafkaConsumer
from kafka import KafkaProducer
import kafka

class Kafka:

    host_ip: str
    host_port: str

    def __init__(self, host_ip: str, host_port: str) -> None:
        
        self.host_ip = host_ip
        self.host_port = host_port
    
    def create_topic(self, topic_name: str, num_partitions: int, replication_factor: int) -> None:
        
        os.system(f'kafka/bin/kafka-topics.sh --create --zookeeper {self.host_ip}:{self.host_port} --replication-factor {replication_factor} --partitions {num_partitions} --topic {topic_name}')

    def get_consumer(self, topic_name: str) -> KafkaConsumer:

        return KafkaConsumer(topic_name, bootstrap_servers=f'{self.host_ip}:9092', group_id='test')
    
    def get_producer(self,) -> KafkaProducer:
        
        return KafkaProducer(bootstrap_servers=f'{self.host_ip}:9092')

    def topic_exists(self, topic: str):

        consumer = KafkaConsumer(bootstrap_servers=f'{self.host_ip}:9092', group_id='test')
        server_topics = consumer.topics()

        if topic in server_topics:
            return True
        
        else:
            return False