import argparse
import csv
import json
import os
import os.path

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from time import sleep

class Publisher:
    def __init__(self, bootstrap_addr: str):
        kafka_config = {
            'bootstrap_servers': bootstrap_addr
        }
        # create a client
        self.admin = KafkaAdminClient(**kafka_config)
        # create a producer
        self.producer = KafkaProducer(**kafka_config)
        # cached results of topics that should exist
        self.topic_cache = set()
    
    def create_topic(self, topic: str):
        topic_atom = NewTopic(name = topic, num_partitions = 1, replication_factor = 1)
        topic_list = [ topic_atom ]
        try:
            self.admin.create_topics(new_topics = topic_list, validate_only = False)
        except TopicAlreadyExistsError:
            pass

        self.topic_cache.add(topic)

    def publish(self, topic: str, data: dict):
        if topic not in self.topic_cache:
            print(f'creating topic {topic}')
            self.create_topic(topic)
        
        # serialize the data to json
        data = json.dumps(data)
        # encode the data as utf-8
        data = data.encode('utf-8')

        print(f"publishing {data} to {topic}")
        self.producer.send(topic, data)

# Parses command line arguments
def parseCmdLineArgs ():
    parser = argparse.ArgumentParser (description='MarketData Replay')
    parser.add_argument ('-d', '--debug', dest='debug', action='store_true', help='Enable debugging logs')
    parser.add_argument ('-l', dest='log', type=str, default=None, help='Log directory (if desired)')
    # zookeeper
    parser.add_argument ('-b', '--bootstrap-server', dest='bootstrap_server', type=str, default='localhost:9092', help='Kafka bootstrap server')
    # replay feed information
    parser.add_argument ('-f', '--file', dest='filename', type=str, default=None, required = True, help='Input filename')
    parser.add_argument ('-i', '--interval', dest='interval', type=float, default=1.0, required = False, help='Sleep Interval')

    return parser.parse_args()

def stream_csv_data(filename):
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield row

def main():
    args = parseCmdLineArgs()

    # create the publisher
    publisher = Publisher(args.bootstrap_server)

    # stream the replay data
    if args.filename.endswith('csv'):
        symbol = os.path.basename(args.filename)[0:-4]
        symbol_topic = f'OHLC.{symbol}'
        print(f'Starting stream for {symbol_topic}')

        # csv streamer
        for record in stream_csv_data(args.filename):
            # records should have inherent timing in them to tell how long to sleep between records,
            # but at this juncture there is nothing, so we basically just stream at a rate that
            # make sense to us
            publisher.publish(symbol_topic, record)
            sleep(args.interval)

if __name__ == '__main__':
    main()