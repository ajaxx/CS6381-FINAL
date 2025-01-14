#!/usr/bin/python3

import argparse
import csv
import json
import os
import os.path
import time

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
            self.create_topic(topic)
        
        # serialize the data to json
        data = json.dumps(data)
        # encode the data as utf-8
        data = data.encode('utf-8')

        #print(f"publishing {data} to {topic}")
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

    parser.add_argument ('-t', '--topic', dest='topic_mapping', choices = ['full', 'single', 'double'], default='single', help='Topic mapping strategy')
    parser.add_argument ('--stats', dest='stats', action='store_true', default=False, help='Output execution statistics')

    return parser.parse_args()

def stream_csv_data(filename: str, symbol: str):
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row['__symbol__'] = symbol
            row['__mtime__'] = int(time.time_ns() / 1000.00) # microsecond time
            yield row

def main():
    args = parseCmdLineArgs()

    # create the publisher
    publisher = Publisher(args.bootstrap_server)

    capture_statistics = args.stats

    stats_start_time = int(time.time_ns() / 1000)
    stats_records = 0

    symbol = None
    symbol_topic = None

    # stream the replay data
    if args.filename.endswith('csv'):
        symbol = os.path.basename(args.filename)[0:-4]

        # determine the topic for the symbol
        if args.topic_mapping == 'full':
            symbol_topic = f'OHLC.{symbol}'
        elif args.topic_mapping == 'single':
            symbol_topic = f'OHLC.{symbol[0]}'
        elif args.topic_mapping == 'double':
            symbol_topic = f'OHLC.{symbol[0:2]}'

        if not args.stats:
            print(f'Starting stream for {symbol} => {symbol_topic}')

        # csv streamer
        for record in stream_csv_data(args.filename, symbol):
            # records should have inherent timing in them to tell how long to sleep between records,
            # but at this juncture there is nothing, so we basically just stream at a rate that
            # make sense to us
            publisher.publish(symbol_topic, record)
            stats_records += 1
            sleep(args.interval)
        
    if args.stats:
        stats_end_time = int(time.time_ns() / 1000)
        stats_elapsed = stats_end_time - stats_start_time
        print (f'{symbol},{symbol_topic},{stats_records},{stats_elapsed}')

if __name__ == '__main__':
    main()
