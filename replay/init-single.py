#!/usr/bin/python3

import argparse
import os
import os.path
import string

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

# Parses command line arguments
def parseCmdLineArgs ():
    parser = argparse.ArgumentParser (description='MarketData Replay')
    parser.add_argument ('-d', '--debug', dest='debug', action='store_true', help='Enable debugging logs')
    parser.add_argument ('-l', dest='log', type=str, default=None, help='Log directory (if desired)')
    # zookeeper
    parser.add_argument ('-b', '--bootstrap-server', dest='bootstrap_server', type=str, default='localhost:9092', help='Kafka bootstrap server')

    return parser.parse_args()

def main():
    args = parseCmdLineArgs()

    kafka_config = {
        'bootstrap_servers': args.bootstrap_server
    }

    admin = KafkaAdminClient(**kafka_config)

    topics = [f'OHLC.{x}' for x in string.ascii_uppercase]
    topic_list = [NewTopic(name = x, num_partitions = 1, replication_factor = 1) for x in topics]

    try:
        admin.create_topics(new_topics = topic_list, validate_only = False)
    except TopicAlreadyExistsError:
        pass

if __name__ == '__main__':
    main()
