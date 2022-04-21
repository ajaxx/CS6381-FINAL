#!/bin/bash

TOPICS=$(kafka/bin/kafka-topics.sh --zookeeper 10.0.0.1:2181 --list )

for T in $TOPICS
do
  if [ "$T" != "__consumer_offsets" ]; then
    kafka/bin/kafka-topics.sh --zookeeper 10.0.0.1:2181 --delete --topic $T
  fi
done