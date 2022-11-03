#!/bin/bash
set -ex
mkdir -p /tmp/farts
cd /tmp/farts
if [ ! -f kafka_2.13-3.3.1.tgz ]; then
  wget https://dlcdn.apache.org/kafka/3.3.1/kafka_2.13-3.3.1.tgz
fi
if [ ! -d kafka_2.13-3.3.1 ]; then
  tar -xf kafka_2.13-3.3.1.tgz
fi
cd kafka_2.13-3.3.1
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties --ignore-formatted
bin/kafka-server-start.sh config/kraft/server.properties &
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
