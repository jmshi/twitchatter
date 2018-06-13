#!/bin/bash
if [ $# -ne 1 ]; then
  echo "need to specify topic name"
  exit 1
fi
topic=$1
root_dir=/usr/local/kafka/
# delete topic if existing
${root_dir}/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ${topic}


${root_dir}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 1 --replication-factor 2 --topic ${topic}

python producer.py &

bg %1
