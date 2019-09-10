#!/usr/bin/env bash
set -e
[[ -v KAFKA_HOME ]] || echo "Warning: \$KAFKA_HOME variable not set"

logdir=logs/

test -d ${logdir} || mkdir ${logdir}

$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > ${logdir}zookeeper.log &
sleep 5
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > ${logdir}server.log &
sleep 5
