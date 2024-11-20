#!/bin/bash

mkdir -p /opt/flink/checkpoints
mkdir -p /opt/flink/savepoints
chmod -R 777 /opt/flink/checkpoints
chmod -R 777 /opt/flink/savepoints

/docker-entrypoint.sh "$@"
