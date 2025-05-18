#!/bin/bash
export HOME=/tmp
export USER=root
mkdir -p /tmp/.ivy2/local

/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/MetaStream/spark/stream_processor.py
