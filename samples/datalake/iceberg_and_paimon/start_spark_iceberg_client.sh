#!/usr/bin/env bash
sudo docker exec -it doris-iceberg-paimon-spark spark-sql --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions