#!/usr/bin/env bash
sudo docker exec -it doris-iceberg-paimon-jobmanager sql-client.sh -i /opt/flink/sql/init_tables.sql