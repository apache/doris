#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

echo "Copying spark default config and setting up configs"
cp /var/scripts/config/spark-defaults.conf $SPARK_CONF_DIR/.
cp /var/scripts/config/log4j2.properties $SPARK_CONF_DIR/.
echo "sleep 10, wait hdfs start"
sleep 10
echo "hadoop fs -mkdir -p /var/demo/"
hadoop fs -mkdir -p /var/demo/
echo "hadoop fs -mkdir -p /tmp/spark-events"
hadoop fs -mkdir -p /tmp/spark-events
echo "hadoop fs -mkdir -p /user/hive/"
hadoop fs -mkdir -p /user/hive/
echo "hadoop fs -copyFromLocal -f /var/scripts/config /var/demo/."
hadoop fs -copyFromLocal -f /var/scripts/config /var/demo/.
echo "hadoop fs -copyFromLocal -f /var/scripts/hudi_docker_compose_attached_file/warehouse /user/hive/"
hadoop fs -copyFromLocal -f /var/scripts/hudi_docker_compose_attached_file/warehouse /user/hive/
echo "chmod +x /var/scripts/run_sync_tool.sh"
chmod +x /var/scripts/run_sync_tool.sh

echo "Start synchronizing the stock_ticks_cow table"
/var/scripts/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by date \
  --base-path /user/hive/warehouse/stock_ticks_cow \
  --database default \
  --table stock_ticks_cow \
  --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor

echo "Start synchronizing the stock_ticks_mor table"
/var/scripts/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by date \
  --base-path /user/hive/warehouse/stock_ticks_mor \
  --database default \
  --table stock_ticks_mor \
  --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor

echo "Start synchronizing the hudi_cow_pt_tbl table"
/var/scripts/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by dt \
  --base-path /user/hive/warehouse/hudi_cow_pt_tbl \
  --database default \
  --table hudi_cow_pt_tbl \
  --partition-value-extractor org.apache.hudi.hive.HiveStylePartitionValueExtractor

echo "Start synchronizing the hudi_non_part_cow table"
/var/scripts/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --base-path /user/hive/warehouse/hudi_non_part_cow \
  --database default \
  --table hudi_non_part_cow \
