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
echo "hadoop fs -copyFromLocal -f /var/scripts/config /var/demo/."
hadoop fs -copyFromLocal -f /var/scripts/config /var/demo/.
echo "chmod +x /var/scripts/run_sync_tool.sh"
chmod +x /var/scripts/run_sync_tool.sh
