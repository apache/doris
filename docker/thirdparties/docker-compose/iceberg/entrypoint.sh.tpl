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

export SPARK_MASTER_HOST=doris--spark-iceberg

# wait iceberg-rest start
while ! curl -s --fail http://rest:8181/v1/config >/dev/null; do
    sleep 1
done

set -ex

mkdir -p /opt/spark/events
SPARK_THRIFT_EXTENSIONS="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"

for f in /opt/spark/sbin/*; do
  ln -s $f /usr/local/bin/$(basename $f)
done

for f in /opt/spark/bin/*; do
  ln -s $f /usr/local/bin/$(basename $f)
done


start-master.sh -p 7077
start-worker.sh spark://doris--spark-iceberg:7077
start-history-server.sh

# The creation of a Spark SQL client is time-consuming,
# and reopening a new client for each SQL file execution leads to significant overhead.
# To reduce the time spent on creating clients,
# we group these files together and execute them using a single client.
# This approach can reduce the time from 150s to 40s.

START_TIME1=$(date +%s)
find /mnt/scripts/create_preinstalled_scripts/iceberg -name '*.sql' | sed 's|^|source |' | sed 's|$|;|'> iceberg_total.sql
spark-sql --master spark://doris--spark-iceberg:7077 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions -f iceberg_total.sql 
END_TIME1=$(date +%s)
EXECUTION_TIME1=$((END_TIME1 - START_TIME1))
echo "Script iceberg total: {} executed in $EXECUTION_TIME1 seconds"

START_TIME2=$(date +%s)
find /mnt/scripts/create_preinstalled_scripts/paimon -name '*.sql' | sed 's|^|source |' | sed 's|$|;|'> paimon_total.sql
spark-sql  --master  spark://doris--spark-iceberg:7077 --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions -f paimon_total.sql
END_TIME2=$(date +%s)
EXECUTION_TIME2=$((END_TIME2 - START_TIME2))
echo "Script paimon total: {} executed in $EXECUTION_TIME2 seconds"

START_TIME3=$(date +%s)
find /mnt/scripts/create_preinstalled_scripts/iceberg_load -name '*.sql' | sed 's|^|source |' | sed 's|$|;|'> iceberg_load_total.sql
spark-sql --master spark://doris--spark-iceberg:7077 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions -f iceberg_load_total.sql 
END_TIME3=$(date +%s)
EXECUTION_TIME3=$((END_TIME3 - START_TIME3))
echo "Script iceberg load total: {} executed in $EXECUTION_TIME3 seconds"

spark-sql \
  --master spark://doris--spark-iceberg:7077 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "CREATE DATABASE IF NOT EXISTS demo.default"

start-thriftserver.sh \
  --master spark://doris--spark-iceberg:7077 \
  --conf "spark.sql.extensions=${SPARK_THRIFT_EXTENSIONS}" \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.cores.max=8 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.default.parallelism=16 \
  --driver-java-options "-Dderby.system.home=/tmp/derby"

SPARK_THRIFT_READY_ATTEMPTS=0
while ! beeline \
  -u "jdbc:hive2://localhost:10000/default" \
  -n hadoop \
  -p hadoop \
  -e "SELECT 1" >/tmp/spark-thriftserver-ready.log 2>&1; do
    SPARK_THRIFT_READY_ATTEMPTS=$((SPARK_THRIFT_READY_ATTEMPTS + 1))
    if [ "${SPARK_THRIFT_READY_ATTEMPTS}" -ge 120 ]; then
      echo "ERROR: Spark thriftserver did not become ready after ${SPARK_THRIFT_READY_ATTEMPTS} attempts" >&2
      cat /tmp/spark-thriftserver-ready.log >&2 || true
      tail -n 200 /opt/spark/logs/*HiveThriftServer2*.out >&2 || true
      exit 1
    fi
    sleep 1
done

touch /mnt/SUCCESS;

tail -f /dev/null
