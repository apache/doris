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
while [[ ! $(curl -s --fail http://rest:8181/v1/config) ]]; do
    sleep 1
done

set -ex

# remove /opt/spark/jars/iceberg-aws-bundle-1.5.0.jar\:/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar
rm /opt/spark/jars/iceberg-aws-bundle-1.5.0.jar
rm /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar

start-master.sh -p 7077
start-worker.sh spark://doris--spark-iceberg:7077
start-history-server.sh
start-thriftserver.sh --driver-java-options "-Dderby.system.home=/tmp/derby"

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

# wait for hive-metastore to be ready (needed by delta lake)
echo "Waiting for Hive Metastore to be ready..."
while ! bash -c "cat < /dev/null > /dev/tcp/hive-metastore/9083" 2>/dev/null; do
    sleep 2
done
echo "Hive Metastore is ready."

START_TIME4=$(date +%s)
find /mnt/scripts/create_preinstalled_scripts/deltalake -name '*.sql' | sort | sed 's|^|source |' | sed 's|$|;|'> deltalake_total.sql
spark-sql --master spark://doris--spark-iceberg:7077 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.delta_lake=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.catalog.delta_lake.type=hive \
    --conf spark.sql.catalog.delta_lake.hive.metastore.uris=thrift://hive-metastore:9083 \
    --conf spark.sql.catalog.delta_lake.spark.sql.warehouse.dir=s3a://warehouse/wh/ \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=admin \
    --conf spark.hadoop.fs.s3a.secret.key=password \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    -f deltalake_total.sql
END_TIME4=$(date +%s)
EXECUTION_TIME4=$((END_TIME4 - START_TIME4))
echo "Script delta lake total: {} executed in $EXECUTION_TIME4 seconds"

touch /mnt/SUCCESS;

tail -f /dev/null
