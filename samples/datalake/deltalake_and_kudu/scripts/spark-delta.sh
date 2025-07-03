#!/usr/bin/env bash

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

export SPARK_HOME=/opt/spark
export HIVE_HOME=/opt/apache-hive-3.1.2-bin
export HADOOP_HOME=/opt/hadoop-3.3.1

if [[ ! -d "${SPARK_HOME}" ]]; then
    cp -r /opt/spark-3.4.2-bin-hadoop3 "${SPARK_HOME}"
fi

cp "${HIVE_HOME}"/conf/hive-site.xml "${SPARK_HOME}"/conf/
cp "${HIVE_HOME}"/lib/postgresql-jdbc.jar "${SPARK_HOME}"/jars/
cp "${HADOOP_HOME}"/etc/hadoop/core-site.xml "${SPARK_HOME}"/conf/

"${SPARK_HOME}"/bin/spark-sql \
    --master local[*] \
    --name "spark-delta-sql" \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
