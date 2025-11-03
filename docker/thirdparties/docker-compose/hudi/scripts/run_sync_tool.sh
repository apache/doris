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

function error_exit {
    echo "$1" >&2   ## Send message to stderr. Exclude >&2 if you don't want it that way.
    exit "${2:-1}"  ## Return a code specified by $2 or 1 by default.
}

if [ -z "${HADOOP_HOME}" ]; then
  error_exit "Please make sure the environment variable HADOOP_HOME is setup"
fi

if [ -z "${HIVE_HOME}" ]; then
  error_exit "Please make sure the environment variable HIVE_HOME is setup"
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HUDI_HIVE_UBER_JAR=`ls -c $DIR/./hudi_docker_compose_attached_file/jar/hoodie-hive-sync-bundle.jar | grep -v source | head -1`

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
fi

## Include only specific packages from HIVE_HOME/lib to avoid version mismatches
HIVE_EXEC=`ls ${HIVE_HOME}/lib/hive-exec-*.jar | tr '\n' ':'`
HIVE_SERVICE=`ls ${HIVE_HOME}/lib/hive-service-*.jar | grep -v rpc | tr '\n' ':'`
HIVE_METASTORE=`ls ${HIVE_HOME}/lib/hive-metastore-*.jar | tr '\n' ':'`
HIVE_JDBC=`ls ${HIVE_HOME}/lib/hive-jdbc-*.jar | tr '\n' ':'`
if [ -z "${HIVE_JDBC}" ]; then
  HIVE_JDBC=`ls ${HIVE_HOME}/lib/hive-jdbc-*.jar | grep -v handler | tr '\n' ':'`
fi
HIVE_JACKSON=`ls ${HIVE_HOME}/lib/jackson-*.jar | tr '\n' ':'`
HIVE_JARS=$HIVE_METASTORE:$HIVE_SERVICE:$HIVE_EXEC:$HIVE_JDBC:$HIVE_JACKSON

HADOOP_HIVE_JARS=${HIVE_JARS}:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*

echo "Running Command : java -cp ${HADOOP_HIVE_JARS}:${HADOOP_CONF_DIR}:$HUDI_HIVE_UBER_JAR org.apache.hudi.hive.HiveSyncTool $@"
java -cp $HUDI_HIVE_UBER_JAR:${HADOOP_HIVE_JARS}:${HADOOP_CONF_DIR} org.apache.hudi.hive.HiveSyncTool "$@"
