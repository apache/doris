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
set -euo pipefail
source /usr/local/common/hive-configure.sh
source /usr/local/common/event-hook.sh

echo "Configuring hive"
configure /etc/hive/conf/hive-site.xml hive HIVE_SITE_CONF
configure /etc/hive/conf/hiveserver2-site.xml hive HIVE_SITE_CONF
configure /etc/hadoop/conf/core-site.xml core CORE_CONF
configure /etc/hadoop/conf/hdfs-site.xml hdfs HDFS_CONF
configure /etc/hadoop/conf/yarn-site.xml yarn YARN_CONF
configure /etc/hadoop/conf/mapred-site.xml mapred MAPRED_CONF
configure /etc/hive/conf/beeline-site.xml beeline BEELINE_SITE_CONF

echo "Copying kerberos keytabs to keytabs/"
mkdir -p /etc/hadoop-init.d/

if [ "$1" == "1" ]; then
    cp /etc/trino/conf/* /keytabs/
elif [ "$1" == "2" ]; then
    cp /etc/trino/conf/hive-presto-master.keytab /keytabs/other-hive-presto-master.keytab
    cp /etc/trino/conf/presto-server.keytab /keytabs/other-presto-server.keytab
else
    echo "Invalid index parameter. Exiting."
    exit 1
fi
/usr/local/hadoop-run.sh &

# check healthy hear
echo "Waiting for hadoop to be healthy"

for i in {1..60}; do
    if /usr/local/health.sh; then
        echo "Hadoop is healthy"
        break
    fi
    echo "Hadoop is not healthy yet. Retrying in 60 seconds..."
    sleep 5
done

if [ $i -eq 60 ]; then
    echo "Hadoop did not become healthy after 60 attempts. Exiting."
    exit 1
fi

echo "Init kerberos test data"

if [ "$1" == "1" ]; then
    kinit -kt /etc/hive/conf/hive.keytab hive/hadoop-master@LABS.TERADATA.COM
elif [ "$1" == "2" ]; then
    kinit -kt /etc/hive/conf/hive.keytab hive/hadoop-master-2@OTHERREALM.COM
else
    echo "Invalid index parameter. Exiting."
    exit 1
fi
hive  -f /usr/local/sql/create_kerberos_hive_table.sql

exec_success_hook