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

nohup /opt/hive/bin/hive --service metastore &

# wait metastore start
sleep 10s

# if you test in your local，better use # to annotation section about tpch1.db
if [[ ! -d "/mnt/scripts/tpch1.db" ]]; then
    echo "/mnt/scripts/tpch1.db does not exist"
    cd /mnt/scripts/
    wget -P /mnt/scripts https://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression/load/tpch1_parquet/tpch1.db.tar.gz
    tar -zxf tpch1.db.tar.gz
    rm -rf tpch1.db.tar.gz
    cd -
else
    echo "/mnt/scripts/tpch1.db exist, continue !"
fi

# put data file
## put tpch1
echo "hadoop fs -mkdir /user/doris/"
hadoop fs -mkdir -p /user/doris/
echo "hadoop fs -put /mnt/scripts/tpch1.db /user/doris/"
hadoop fs -put /mnt/scripts/tpch1.db /user/doris/


# if you test in your local，better use # to annotation section about paimon
if [[ ! -d "/mnt/scripts/paimon1" ]]; then
    echo "/mnt/scripts/paimon1 does not exist"
    cd /mnt/scripts/
    curl -O https://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression/paimon/paimon1.tar.gz
    tar -zxf paimon1.tar.gz
    rm -rf paimon1.tar.gz
    cd -
else
    echo "/mnt/scripts/paimon1 exist, continue !"
fi

## put paimon1
echo "hadoop fs -put /mnt/scripts/paimon1 /user/doris/"
hadoop fs -put /mnt/scripts/paimon1 /user/doris/

## put other preinstalled data
echo "hadoop fs -put /mnt/scripts/preinstalled_data /user/doris/"
hadoop fs -put /mnt/scripts/preinstalled_data /user/doris/

# create table
echo "hive -f /mnt/scripts/create_tpch1_orc.hql"
hive -f /mnt/scripts/create_tpch1_orc.hql

echo "hive -f /mnt/scripts/create_tpch1_parquet.hql"
hive -f /mnt/scripts/create_tpch1_parquet.hql

echo "hive -f /mnt/scripts/create_tpcds100_orc.hql"
hive -f /mnt/scripts/create_tpcds100_orc.hql

echo "hive -f /mnt/scripts/create_tpcds100_parquet.hql"
hive -f /mnt/scripts/create_tpcds100_parquet.hql

echo "hive -f /mnt/scripts/create_preinstalled_table.hql"
hive -f /mnt/scripts/create_preinstalled_table.hql

echo "touch /mnt/SUCCESS"
touch /mnt/SUCCESS

# Avoid container exit
while true; do
    sleep 1
done
