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
sleep 10
if [[ ! -d "/mnt/scripts/tpch1.db" ]]; then
    echo "/mnt/scripts/tpch1.db does not exist"
    exit 1
fi

echo "hadoop fs -mkdir /user/doris/"
hadoop fs -mkdir -p /user/doris/
echo "hadoop fs -put /mnt/scripts/tpch1.db /user/doris/"
hadoop fs -put /mnt/scripts/tpch1.db /user/doris/
echo "hadoop fs -put /mnt/scripts/json_format_test.db /user/doris/"
hadoop fs -put /mnt/scripts/json_format_test /user/doris/
echo "hive -f /mnt/scripts/create.hql"
hive -f /mnt/scripts/create.hql

echo "touch /mnt/SUCCESS"
touch /mnt/SUCCESS

# Avoid container exit
while true; do
    sleep 1
done
