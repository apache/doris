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

##############################################################
# See emr_tools.sh
##############################################################

# specified sevices: ali,hw,tx
export SERVICE=ali
# doris host
export HOST=127.0.0.1
# doris user
export USER=root
# doris mysql cli port
export PORT=9030

# prepare endpoint,region,ak/sk
if [[ ${SERVICE} == 'ali' ]]; then
    export CASE=ping
    export AK=ak
    export SK=sk
    export ENDPOINT=oss-cn-beijing-internal.aliyuncs.com
    export REGION=oss-cn-beijing
    export HMS_META_URI="thrift://172.16.1.1:9083"
    export HMS_WAREHOUSE=oss://benchmark-oss/user
elif [[ ${SERVICE} == 'hw' ]]; then
    export CASE=ping
    export AK=ak
    export SK=sk
    export ENDPOINT=obs.cn-north-4.myhuaweicloud.com
    export REGION=cn-north-4
    export HMS_META_URI="thrift://node1:9083,thrift://node2:9083"
    export HMS_WAREHOUSE=obs://datalake-bench/user
    export BEELINE_URI="jdbc:hive2://192.168.0.1:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;hive.server2.proxy.user=hive"
elif [[ ${SERVICE} == 'tx' ]]; then
    export CASE=ping
    export AK=ak
    export SK=sk
    export ENDPOINT=cos.ap-beijing.mycloud.com
    export REGION=ap-beijing
    export HMS_META_URI="thrift://172.21.0.1:7004"
    export HMS_WAREHOUSE=cosn://datalake-bench-cos-1308700295/user
fi
