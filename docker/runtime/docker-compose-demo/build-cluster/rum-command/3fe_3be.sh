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
docker network create --driver bridge --subnet=172.20.80.0/24 doris-network
docker run -itd \
    --name=fe-01 \
    --env FE_SERVERS="fe1:172.20.80.2:9010,fe2:172.20.80.3:9010,fe3:172.20.80.4:9010" \
    --env FE_ID=1 \
    -p 8031:8030 \
    -p 9031:9030 \
    -v /data/fe-01/doris-meta:/opt/apache-doris/fe/doris-meta \
    -v /data/fe-01/log:/opt/apache-doris/fe/log \
    --network=doris-network \
    --ip=172.20.80.2 \
    apache/doris:2.0.0_alpha-fe-x86_64

docker run -itd \
    --name=fe-02 \
    --env FE_SERVERS="fe1:172.20.80.2:9010,fe2:172.20.80.3:9010,fe3:172.20.80.4:9010" \
    --env FE_ID=2 \
    -p 8032:8030 \
    -p 9032:9030 \
    -v /data/fe-02/doris-meta:/opt/apache-doris/fe/doris-meta \
    -v /data/fe-02/log:/opt/apache-doris/fe/log \
    --network=doris-network \
    --ip=172.20.80.3 \
    apache/doris:2.0.0_alpha-fe-x86_64

docker run -itd \
    --name=fe-03 \
    --env FE_SERVERS="fe1:172.20.80.2:9010,fe2:172.20.80.3:9010,fe3:172.20.80.4:9010" \
    --env FE_ID=3 \
    -p 8033:8030 \
    -p 9033:9030 \
    -v /data/fe-03/doris-meta:/opt/apache-doris/fe/doris-meta \
    -v /data/fe-03/log:/opt/apache-doris/fe/log \
    --network=doris-network \
    --ip=172.20.80.4 \
    apache/doris:2.0.0_alpha-fe-x86_64

docker run -itd \
    --name=be-01 \
    --env FE_SERVERS="fe1:172.20.80.2:9010,fe2:172.20.80.3:9010,fe3:172.20.80.4:9010" \
    --env BE_ADDR="172.20.80.5:9050" \
    -p 8041:8040 \
    -v /data/be-01/storage:/opt/apache-doris/be/storage \
    -v /data/be-01/log:/opt/apache-doris/be/log \
    --network=doris-network \
    --ip=172.20.80.5 \
    apache/doris:2.0.0_alpha-be-x86_64

docker run -itd \
    --name=be-02 \
    --env FE_SERVERS="fe1:172.20.80.2:9010,fe2:172.20.80.3:9010,fe3:172.20.80.4:9010" \
    --env BE_ADDR="172.20.80.6:9050" \
    -p 8042:8040 \
    -v /data/be-02/storage:/opt/apache-doris/be/storage \
    -v /data/be-02/log:/opt/apache-doris/be/log \
    --network=doris-network \
    --ip=172.20.80.6 \
    apache/doris:2.0.0_alpha-be-x86_64

docker run -itd \
    --name=be-03 \
    --env FE_SERVERS="fe1:172.20.80.2:9010,fe2:172.20.80.3:9010,fe3:172.20.80.4:9010" \
    --env BE_ADDR="172.20.80.7:9050" \
    -p 8043:8040 \
    -v /data/be-03/storage:/opt/apache-doris/be/storage \
    -v /data/be-03/log:/opt/apache-doris/be/log \
    --network=doris-network \
    --ip=172.20.80.7 \
    apache/doris:2.0.0_alpha-be-x86_64
