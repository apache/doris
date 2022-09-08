#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

echo "Welcome to Apache-Doris Docker-Compose to quickly build test images!"
echo "Start modifying the configuration files of FE and BE and run FE and BE!"
docker cp /root/init_fe.sh doris-fe:/root/init_fe.sh
docker exec doris-fe bash -c "/root/init_fe.sh"
docker cp /root/init_be.sh doris-be:/root/init_be.sh
docker exec doris-be bash -c "/root/init_be.sh"
sleep 30
echo "Get started with the Apache Doris registration steps!"
mysql -h 172.20.80.2 -P 9030 -uroot -e "ALTER SYSTEM ADD BACKEND \"172.20.80.3:9050\";"
echo "The initialization task of Apache-Doris has been completed, please start to experience it!"
