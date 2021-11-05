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

echo "build doris manager home path"

echo "build doris manager web start"
cd doris-manager
npm install
npm run build
echo "build doris manager web end"

cd ../
echo "copy doris manager web resources to server"
rm -rf manager-server/src/main/resources/web-resource
mv doris-manager/dist manager-server/src/main/resources/web-resource
echo "copy doris manager web resources to server end"

echo "build doris manager server start"
set -e
mvn clean install
echo "build doris manager server end"

echo "copy to output package start"
rm -rf output
rm -rf doris-manager-1.0.0.tar.gz
mkdir -p output
mkdir -p output/server/lib
mv manager-server/target/manager-server-1.0.0.jar output/server/lib/doris-manager.jar
cp -r conf output/server/
cp -r manager-bin output/
mv output/manager-bin/agent output/
mv output/manager-bin output/server/bin
mkdir -p output/agent/lib
mv dm-agent/target/dm-agent-1.0.0.jar output/agent/lib/dm-agent.jar
cp -r manager-server/src/main/resources/web-resource output/server/
tar -zcvf doris-manager-1.0.0.tar.gz output/
echo "copy to output package end"