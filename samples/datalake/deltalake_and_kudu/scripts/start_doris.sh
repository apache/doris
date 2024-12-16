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

export JAVA_HOME=/opt/jdk-17.0.2

cp -r /opt/doris-bin /opt/doris
echo "trino_connector_plugin_dir=/opt/connectors/" >> /opt/doris/fe/conf/fe.conf
echo "trino_connector_plugin_dir=/opt/connectors/" >> /opt/doris/be/conf/be.conf

/opt/doris/fe/bin/start_fe.sh --daemon
/opt/doris/be/bin/start_be.sh --daemon
tail -F /dev/null
