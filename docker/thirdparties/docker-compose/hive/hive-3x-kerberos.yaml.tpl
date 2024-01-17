#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

version: "3.8"

services:
  doris--kerberos:
    image: doris_krb:1.0.0
    environment:
      - CLUSTER_NAME=test
    env_file:
      - ./hadoop-hive.env
    command: [ "/bin/bash", "/mnt/scripts/init-kerberos.sh" ] 
    container_name: doris--kerberos
    expose:
      - "80"
      - "9083"
      - "8020"
    volumes:
      - ./scripts:/mnt/scripts
    healthcheck:
      test: [ "CMD", "cat", "/etc/krb5.conf" ]
      interval: 5s
      timeout: 120s
      retries: 120
    network_mode: "host"


