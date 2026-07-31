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

x-kerberos-image: &kerberos-image
  image: doris-kerberos-minimal:${CONTAINER_UID}
  build:
    context: .
    dockerfile: Dockerfile
    network: host

x-kerberos-service: &kerberos-service
  <<: *kerberos-image
  network_mode: "host"
  mem_limit: 1536m
  stop_grace_period: 30s

services:
  hive-krb1:
    <<: *kerberos-service
    container_name: doris-${CONTAINER_UID}-kerberos1
    hostname: hadoop-master
    volumes:
      - ./conf/kerberos1:/opt/doris/conf:ro
      - ./conf/kerberos1/krb5.conf:/etc/krb5.conf:ro
      - ./data/kerberos1:/data
      - ./two-kerberos-hives:/keytabs
    env_file:
      - ./hadoop-hive-1.env

  hive-krb2:
    <<: *kerberos-service
    container_name: doris-${CONTAINER_UID}-kerberos2
    hostname: hadoop-master-2
    volumes:
      - ./conf/kerberos2:/opt/doris/conf:ro
      - ./conf/kerberos2/krb5.conf:/etc/krb5.conf:ro
      - ./data/kerberos2:/data
      - ./two-kerberos-hives:/keytabs
    env_file:
      - ./hadoop-hive-2.env
