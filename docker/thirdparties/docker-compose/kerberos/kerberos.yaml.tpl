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
version: "3"
services:
    hive-krb:
        image: ghcr.io/trinodb/testing/hdp3.1-hive-kerberized
        volumes:
            - ./two-kerberos-hives:/keytabs
            - ./sql:/usr/local/sql
            - ./common/hadoop/apply-config-overrides.sh:/etc/hadoop-init.d/00-apply-config-overrides.sh
            - ./common/hadoop/hadoop-run.sh:/usr/local/hadoop-run.sh
            - ./health-checks/hadoop-health-check.sh:/etc/health.d/hadoop-health-check.sh
            - ./entrypoint-hive-master.sh:/usr/local/entrypoint-hive-master.sh
        hostname: hadoop-master
        entrypoint: /usr/local/entrypoint-hive-master.sh
        healthcheck:
          test: ./health-checks/health.sh
        ports:
            - "5006:5806"
            - "8020:8820"
            - "8042:8842"
            - "9000:9800"
            - "9083:9883"
            - "10000:18000"
            - "19888:19888"
        networks:
            doris_net:
                ipv4_address: 172.20.70.25

    hive-krb2:
        image: ghcr.io/trinodb/testing/hdp3.1-hive-kerberized-2:96
        volumes:
            - ./two-kerberos-hives:/keytabs
            - ./sql:/usr/local/sql
            - ./common/hadoop/apply-config-overrides.sh:/etc/hadoop-init.d/00-apply-config-overrides.sh
            - ./common/hadoop/hadoop-run.sh:/usr/local/hadoop-run.sh
            - ./health-checks/hadoop-health-check.sh:/etc/health.d/hadoop-health-check.sh
            - ./entrypoint-hive-master-2.sh:/usr/local/entrypoint-hive-master-2.sh
        hostname: hadoop-master-2
        entrypoint: /usr/local/entrypoint-hive-master-2.sh
        healthcheck:
          test: ./health-checks/health.sh
        networks:
            doris_net:
                ipv4_address: 172.20.70.26

networks:
    doris_net:
        ipam:
            config:
                - subnet: 172.20.70.0/24
