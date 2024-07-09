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
    image: doristhirdpartydocker/trinodb:hdp3.1-hive-kerberized_96
    container_name: doris--kerberos1
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
      - "5806:5006"
      - "8820:8020"
      - "8842:8042"
      - "9800:9000"
      - "9883:9083"
      - "18000:10000"
    networks:
      doris--krb_net:
        ipv4_address: 172.31.71.25

  hive-krb2:
    image: doristhirdpartydocker/trinodb:hdp3.1-hive-kerberized-2_96
    container_name: doris--kerberos2
    hostname: hadoop-master-2
    volumes:
      - ./two-kerberos-hives:/keytabs
      - ./sql:/usr/local/sql
      - ./common/hadoop/apply-config-overrides.sh:/etc/hadoop-init.d/00-apply-config-overrides.sh
      - ./common/hadoop/hadoop-run.sh:/usr/local/hadoop-run.sh
      - ./health-checks/hadoop-health-check.sh:/etc/health.d/hadoop-health-check.sh
      - ./entrypoint-hive-master-2.sh:/usr/local/entrypoint-hive-master-2.sh
    entrypoint: /usr/local/entrypoint-hive-master-2.sh
    healthcheck:
      test: ./health-checks/health.sh
    ports:
      - "15806:5006"
      - "18820:8020"
      - "18842:8042"
      - "19800:9000"
      - "19883:9083"
      - "18800:10000"
    networks:
      doris--krb_net:
        ipv4_address: 172.31.71.26

networks:
  doris--krb_net:
    ipam:
      config:
        - subnet: 172.31.71.0/24
