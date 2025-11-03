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
  hive-krb1:
    image: doristhirdpartydocker/trinodb:hdp3.1-hive-kerberized_96
    container_name: doris-${CONTAINER_UID}-kerberos1
    volumes:
      - ../common:/usr/local/common
      - ./two-kerberos-hives:/keytabs
      - ./sql:/usr/local/sql
      - ./common/hadoop/apply-config-overrides.sh:/etc/hadoop-init.d/00-apply-config-overrides.sh
      - ./common/hadoop/hadoop-run.sh:/usr/local/hadoop-run.sh
      - ./health-checks/health.sh:/usr/local/health.sh
      - ./health-checks/supervisorctl-check.sh:/etc/health.d/supervisorctl-check.sh
      - ./health-checks/hive-health-check.sh:/etc/health.d/hive-health-check.sh
      - ./entrypoint-hive-master.sh:/usr/local/entrypoint-hive-master.sh
      - ./conf/kerberos1/my.cnf:/etc/my.cnf
      - ./conf/kerberos1/kdc.conf:/var/kerberos/krb5kdc/kdc.conf
      - ./conf/kerberos1/krb5.conf:/etc/krb5.conf
      - ./paimon_data:/tmp/paimon_data
    hostname: hadoop-master
    entrypoint: /usr/local/entrypoint-hive-master.sh 1
    healthcheck:
      test: ["CMD", "ls", "/tmp/SUCCESS"]
      interval: 5s
      timeout: 10s
      retries: 120
    network_mode: "host"
    env_file:
      - ./hadoop-hive-1.env
  hive-krb2:
    image: doristhirdpartydocker/trinodb:hdp3.1-hive-kerberized-2_96
    container_name: doris-${CONTAINER_UID}-kerberos2
    hostname: hadoop-master-2
    volumes:
      - ../common:/usr/local/common
      - ./two-kerberos-hives:/keytabs
      - ./sql:/usr/local/sql
      - ./common/hadoop/apply-config-overrides.sh:/etc/hadoop-init.d/00-apply-config-overrides.sh
      - ./common/hadoop/hadoop-run.sh:/usr/local/hadoop-run.sh
      - ./health-checks/health.sh:/usr/local/health.sh
      - ./health-checks/supervisorctl-check.sh:/etc/health.d/supervisorctl-check.sh
      - ./health-checks/hive-health-check-2.sh:/etc/health.d/hive-health-check-2.sh
      - ./entrypoint-hive-master.sh:/usr/local/entrypoint-hive-master.sh
      - ./conf/kerberos2/my.cnf:/etc/my.cnf
      - ./conf/kerberos2/kdc.conf:/var/kerberos/krb5kdc/kdc.conf
      - ./conf/kerberos2/krb5.conf:/etc/krb5.conf
      - ./paimon_data:/tmp/paimon_data
    entrypoint: /usr/local/entrypoint-hive-master.sh 2
    healthcheck:
      test: ["CMD", "ls", "/tmp/SUCCESS"]
      interval: 5s
      timeout: 10s
      retries: 120
    network_mode: "host"
    env_file:
      - ./hadoop-hive-2.env