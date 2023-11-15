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

version: "3.3"

networks:
  doris--hudi:
    ipam:
      driver: default
      config:
        - subnet: 168.37.0.0/24

services:

  namenode:
    image: apachehudi/hudi-hadoop_2.8.4-namenode:latest
    hostname: namenode
    container_name: namenode
    environment:
      - CLUSTER_NAME=hudi_hadoop284_hive232_spark244
    ports:
      - "50070:50070"
      - "8020:8020"
      # JVM debugging port (will be mapped to a random port on host)
      - "5005"
    env_file:
      - ./hadoop.env
    healthcheck:
      test: ["CMD", "curl", "-f", "http://namenode:50070"]
      interval: 30s
      timeout: 10s
      retries: 3
    networks:
      - doris--hudi

  datanode1:
    image: apachehudi/hudi-hadoop_2.8.4-datanode:latest
    container_name: datanode1
    hostname: datanode1
    environment:
      - CLUSTER_NAME=hudi_hadoop284_hive232_spark244
    env_file:
      - ./hadoop.env
    ports:
      - "50075:50075"
      - "50010:50010"
      # JVM debugging port (will be mapped to a random port on host)
      - "5005"
    links:
      - "namenode"
      - "historyserver"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://datanode1:50075"]
      interval: 30s
      timeout: 10s
      retries: 3
    depends_on:
      - namenode
    networks:
      - doris--hudi

  historyserver:
    image: apachehudi/hudi-hadoop_2.8.4-history:latest
    hostname: historyserver
    container_name: historyserver
    environment:
      - CLUSTER_NAME=hudi_hadoop284_hive232_spark244
    depends_on:
      - "namenode"
    links:
      - "namenode"
    ports:
      - "58188:8188"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://historyserver:8188"]
      interval: 30s
      timeout: 10s
      retries: 3
    env_file:
      - ./hadoop.env
    volumes:
      - ./historyserver:/hadoop/yarn/timeline
    networks:
      - doris--hudi

  hive-metastore-postgresql:
    image: bde2020/hive-metastore-postgresql:2.3.0
    volumes:
      - ./hive-metastore-postgresql:/var/lib/postgresql
    hostname: hive-metastore-postgresql
    container_name: hive-metastore-postgresql
    networks:
      - doris--hudi

  hivemetastore:
    image: apachehudi/hudi-hadoop_2.8.4-hive_2.3.3:latest
    hostname: hivemetastore
    container_name: hivemetastore
    links:
      - "hive-metastore-postgresql"
      - "namenode"
    env_file:
      - ./hadoop.env
    command: /opt/hive/bin/hive --service metastore
    environment:
      SERVICE_PRECONDITION: "namenode:50070 hive-metastore-postgresql:5432"
    ports:
      - "9083:9083"
      # JVM debugging port (will be mapped to a random port on host)
      - "5005"
    healthcheck:
      test: ["CMD", "nc", "-z", "hivemetastore", "9083"]
      interval: 30s
      timeout: 10s
      retries: 3
    depends_on:
      - "hive-metastore-postgresql"
      - "namenode"
    networks:
      - doris--hudi

  hiveserver:
    image: apachehudi/hudi-hadoop_2.8.4-hive_2.3.3:latest
    hostname: hiveserver
    container_name: hiveserver
    env_file:
      - ./hadoop.env
    environment:
      SERVICE_PRECONDITION: "hivemetastore:9083"
    ports:
      - "10000:10000"
      # JVM debugging port (will be mapped to a random port on host)
      - "5005"
    depends_on:
      - "hivemetastore"
    links:
      - "hivemetastore"
      - "hive-metastore-postgresql"
      - "namenode"
    volumes:
      - ./scripts:/var/scripts
    networks:
      - doris--hudi

  sparkmaster:
    image: apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkmaster_2.4.4:latest
    hostname: sparkmaster
    container_name: sparkmaster
    env_file:
      - ./hadoop.env
    ports:
      - "8080:8080"
      - "7077:7077"
      # JVM debugging port (will be mapped to a random port on host)
      - "5005"
    environment:
      - INIT_DAEMON_STEP=setup_spark
    links:
      - "hivemetastore"
      - "hiveserver"
      - "hive-metastore-postgresql"
      - "namenode"
    networks:
      - doris--hudi

  spark-worker-1:
    image: apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkworker_2.4.4:latest
    hostname: spark-worker-1
    container_name: spark-worker-1
    env_file:
      - ./hadoop.env
    depends_on:
      - sparkmaster
    ports:
      - "8081:8081"
      # JVM debugging port (will be mapped to a random port on host)
      - "5005"
    environment:
      - "SPARK_MASTER=spark://sparkmaster:7077"
    links:
      - "hivemetastore"
      - "hiveserver"
      - "hive-metastore-postgresql"
      - "namenode"
    networks:
      - doris--hudi

#  zookeeper:
#    image: 'bitnami/zookeeper:3.4.12-r68'
#    hostname: zookeeper
#    container_name: zookeeper
#    ports:
#      - "2181:2181"
#    environment:
#      - ALLOW_ANONYMOUS_LOGIN=yes
#    networks:
#      - doris--hudi

#  kafka:
#    image: 'bitnami/kafka:2.0.0'
#    hostname: kafkabroker
#    container_name: kafkabroker
#    ports:
#      - "9092:9092"
#    environment:
#      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
#      - ALLOW_PLAINTEXT_LISTENER=yes
#    networks:
#      - doris--hudi

  adhoc-1:
    image: apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkadhoc_2.4.4:latest
    hostname: adhoc-1
    container_name: adhoc-1
    env_file:
      - ./hadoop.env
    depends_on:
      - sparkmaster
    ports:
      - '4040:4040'
      # JVM debugging port (mapped to 5006 on the host)
      - "5006:5005"
    environment:
      - "SPARK_MASTER=spark://sparkmaster:7077"
    links:
      - "hivemetastore"
      - "hiveserver"
      - "hive-metastore-postgresql"
      - "namenode"
    volumes:
      - ./scripts:/var/scripts
    networks:
      - doris--hudi

  adhoc-2:
    image: apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkadhoc_2.4.4:latest
    hostname: adhoc-2
    container_name: adhoc-2
    env_file:
      - ./hadoop.env
    ports:
      # JVM debugging port (mapped to 5005 on the host)
      - "5005:5005"
    depends_on:
      - sparkmaster
    environment:
      - "SPARK_MASTER=spark://sparkmaster:7077"
    links:
      - "hivemetastore"
      - "hiveserver"
      - "hive-metastore-postgresql"
      - "namenode"
    volumes:
      - ./scripts:/var/scripts
    networks:
      - doris--hudi
