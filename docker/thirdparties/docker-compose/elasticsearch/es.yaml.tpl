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

version: "3.9"

services:
  doris--es_6:
    # es official not provide 6.x image for arm/v8, use compatible image.
    image: webhippie/elasticsearch:6.8
    ports:
      - ${DOCKER_ES_6_EXTERNAL_PORT}:9200
    environment:
      ELASTICSEARCH_CLUSTER_NAME: "elasticsearch6"
      ES_JAVA_OPTS: "-Xms256m -Xmx256m"
      discovery.type: "single-node"
      ELASTICSEARCH_XPACK_SECURITY_ENABLED: "false"
    volumes:
      - ./data/es6/:/usr/share/elasticsearch/data
    networks:
      - doris--es
    healthcheck:
      test: [ "CMD", "curl", "localhost:9200" ]
      interval: 30s
      timeout: 10s
      retries: 100
  doris--es_7:
    image: elasticsearch:7.17.5
    ports:
      - ${DOCKER_ES_7_EXTERNAL_PORT}:9200
    environment:
      cluster.name: "elasticsearch7"
      ES_JAVA_OPTS: "-Xms256m -Xmx256m"
      discovery.type: "single-node"
      xpack.security.enabled: "false"
    volumes:
      - ./data/es7/:/usr/share/elasticsearch/data
    networks:
      - doris--es
    healthcheck:
      test: [ "CMD", "curl", "localhost:9200" ]
      interval: 30s
      timeout: 10s
      retries: 100
  doris--es_8:
    image: elasticsearch:8.3.3
    ports:
      - ${DOCKER_ES_8_EXTERNAL_PORT}:9200
    environment:
      cluster.name: "elasticsearch8"
      ES_JAVA_OPTS: "-Xms256m -Xmx256m"
      discovery.type: "single-node"
      xpack.security.enabled: "false"
    volumes:
      - ./data/es8/:/usr/share/elasticsearch/data
    networks:
      - doris--es
    healthcheck:
      test: [ "CMD", "curl", "localhost:9200" ]
      interval: 30s
      timeout: 10s
      retries: 100
  doris--_init_data:
    image: webdevops/toolbox
    volumes:
      - ./scripts/:/mnt/scripts
    environment:
      ES_6_HOST: "doris--es_6"
      ES_7_HOST: "doris--es_7"
      ES_8_HOST: "doris--es_8"
    command: [ "sh","-c","/mnt/scripts/es_init.sh" ]
    depends_on:
      doris--es_6:
        condition: service_healthy
      doris--es_7:
        condition: service_healthy
      doris--es_8:
        condition: service_healthy
    networks:
      - doris--es

networks:
  doris--es: