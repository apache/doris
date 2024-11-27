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

version: "2.1"

services:
  doris--oceanbase:
    image: oceanbase/oceanbase-ce:4.2.1
    restart: always
    environment:
      MODE: slim
      OB_MEMORY_LIMIT: 5G
      TZ: Asia/Shanghai
    ports:
      - ${DOCKER_OCEANBASE_EXTERNAL_PORT}:2881
    healthcheck:
      test: ["CMD-SHELL", "obclient -h127.1 -uroot@sys -P2881  -e 'SELECT 1'"]
      interval: 5s
      timeout: 60s
      retries: 120
    volumes:
      - ./init:/root/boot/init.d
    networks:
      - doris--oceanbase
  doris--oceanbase-hello-world:
    image: hello-world
    depends_on:
      doris--oceanbase:
        condition: service_healthy
    networks:
      - doris--oceanbase
networks:
  doris--oceanbase:
    ipam:
      driver: default
      config:
        - subnet: 168.32.0.0/24
