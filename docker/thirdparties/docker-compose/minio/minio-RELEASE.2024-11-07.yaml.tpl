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

version: '3'

services:
  doris--minio:
    image: minio/minio:RELEASE.2024-11-07T00-52-20Z
    restart: always
    ports:
      - ${DOCKER_MINIO_EXTERNAL_PORT}:9000
    privileged: true
    healthcheck:
      test: [ "CMD", "bash", "-c", "mc ping --count 1 local" ]
      interval: 20s
      timeout: 60s
      retries: 120
    environment:
      - MINIO_ROOT_USER=minioadmin
      - MINIO_ROOT_PASSWORD=minioadmin
      - MINIO_REGION_NAME=us-east-1
      - MINIO_DOMAIN=myminio.com
      - TZ=Asia/Shanghai
    volumes:
      - ./script/minio_init.sh:/bin/minio_init.sh
    entrypoint : ["bash", "-c", "bash /bin/minio_init.sh"]
    networks:
      - doris--minio

networks:
  doris--minio:
    ipam:
      driver: default
      config:
        - subnet: 168.43.0.0/24
