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

name: doris--mongodb

services:
  mongodb:
    image: mongo:8.0.20
    container_name: doris--mongodb
    ports:
      - "${MONGODB_PORT:-27017}:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: mongodb_test
      MONGO_INITDB_ROOT_PASSWORD: mongodb_test
    volumes:
      - ./init:/docker-entrypoint-initdb.d:ro
    # Override both image-declared volumes so recreating this disposable fixture leaves no data volumes.
    tmpfs:
      - /data/db:size=1g
      - /data/configdb:size=64m
    healthcheck:
      test:
        - CMD
        - mongosh
        - --quiet
        # The init server binds only loopback; service DNS reaches only the final network-facing server.
        - --host
        - mongodb
        - --username
        - mongodb_test
        - --password
        - mongodb_test
        - --authenticationDatabase
        - admin
        - --eval
        - >-
          const fixture = db.getSiblingDB('doris_mongodb_test');
          quit(db.adminCommand('ping').ok && fixture.large_records.countDocuments() === 2081
          && fixture.getCollection('_schema').countDocuments({table: 'numeric_limits'}) === 1
          && db.getSiblingDB('doris_mongodb_native').typed_records.countDocuments() === 4
          && db.getSiblingDB('doris_mongodb_native').large_records.countDocuments() === 2081
          && db.getSiblingDB('doris_mongodb_native').record_view.countDocuments() === 2
          && db.getSiblingDB('doris_mongodb_native').measurements.countDocuments() === 3
          && db.getSiblingDB('doris_mongodb_native').wide_records.countDocuments() === 1
          && db.getSiblingDB('doris_mongodb_native').tuple_validator.countDocuments() === 1
          && db.getSiblingDB('doris_mongodb_native').pattern_validator.countDocuments() === 1
          && db.getSiblingDB('doris_mongodb_native').dependency_validator.countDocuments() === 1
          && db.getSiblingDB('doris_mongodb_native').nested_pattern_validator.countDocuments() === 1
          && db.getSiblingDB('doris_mongodb_pagination').getCollectionNames().length === 140 ? 0 : 1);
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 10s
