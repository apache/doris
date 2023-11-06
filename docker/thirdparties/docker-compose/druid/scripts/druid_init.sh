#!/bin/bash
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

# Apache druid ingestion data

curl "http://${DRUID_COODRINATOR_HOST}:8081/druid/indexer/v1/task" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/kttm.json'

curl "http://${DRUID_COODRINATOR_HOST}:8081/druid/indexer/v1/task" -H "Content-Type:application/json" -X POST -d "@/mnt/scripts/data/nyc.json"

curl "http://${DRUID_COODRINATOR_HOST}:8081/druid/indexer/v1/task" -H "Content-Type:application/json" -X POST -d "@/mnt/scripts/data/xtrip.json"

curl "http://${DRUID_COODRINATOR_HOST}:8081/druid/indexer/v1/task" -H "Content-Type:application/json" -X POST -d "@/mnt/scripts/data/wikipiea.json"

curl "http://${DRUID_COODRINATOR_HOST}:8081/druid/indexer/v1/task" -H "Content-Type:application/json" -X POST -d "@/mnt/scripts/data/test_nested.json"

