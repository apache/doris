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

# es 6
# create index test1
# shellcheck disable=SC2154
curl "http://${ES_6_HOST}:9200/test1" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es6_test1.json"
# create index test2
curl "http://${ES_6_HOST}:9200/test2_20220808" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es6_test2.json'
# put data
curl "http://${ES_6_HOST}:9200/test1/doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_6_HOST}:9200/test1/doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_6_HOST}:9200/test1/doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
curl "http://${ES_6_HOST}:9200/test2_20220808/doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_6_HOST}:9200/test2_20220808/doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_6_HOST}:9200/test2_20220808/doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'

# es7
# create index test1
curl "http://${ES_7_HOST}:9200/test1" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es7_test1.json"
# create index test2
curl "http://${ES_7_HOST}:9200/test2_20220808" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es7_test2.json'
# put data
curl "http://${ES_7_HOST}:9200/test1/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_7_HOST}:9200/test1/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_7_HOST}:9200/test1/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
curl "http://${ES_7_HOST}:9200/test2_20220808/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_7_HOST}:9200/test2_20220808/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_7_HOST}:9200/test2_20220808/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'

# es8
# create index test1
curl "http://${ES_8_HOST}:9200/test1" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es7_test1.json"
# create index test2
curl "http://${ES_8_HOST}:9200/test2_20220808" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es7_test2.json'
# put data
curl "http://${ES_8_HOST}:9200/test1/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_8_HOST}:9200/test1/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_8_HOST}:9200/test1/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
curl "http://${ES_8_HOST}:9200/test2_20220808/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_8_HOST}:9200/test2_20220808/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_8_HOST}:9200/test2_20220808/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
