#!/usr/bin/env zsh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e
mkdir -p data/sources/taxi
(cd data/sources/taxi; wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-11.parquet )
(cd data/sources/taxi; wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-12.parquet )

mkdir -p data/sources/github
(cd data/sources/github; wget http://data.gharchive.org/2015-11-{01..15}-{0..23}.json.gz)
