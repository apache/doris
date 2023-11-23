#!/usr/bin/env bash
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

export FE_HOST=
export FE_QUERY_PORT=
export FE_WEB_PORT=
export FE_USER='root'
export FE_DB='test_query_qa'
export FE_PASSWORD=''

export MYSQL_HOST=
export MYSQL_PORT=
export MYSQL_USER=
export MYSQL_PASSWORD=
export MYSQL_DB=test_query_qa

export PALO_CLIENT_LOG_SQL=DEBUG
export PALO_CLIENT_STDOUT=DEBUG

root_path=$(pwd)
export PYTHONPATH=${root_path}:${root_path}/lib:${root_path}/sys:${root_path}/deploy:${root_path}/qe/palo2/lib

# hdfs broker set
export hdfs_data_path='hdfs://xxxxx/user/test/data'
export broker_property="WITH BROKER 'hdfs' ('username'='root', 'password'='')"
