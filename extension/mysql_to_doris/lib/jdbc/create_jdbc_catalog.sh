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

cur_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
home_dir=$(cd "${cur_dir}"/../.. && pwd)

source ${home_dir}/conf/env.conf

# mkdir files to store tables and tables.sql
mkdir -p ${home_dir}/result/mysql

path=${1:-${home_dir}/result/mysql/jdbc_catalog.sql}

rm -f $path

echo 'CREATE CATALOG IF NOT EXISTS '${doris_jdbc_catalog}'
PROPERTIES (
  "type"="jdbc",
  "user"="'${mysql_username}'",
  "password"="'${mysql_password}'",
  "jdbc_url"="jdbc:mysql://'${mysql_host}:${mysql_port}/${doris_jdbc_default_db}'?useSSL=false",
  "driver_url"="'${doris_jdcb_driver_url}'",
  "driver_class"="'${doris_jdbc_driver_class}'"
); ' >> $path
