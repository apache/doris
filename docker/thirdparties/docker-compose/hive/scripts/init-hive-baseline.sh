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

set -e
if [[ "${HIVE_DEBUG:-0}" == "1" ]]; then
    set -x
fi

. /mnt/scripts/hive-module-lib.sh

hadoop fs -mkdir -p /user/doris/
hadoop fs -mkdir -p /user/doris/suites/

copy_to_hdfs_if_selected "tpch1.db"
copy_to_hdfs_if_selected "paimon1"
copy_to_hdfs_if_selected "tvf_data"
copy_to_hdfs_if_selected "preinstalled_data"

if [[ ${enablePaimonHms:-false} == "true" ]]; then
    run_hive_hql /mnt/scripts/create_external_paimon_scripts/create_paimon_tables.hql "create_paimon_table.hql"
fi
