// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_query_sys_data_type", 'query,p0') {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tbName = "test_data_type"
    def dbName = "test_query_db"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE ${dbName}"

    sql """ DROP TABLE IF EXISTS ${tbName} """
    sql """
        create table if not exists ${tbName} (dt date, id int, name char(10), province char(10), os char(1), set1 hll hll_union, set2 bitmap bitmap_union)
        distributed by hash(id) buckets 1 properties("replication_num"="1");
    """

    qt_sql "select column_name, data_type from information_schema.columns where table_schema = '${dbName}' and table_name = '${tbName}'"
}
