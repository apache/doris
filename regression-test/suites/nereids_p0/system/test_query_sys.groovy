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

suite("test_query_sys", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "use test_query_db;"

    def tableName = "test"
    sql "SELECT DATABASE();"
    sql "SELECT \"welecome to my blog!\";"
    sql "describe ${tableName};"
    sql "select version();"
    sql "select rand();"
    sql "select rand(20);"
    sql "select random();"
    sql "select random(20);"
    sql "SELECT CONNECTION_ID();"
    sql "SELECT CURRENT_USER();"
    // sql "select now();"
    sql "select localtime();"
    sql "select localtimestamp();"
    sql "select pi();"
    sql "select e();"
    sql "select sleep(2);"

    // INFORMATION_SCHEMA
    sql "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema=\"test_query_db\" and TABLE_TYPE = \"BASE TABLE\" order by table_name"
    sql "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = \"${tableName}\" AND table_schema =\"test_query_db\" AND column_name LIKE \"k%\""
}
