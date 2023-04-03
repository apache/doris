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

    sql """ DROP TABLE IF EXISTS array_test """
    sql """
        CREATE TABLE `array_test` (
        `id` int(11) NULL COMMENT "",
        `c_array` ARRAY<int(11)> NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    """
    qt_sql "select column_name, data_type from information_schema.columns where table_schema = '${dbName}' and table_name = 'array_test'"
}
