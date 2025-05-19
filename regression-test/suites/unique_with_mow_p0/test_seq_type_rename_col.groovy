
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

suite("test_seq_type_rename_col", "p0") {

    def table1 = "test_seq_type_rename_col"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
        )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_mow_light_delete" = "false",
            "function_column.sequence_type" = "bigint",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1}(k1,c1,c2,__DORIS_SEQUENCE_COL__) values(1,1,1,1),(2,2,2,2),(3,3,3,3);"
    sql "insert into ${table1}(k1,c1,c2,__DORIS_SEQUENCE_COL__) values(4,4,4,4),(5,5,5,5),(6,6,6,6);"
    sql "sync;"
    qt_sql "select * from ${table1} order by k1;"

    sql "alter table ${table1} rename column c1 c1_rename;"
    

    qt_sql "select * from ${table1} order by k1;"
}
