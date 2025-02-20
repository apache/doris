
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

suite("test_partial_update_lookup_row_key", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect( context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql "use ${db};"
            sql "sync;"

            def tableName = "test_partial_update_publish_conflict_seq"
            sql """ DROP TABLE IF EXISTS ${tableName} force;"""
            sql """ CREATE TABLE ${tableName} (
                `k` int(11) NULL, 
                `v1` BIGINT NULL,
                `v2` BIGINT NULL,
                `v3` BIGINT NULL,
                `v4` BIGINT NULL,
                ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "function_column.sequence_col" = "v1",
                "store_row_column" = "${use_row_store}"); """

            sql """ insert into ${tableName} values
                    (1,400,1,1,1),(2,100,2,2,2),(3,30,3,3,3),(4,300,4,4,4);"""
            sql """ insert into ${tableName} values
                    (1,100,1,1,1),(2,400,2,2,2),(3,100,3,3,3),(4,200,4,4,4);"""
            sql """ insert into ${tableName} values
                    (1,200,1,1,1),(2,200,2,2,2),(3,300,3,3,3),(4,400,4,4,4);"""
            sql """ insert into ${tableName} values
                    (1,300,1,1,1),(2,300,2,2,2),(3,400,3,3,3),(4,100,4,4,4);"""
            qt_1 "select * from ${tableName} order by k;"
            // lookup_row_key will find key rowset with highest version to lowest version
            // the index of valid segment for each key will be in the search seqeuence

            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${tableName}(k,v2) values(1,99),(2,99),(3,99),(4,99),(5,99),(6,99);"
            qt_1 "select *,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"


            sql "truncate table ${tableName};"
            sql """ insert into ${tableName} values
                    (1,400,1,1,1),(2,100,2,2,2),(3,30,3,3,3),(4,300,4,4,4);"""
            sql """ insert into ${tableName} values
                    (1,100,1,1,1),(2,400,2,2,2),(3,100,3,3,3),(4,200,4,4,4);"""
            sql """ insert into ${tableName} values
                    (1,200,1,1,1),(2,200,2,2,2),(3,300,3,3,3),(4,400,4,4,4);"""
            sql """ insert into ${tableName} values
                    (1,300,1,1,1),(2,300,2,2,2),(3,400,3,3,3),(4,100,4,4,4);"""
            qt_2 "select * from ${tableName} order by k;"

            sql "insert into ${tableName}(k,v1,v3) values(1,500,88),(2,500,88),(3,300,88),(4,200,88),(5,200,88),(6,200,88);"
            qt_2 "select *,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
        }
    }
}
