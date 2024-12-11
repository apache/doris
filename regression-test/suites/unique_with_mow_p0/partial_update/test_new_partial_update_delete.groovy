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

suite('test_new_partial_update_delete') {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect( context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql "use ${db};"

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"

            try {
                def tableMorName1 = "test_new_partial_update_mor_delete1"
                sql "DROP TABLE IF EXISTS ${tableMorName1};"
                sql """ CREATE TABLE IF NOT EXISTS ${tableMorName1} (
                        `k1` int NOT NULL,
                        `c1` int,
                        `c2` int,
                        `c3` int,
                        `c4` int
                        )UNIQUE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES (
                        "disable_auto_compaction" = "true",
                        "enable_unique_key_merge_on_write" = "false",
                        "enable_mow_light_delete" = "true",
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}"); """
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains('enable_mow_light_delete property is only supported for unique merge-on-write table'))
            }

            try {
                def tableMorName2 = "test_new_partial_update_mor_delete2"
                sql "DROP TABLE IF EXISTS ${tableMorName2};"
                sql """ CREATE TABLE IF NOT EXISTS ${tableMorName2} (
                        `k1` int NOT NULL,
                        `c1` int,
                        `c2` int,
                        `c3` int,
                        `c4` int
                        )UNIQUE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES (
                        "disable_auto_compaction" = "true",
                        "enable_unique_key_merge_on_write" = "false",
                        "enable_mow_light_delete" = "false",
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}"); """
                sql """alter table ${tableMorName2} set ("enable_mow_light_delete"="true")"""
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains('enable_mow_light_delete property is only supported for unique merge-on-write table'))
            }

            def tableName1 = "test_new_partial_update_delete1"
            sql "DROP TABLE IF EXISTS ${tableName1};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableName1} (
                    `k1` int NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int,
                    `c4` int
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "enable_mow_light_delete" = "false",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """

            def output1 = sql "show create table ${tableName1}"
            assertTrue output1[0][1].contains("\"enable_mow_light_delete\" = \"false\"");
            sql "insert into ${tableName1} values(1,1,1,1,1)"
            // 1,1,1,1,1
            qt_sql1 "select * from ${tableName1} order by k1;"
            sql "delete from ${tableName1} where k1 = 1"
            // empty
            qt_sql2 "select * from ${tableName1} order by k1;"
            sql "set show_hidden_columns = true;"
            // 1,null,null,null,null,1
            qt_sql3 "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__ from ${tableName1} order by k1;"
            sql "set show_hidden_columns = false;"
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "insert into ${tableName1} (k1,c1) values(1,2)"
            // 1,2,NULL,NULL,NULL
            qt_sql4 "select * from ${tableName1} order by k1;"



            sql """alter table ${tableName1} set ("enable_mow_light_delete"="true") """
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"
            def output2 = sql "show create table ${tableName1}"
            assertTrue output2[0][1].contains("\"enable_mow_light_delete\" = \"true\"");
            sql "insert into ${tableName1} values(2,2,2,2,2)"
            // 1,2,NULL,NULL,NULL
            // 2,2,2,2,2
            qt_sql11 "select * from ${tableName1} order by k1;"
            sql "delete from ${tableName1} where k1 <= 2"
            // empty
            qt_sql12 "select * from ${tableName1} order by k1;"
            sql "set show_hidden_columns = true;"
            // empty
            qt_sql13 "select * from ${tableName1} order by k1;"
            sql "set show_hidden_columns = false;"
            sql "set skip_delete_predicate = true;"
            // 1,2,NULL,NULL,NULL
            // 2,2,2,2,2
            qt_sql14 "select * from ${tableName1} order by k1;"
            sql "set skip_delete_predicate = false;"
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "insert into ${tableName1} (k1,c1) values(2,3)"
            // 2,3,2,2,2
            qt_sql15 "select * from ${tableName1} order by k1;"
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"

            sql "drop table if exists ${tableName1};"



            // old planner
            try {
                def tableMorName3 = "test_new_partial_update_mor_delete3"
                sql "DROP TABLE IF EXISTS ${tableMorName3};"
                sql """ CREATE TABLE IF NOT EXISTS ${tableMorName3} (
                        `k1` int NOT NULL,
                        `c1` int,
                        `c2` int,
                        `c3` int,
                        `c4` int
                        )UNIQUE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES (
                        "disable_auto_compaction" = "true",
                        "enable_unique_key_merge_on_write" = "false",
                        "enable_mow_light_delete" = "true",
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}"); """
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains('enable_mow_light_delete property is only supported for unique merge-on-write table'))
            }

            try {
                def tableMorName4 = "test_new_partial_update_mor_delete4"
                sql "DROP TABLE IF EXISTS ${tableMorName4};"
                sql """ CREATE TABLE IF NOT EXISTS ${tableMorName4} (
                        `k1` int NOT NULL,
                        `c1` int,
                        `c2` int,
                        `c3` int,
                        `c4` int
                        )UNIQUE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES (
                        "disable_auto_compaction" = "true",
                        "enable_unique_key_merge_on_write" = "false",
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}"); """
                sql """alter table ${tableMorName4} set ("enable_mow_light_delete"="true")"""
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains('enable_mow_light_delete property is only supported for unique merge-on-write table'))
            }
            sql "set enable_nereids_planner=false"
            def tableName2 = "test_new_partial_update_delete2"
            sql "DROP TABLE IF EXISTS ${tableName2};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableName2} (
                    `k1` int NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int,
                    `c4` int
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "enable_mow_light_delete" = "false",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """

            def output3 = sql "show create table ${tableName2}"
            assertTrue output3[0][1].contains("\"enable_mow_light_delete\" = \"false\"");
            sql "insert into ${tableName2} values(1,1,1,1,1)"
            // 1,1,1,1,1
            qt_sql21 "select * from ${tableName2} order by k1;"
            sql "delete from ${tableName2} where k1 = 1"
            // empty
            qt_sql22 "select * from ${tableName2} order by k1;"
            sql "set show_hidden_columns = true;"
            // 1,null,null,null,1
            qt_sql23 "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__ from ${tableName2} order by k1;"
            sql "set show_hidden_columns = false;"
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "insert into ${tableName2} (k1,c1) values(1,2)"
            // 1,2,NULL,NULL,NULL
            qt_sql24 "select * from ${tableName2} order by k1;"



            sql """alter table ${tableName2} set ("enable_mow_light_delete"="true") """
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"
            def output4 = sql "show create table ${tableName2}"
            assertTrue output4[0][1].contains("\"enable_mow_light_delete\" = \"true\"");
            sql "insert into ${tableName2} values(2,2,2,2,2)"
            // 1,2,NULL,NULL,NULL
            // 2,2,2,2,2
            qt_sql31 "select * from ${tableName2} order by k1;"
            sql "delete from ${tableName2} where k1 <= 2"
            // empty
            qt_sql32 "select * from ${tableName2} order by k1;"
            sql "set show_hidden_columns = true;"
            // empty
            qt_sql33 "select * from ${tableName2} order by k1;"
            sql "set show_hidden_columns = false;"
            sql "set skip_delete_predicate = true;"
            // 1,2,NULL,NULL,NULL
            // 2,2,2,2,2
            qt_sql34 "select * from ${tableName2} order by k1;"
            sql "set skip_delete_predicate = false;"
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "insert into ${tableName2} (k1,c1) values(2,3)"
            // 2,3,2,2,2
            qt_sql35 "select * from ${tableName2} order by k1;"
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"

            sql "drop table if exists ${tableName2};"
        }
    }

    connect( context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
        sql "use ${db};"
        try {
            def tableAggName = "test_new_partial_update_agg_delete"
            sql "DROP TABLE IF EXISTS ${tableAggName};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableAggName} (
                    `k1` int NOT NULL,
                    `c1` int replace,
                    `c2` int replace,
                    `c3` int replace,
                    `c4` int replace
                    )AGGREGATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "enable_mow_light_delete" = "true",
                    "replication_num" = "1"); """
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains('enable_mow_light_delete property is only supported for unique merge-on-write table'))
        }

        try {
            def tableDupName = "test_new_partial_update_dup_delete"
            sql "DROP TABLE IF EXISTS ${tableDupName};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableDupName} (
                    `k1` int NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int,
                    `c4` int
                    )DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "enable_mow_light_delete" = "true",
                    "replication_num" = "1"); """
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains('enable_mow_light_delete property is only supported for unique merge-on-write table'))
        }
    }
}
