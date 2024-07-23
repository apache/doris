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

suite('test_mix_partial_update') {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"

            def tableInsertName1 = "test_mix_partial_update"
            sql "DROP TABLE IF EXISTS ${tableInsertName1};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableInsertName1} (
                    `k1` int NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int,
                    `seq` int
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "function_column.sequence_col" = "seq",
                    "store_row_column" = "${use_row_store}"); """
            sql "insert into ${tableInsertName1} values(1,1,1,1,1),(1,1,1,1,10),(2,2,2,2,2),(3,3,3,3,3)"
            // 1,1,1,1,10
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select1 "select * from ${tableInsertName1} order by k1"
            sql "insert into ${tableInsertName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,10,1)"
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select2 "select * from ${tableInsertName1} order by k1"
            sql "insert into ${tableInsertName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,20,1)"
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select3 "select * from ${tableInsertName1} order by k1"
            sql "insert into ${tableInsertName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,1,0)"
            // error
            // 1,10,null,10,1
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select4 "select * from ${tableInsertName1} order by k1"
            sql "update ${tableInsertName1} set seq = 30 where k1 = 1"
            // 1,10,null,30,1
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select5 "select * from ${tableInsertName1} order by k1"

            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"

            sql "insert into ${tableInsertName1} (k1,seq) values(2,1)"
            // 2,2,2,2,1
            // 3,3,3,3,3
            qt_select6 "select * from ${tableInsertName1} order by k1"

            sql "insert into ${tableInsertName1} (k1,seq,__DORIS_DELETE_SIGN__) values(3,1,1)"
            // 2,2,2,2,1
            // 3,3,3,3,3
            qt_select7 "select * from ${tableInsertName1} order by k1"

            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"
            sql "insert into ${tableInsertName1} values(4,4,4,4,4)"
            // 2,2,2,2,1
            // 3,3,3,3,3
            // 4,4,4,4,4
            qt_select8 "select * from ${tableInsertName1} order by k1"

            sql "update ${tableInsertName1} set seq = 1 where k1 = 4"
            // 2,2,2,2,1
            // 3,3,3,3,3
            // 4,4,4,4,4
            qt_select9 "select * from ${tableInsertName1} order by k1"

            def tableStreamName1 = "test_mix_partial_update"
            sql "DROP TABLE IF EXISTS ${tableStreamName1};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableStreamName1} (
                    `k1` int NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int,
                    `seq` int
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "function_column.sequence_col" = "seq",
                    "store_row_column" = "${use_row_store}"); """

            streamLoad {
                table "${tableStreamName1}"
                set 'column_separator', ','
                set 'format', 'csv'
                file "test_mix_partial_update_load1.csv"
                time 10000 // limit inflight 10s
            }
            // sql "insert into ${tableInsertName1} values(1,1,1,1,1),(1,1,1,1,10),(2,2,2,2,2),(3,3,3,3,3)"
            // 1,1,1,1,10
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select1 "select * from ${tableStreamName1} order by k1"

            streamLoad {
                table "${tableStreamName1}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__'
                //set 'partial_columns', 'true'
                file "test_mix_partial_update_load2.csv"
                time 10000 // limit inflight 10s
            }
            //sql "insert into ${tableStreamName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,10,1)"
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select2 "select * from ${tableStreamName1} order by k1"


            streamLoad {
                table "${tableStreamName1}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__'
                //set 'partial_columns', 'true'
                file "test_mix_partial_update_load3.csv"
                time 10000 // limit inflight 10s
            }
            //sql "insert into ${tableStreamName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,20,1)"
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select3 "select * from ${tableStreamName1} order by k1"


            streamLoad {
                table "${tableStreamName1}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__'
                //set 'partial_columns', 'true'
                file "test_mix_partial_update_load4.csv"
                time 10000 // limit inflight 10s
            }
            //sql "insert into ${tableStreamName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,1,0)"
            // 1,10,null,10,1
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select4 "select * from ${tableStreamName1} order by k1"
            sql "update ${tableInsertName1} set seq = 30 where k1 = 1"
            // 1,10,null,30,1
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select5 "select * from ${tableStreamName1} order by k1"

            streamLoad {
                table "${tableStreamName1}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k1,seq'
                set 'partial_columns', 'true'
                file "test_mix_partial_update_load5.csv"
                time 10000 // limit inflight 10s
            }
            //sql "set enable_unique_key_partial_update=true;"
            //sql "set enable_insert_strict=false;"
            //sql "insert into ${tableStreamName1} (k1,seq) values(2,1)"
            // 2,2,2,2,1
            // 3,3,3,3,3
            qt_select6 "select * from ${tableStreamName1} order by k1"

            streamLoad {
                table "${tableStreamName1}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k1,seq,__DORIS_DELETE_SIGN__'
                set 'partial_columns', 'true'
                file "test_mix_partial_update_load6.csv"
                time 10000 // limit inflight 10s
            }
            //sql "set enable_unique_key_partial_update=true;"
            //sql "set enable_insert_strict=false;"
            // sql "insert into ${tableStreamName1} (k1,seq,__DORIS_DELETE_SIGN__) values(3,1,1)"
            // 2,2,2,2,1
            // 3,3,3,3,3
            qt_select7 "select * from ${tableStreamName1} order by k1"

            streamLoad {
                table "${tableStreamName1}"
                set 'column_separator', ','
                set 'format', 'csv'
                file "test_mix_partial_update_load7.csv"
                time 10000 // limit inflight 10s
            }
            // sql "insert into ${tableStreamName1} values(4,4,4,4,4)"
            // 2,2,2,2,1
            // 3,3,3,3,3
            // 4,4,4,4,4
            qt_select8 "select * from ${tableStreamName1} order by k1"

            sql "update ${tableStreamName1} set seq = 1 where k1 = 4"
            // 2,2,2,2,1
            // 3,3,3,3,3
            // 4,4,4,4,4
            qt_select9 "select * from ${tableStreamName1} order by k1"


            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"

            def tableInsertName2 = "test_mix_partial_update2"
            sql "DROP TABLE IF EXISTS ${tableInsertName2};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableInsertName2} (
                    `k1` int NOT NULL,
                    `c1` bigint NOT NULL auto_increment(100),
                    `c2` int,
                    `c3` int,
                    `c4` map<string, int>,
                    `c5` datetimev2(3) DEFAULT CURRENT_TIMESTAMP,
                    `c6` datetimev2(3) DEFAULT CURRENT_TIMESTAMP
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """
            
            sql "insert into ${tableInsertName2} (k1,c2,c3,c4) values(1,1,1,{'a':100,'b':100})"
            qt_select_A "select k1,c2,c3,c4 from ${tableInsertName2}"
            qt_select_AA "select count(distinct c1) from ${tableInsertName2}"
            qt_select_AAA"select count(*) from ${tableInsertName2} where c5 = c6"


            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "insert into ${tableInsertName2} (k1,c2,c3,c4) values(2,2,2,{'a':200,'b':200})"
            qt_select_B "select k1,c2,c3,c4 from ${tableInsertName2}"
            qt_select_BB "select count(distinct c1) from ${tableInsertName2}"
            qt_select_BBB "select count(*) from ${tableInsertName2} where c5 = c6"
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"

            sql "update ${tableInsertName2} set c1 = 100"
            qt_select_C "select k1,c2,c3,c4 from ${tableInsertName2}"
            qt_select_CC "select count(distinct c1) from ${tableInsertName2}"
            qt_select_CCC "select count(*) from ${tableInsertName2} where c5 = c6"

            // do light weight schema change
            sql """ ALTER TABLE ${tableInsertName2} add column `c7` datetimev2(3) DEFAULT CURRENT_TIMESTAMP after c6; """
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableInsertName2}' ORDER BY createtime DESC LIMIT 1 """
                time 60
            }

            sql "insert into ${tableInsertName2} (k1,c2,c3,c4) values(3,3,3,{'a':300,'b':300})"
            qt_select_D "select k1,c2,c3,c4 from ${tableInsertName2}"
            qt_select_DD "select count(distinct c1) from ${tableInsertName2}"
            qt_select_DDD "select count(*) from ${tableInsertName2} where c5 = c7"

            // do heavy weight schema change
            sql """ ALTER TABLE ${tableInsertName2} add column `k2` int after k1; """
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableInsertName2}' ORDER BY createtime DESC LIMIT 1 """
                time 60
            }

            sql "insert into ${tableInsertName2} (k1,k2,c2,c3,c4) values(4,4,4,4,{'a':400,'b':400})"
            qt_select_E "select k1,c2,c3,c4 from ${tableInsertName2}"
            qt_select_EE "select count(distinct c1) from ${tableInsertName2}"
            qt_select_EEE "select count(*) from ${tableInsertName2} where c5 = c7"

            def tableStreamName2 = "test_mix_partial_update2"
            sql "DROP TABLE IF EXISTS ${tableStreamName2};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableStreamName2} (
                    `k1` int NOT NULL,
                    `c1` bigint NOT NULL auto_increment(100),
                    `c2` int,
                    `c3` int,
                    `c4` map<string, int>,
                    `c5` datetimev2(3) DEFAULT CURRENT_TIMESTAMP,
                    `c6` datetimev2(3) DEFAULT CURRENT_TIMESTAMP
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """

            //sql "insert into ${tableInsertName2} (k1,c2,c3,c4) values(1,1,1,{'a':100,'b':100})"
            streamLoad {
                table "${tableStreamName2}"
                set 'columns', 'k1,c2,c3,c4'
                set 'format', 'csv'
                file "test_mix_partial_update_load_A.csv"
                time 10000 // limit inflight 10s
            }
            qt_select_A "select k1,c2,c3,c4 from ${tableStreamName2}"
            qt_select_AA "select count(distinct c1) from ${tableStreamName2}"
            qt_select_AAA "select count(*) from ${tableStreamName2} where c5 = c6"


            // sql "insert into ${tableStreamName2} (k1,c2,c3,c4) values(2,2,2,{'a':200,'b':200})"
            streamLoad {
                table "${tableStreamName2}"
                set 'columns', 'k1,c2,c3,c4'
                set 'partial_columns', 'true'
                set 'format', 'csv'
                file "test_mix_partial_update_load_B.csv"
                time 10000 // limit inflight 10s
            }
            qt_select_B "select k1,c2,c3,c4 from ${tableStreamName2}"
            qt_select_BB "select count(distinct c1) from ${tableStreamName2}"
            qt_select_BBB "select count(*) from ${tableStreamName2} where c5 = c6"

            sql "update ${tableStreamName2} set c1 = 100"
            qt_select_C "select k1,c2,c3,c4 from ${tableStreamName2}"
            qt_select_CC "select count(distinct c1) from ${tableStreamName2}"
            qt_select_CCC "select count(*) from ${tableStreamName2} where c5 = c6"

            // do light weight schema change
            sql """ ALTER TABLE ${tableStreamName2} add column `c7` datetimev2(3) DEFAULT CURRENT_TIMESTAMP after c6; """
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableStreamName2}' ORDER BY createtime DESC LIMIT 1 """
                time 60
            }

            // sql "insert into ${tableInsertName2} (k1,c2,c3,c4) values(3,3,3,{'a':300,'b':300})"
            streamLoad {
                table "${tableStreamName2}"
                set 'columns', 'k1,c2,c3,c4'
                set 'format', 'csv'
                file "test_mix_partial_update_load_C.csv"
                time 10000 // limit inflight 10s
            }
            qt_select_D "select k1,c2,c3,c4 from ${tableStreamName2}"
            qt_select_DD "select count(distinct c1) from ${tableStreamName2}"
            qt_select_DDD "select count(*) from ${tableStreamName2} where c5 = c7"

            // do heavy weight schema change
            sql """ ALTER TABLE ${tableStreamName2} add column `k2` int after k1; """
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableInsertName2}' ORDER BY createtime DESC LIMIT 1 """
                time 60
            }

            // sql "insert into ${tableInsertName2} (k1,k2,c2,c3,c4) values(4,4,4,4,{'a':400,'b':400})"
            streamLoad {
                table "${tableStreamName2}"
                set 'columns', 'k1,k2,c2,c3,c4'
                set 'format', 'csv'
                file "test_mix_partial_update_load_D.csv"
                time 10000 // limit inflight 10s
            }
            qt_select_E "select k1,c2,c3,c4 from ${tableStreamName2}"
            qt_select_EE "select count(distinct c1) from ${tableStreamName2}"
            qt_select_EEE "select count(*) from ${tableStreamName2} where c5 = c7"
        }
    }
}