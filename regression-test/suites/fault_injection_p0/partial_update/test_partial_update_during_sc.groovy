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

suite('test_partial_update_during_sc', 'nonConcurrent') {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
        // block the alter process on BE
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_do_process_alter_tablet.block")
        }
    
        for (def use_nereids : [true, false]) {
            for (def use_row_store : [false, true]) {
                logger.info("current params: use_nereids: ${use_nereids}, use_row_store: ${use_row_store}")
                connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                    sql "use ${db};"
                    if (use_nereids) {
                        sql "set enable_nereids_planner=true"
                        sql "set enable_fallback_to_original_planner=false"
                        sql "set enable_nereids_dml=true;"
                    } else {
                        sql "set enable_nereids_planner=false"
                        sql "set enable_nereids_dml=false;"
                    }
                    sql "sync;"

                    def tableName1 = "test_partial_update_during_sc"
                    sql "DROP TABLE IF EXISTS ${tableName1} FORCE;"
                    sql """ CREATE TABLE IF NOT EXISTS ${tableName1} (
                            `k1` int,
                            `k2` int,
                            `c1` int,
                            `c2` int,
                            `c3` int
                            )UNIQUE KEY(k1,k2)
                        DISTRIBUTED BY HASH(k1) BUCKETS 1
                        PROPERTIES (
                            "enable_unique_key_merge_on_write" = "true",
                            "enable_mow_light_delete" = "false",
                            "disable_auto_compaction" = "true",
                            "replication_num" = "1",
                            "store_row_column" = "${use_row_store}"); """

                    sql "insert into ${tableName1} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5);"
                    qt_sql1 "select * from ${tableName1} order by k1;"

                    sql "alter table ${tableName1} modify column c1 bigint;"
                    Thread.sleep(1000)

                    streamLoad {
                        table "${tableName1}"
                        set 'column_separator', ','
                        set 'format', 'csv'
                        set 'columns', 'k1,k2,c1'
                        set 'partial_columns', 'true'
                        file 'during_sc.csv'
                        time 10000
                        check { result, exception, startTime, endTime ->
                            if (exception != null) {
                                throw exception
                            }
                            log.info("Stream load result: ${result}".toString())
                            def json = parseJson(result)
                            assertEquals("fail", json.Status.toLowerCase())
                            assertTrue(json.Message.contains("Can't do partial update when table is doing schema change"))
                        }
                    }

                    test {
                        sql "delete from ${tableName1} where k1 <= 2;"
                        exception "Can't do partial update when table is doing schema change"
                    }

                    test {
                        sql "update ${tableName1} set c1=c1*10 where k1<=2;"
                        exception "Can't do partial update when table is doing schema change"
                    }

                    sql "set enable_unique_key_partial_update=true;"
                    sql "sync;"
                    test {
                        sql "insert into ${tableName1}(k1,k2,c1) values(1,1,999),(3,3,888);"
                        exception "Can't do partial update when table is doing schema change"
                    }
                    sql "set enable_unique_key_partial_update=false;"
                    sql "sync;"
                }
            }
        }
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
