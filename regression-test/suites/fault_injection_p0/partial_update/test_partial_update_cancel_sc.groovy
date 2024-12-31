
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

suite("test_partial_update_cancel_sc", "nonConcurrent") {

    def table1 = "test_delete_publish_skip_read"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int,
            `c4` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 4
        PROPERTIES (
            "enable_mow_light_delete" = "false",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3);"
    qt_sql "select * from ${table1} order by k1;"

    def block = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_convert_historical_rowsets.block")
        }
    }

    def unblock = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("SchemaChangeJob::_convert_historical_rowsets.block")
        }
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    try {

        block()

        sql "alter table ${table1} modify column c3 varchar(100);"

        def t1 = Thread.start {
            Thread.sleep(3000)
            unblock()
        }

        Thread.sleep(1000)
        sql "set enable_unique_key_partial_update=true;"
        sql "set enable_insert_strict=false;"
        sql "insert into ${table1}(k1,c2,c3) values(1,999,999),(2,999,888);"

        qt_sql "select * from ${table1} order by k1;"

        unblock()
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 """
            time 1000
        }
        def res = sql_return_maparray """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 """
        assertEquals(res.size(), 1)
        assertEquals(res[0].State, "CANCELLED")

    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

}
