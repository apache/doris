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

import org.junit.Assert
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_add_key_partial_update", "nonConcurrent") {

    def table1 = "test_add_key_partial_update"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
        )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_mow_light_delete" = "false",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1),(2,2,2),(3,3,3);"
    sql "insert into ${table1} values(4,4,4),(5,5,5),(6,6,6);"
    sql "insert into ${table1} values(4,4,4),(5,5,5),(6,6,6);"
    sql "insert into ${table1} values(4,4,4),(5,5,5),(6,6,6);"
    sql "sync;"
    order_qt_sql "select * from ${table1};"

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()

        // block the schema change process before it change the shadow index to base index
        GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::process_alter_tablet.leave.sleep")

        sql "alter table ${table1} ADD COLUMN k2 int key;"

        Thread.sleep(1000)
        test {
            sql "delete from ${table1} where k1<=3;"
            exception "Can't do partial update when table is doing schema change."
        }

        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1 """
            time 1000
        }

        sql "set skip_delete_sign=true;"
        sql "sync;"
        qt_sql "select k1,k2,c1,c2,__DORIS_VERSION_COL__,__DORIS_DELETE_SIGN__ from ${table1} order by k1,k2,__DORIS_VERSION_COL__;"
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
