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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_group_commit_async_wal_msg_fault_injection","nonConcurrent") {
    def dbName = "regression_test_fault_injection_p0"
    def tableName = "wal_test"

    def getRowCount = { expectedRowCount ->
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until(
            {
                def result = sql "select count(*) from ${tableName}"
                logger.info("table: ${tableName}, rowCount: ${result}")
                return result[0][0] == expectedRowCount
            }
        )
    }

    // test successful group commit async load
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DISTRIBUTED BY HASH(`k`) 
        BUCKETS 5 
        properties("replication_num" = "1", "group_commit_interval_ms" = "10")
        """

    GetDebugPoint().clearDebugPointsForAllBEs()
    def tableId = getTableId(dbName, tableName)

    def exception = false;
        try {
            GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.get_wal_back_pressure_msg", [table_id:"${tableId}"])
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'group_commit', 'async_mode'
                unset 'label'
                file 'group_commit_wal_msg.csv'
                time 10000 
            }
            assertFalse(true);
        } catch (Exception e) {
            logger.info(e.getMessage())
            assertTrue(e.getMessage().contains('estimated wal bytes 0 Bytes'))
            exception = true;
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.get_wal_back_pressure_msg")
            assertTrue(exception)
        }

    // test failed group commit async load
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DISTRIBUTED BY HASH(`k`) 
        BUCKETS 5 
        properties("replication_num" = "1", "group_commit_interval_ms" = "10")
        """

    GetDebugPoint().clearDebugPointsForAllBEs()
    tableId = getTableId(dbName, tableName)

    exception = false;
        try {
            GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.get_wal_back_pressure_msg", [table_id:"${tableId}"])
            GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.err_st")
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'group_commit', 'async_mode'
                unset 'label'
                file 'group_commit_wal_msg.csv'
                time 10000 
            }
            assertFalse(true);
        } catch (Exception e) {
            logger.info(e.getMessage())
            assertTrue(e.getMessage().contains('estimated wal bytes 0 Bytes'))
            exception = true;
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.get_wal_back_pressure_msg")
            GetDebugPoint().disableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.err_st")
            assertTrue(exception)
        }

        // test group commit abort txn
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DISTRIBUTED BY HASH(`k`) 
        BUCKETS 5 
        properties("replication_num" = "1", "group_commit_interval_ms" = "10")
        """

    GetDebugPoint().clearDebugPointsForAllBEs()
    tableId = getTableId(dbName, tableName)

    exception = false;
        try {
            GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.get_wal_back_pressure_msg", [table_id:"${tableId}"])
            GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.err_status")
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'group_commit', 'async_mode'
                unset 'label'
                file 'group_commit_wal_msg.csv'
                time 10000 
            }
            assertFalse(true);
        } catch (Exception e) {
            logger.info(e.getMessage())
            assertTrue(e.getMessage().contains('abort txn'))
            exception = true;
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.get_wal_back_pressure_msg")
            GetDebugPoint().disableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.err_status")
            assertTrue(exception)
        }

    // test replay wal should success
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DISTRIBUTED BY HASH(`k`) 
        BUCKETS 5 
        properties("replication_num" = "1", "group_commit_interval_ms" = "4000")
    """
    GetDebugPoint().clearDebugPointsForAllBEs()
    try {
        GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.commit_error")
        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            set 'group_commit', 'async_mode'
            unset 'label'
            file 'group_commit_wal_msg.csv'
            time 10000
        }
        getRowCount(5)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.commit_error")
    }
}