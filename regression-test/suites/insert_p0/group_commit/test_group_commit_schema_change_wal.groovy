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

suite("test_group_commit_schema_change_wal", "nonConcurrent") {
    def alterPoint = "FE.SchemaChangeJobV2.runRunning.block"
    def loadPoint = "LoadBlockQueue._finish_group_commit_load.load_error"
    def replayPoint = "WalTable.replay_wals.stop"
    def convertPoint = isCloudMode() ? "CloudSchemaChangeJob::process_alter_tablet.block"
                                     : "SchemaChangeJob::_do_process_alter_tablet.block"

    def awaitAlter = { expected ->
        Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until {
            def jobs = sql_return_maparray """SHOW ALTER TABLE COLUMN
                WHERE TableName='test_group_commit_schema_change_wal'
                ORDER BY CreateTime DESC LIMIT 1"""
            return !jobs.isEmpty() && jobs[0].State == expected
        }
    }
    def replayInState = { state ->
        sql "DROP TABLE IF EXISTS test_group_commit_schema_change_wal"
        sql """CREATE TABLE test_group_commit_schema_change_wal (
            id INT NOT NULL, v INT NULL, other INT NULL
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "light_schema_change"="true",
                   "group_commit_interval_ms"="1000")"""
        sql "INSERT INTO test_group_commit_schema_change_wal VALUES (1,10,100)"
        try {
            if (state == "CANCELLED") {
                // Hold the BE task, leaving the FE job mutex available for CANCEL ALTER.
                GetDebugPoint().enableDebugPointForAllBEs(convertPoint)
            } else {
                GetDebugPoint().enableDebugPointForAllFEs(alterPoint)
            }
            sql "ALTER TABLE test_group_commit_schema_change_wal MODIFY COLUMN v BIGINT NULL"
            awaitAlter("RUNNING")
            GetDebugPoint().enableDebugPointForAllBEs(loadPoint)
            GetDebugPoint().enableDebugPointForAllBEs(replayPoint)
            // The WAL contains both the base INT and the shadow BIGINT with the same unique id.
            sql "SET group_commit = async_mode"
            sql "INSERT INTO test_group_commit_schema_change_wal VALUES (2,20,200),(3,NULL,300)"
            sql "SET group_commit = off_mode"
            if (state == "FINISHED") {
                // Let the normal WAL-drain timeout expire before replaying with the new schema.
                GetDebugPoint().disableDebugPointForAllFEs(alterPoint)
                awaitAlter("FINISHED")
            } else if (state == "CANCELLED") {
                sql "CANCEL ALTER TABLE COLUMN FROM test_group_commit_schema_change_wal"
                awaitAlter("CANCELLED")
            }
            GetDebugPoint().disableDebugPointForAllBEs(replayPoint)
            Awaitility.await().atMost(360, SECONDS).pollInterval(1, SECONDS).until {
                return (sql "SELECT count(*) FROM test_group_commit_schema_change_wal")[0][0] == 3
            }
            GetDebugPoint().disableDebugPointForAllFEs(alterPoint)
            if (state == "RUNNING") {
                awaitAlter("FINISHED")
            }
        } finally {
            sql "SET group_commit = off_mode"
            GetDebugPoint().disableDebugPointForAllBEs(convertPoint)
            GetDebugPoint().disableDebugPointForAllBEs(loadPoint)
            GetDebugPoint().disableDebugPointForAllBEs(replayPoint)
            GetDebugPoint().disableDebugPointForAllFEs(alterPoint)
        }
    }

    sql "SET group_commit = off_mode"
    replayInState("RUNNING")
    order_qt_running "SELECT * FROM test_group_commit_schema_change_wal"
    replayInState("FINISHED")
    order_qt_finished "SELECT * FROM test_group_commit_schema_change_wal"
    replayInState("CANCELLED")
    order_qt_cancelled "SELECT * FROM test_group_commit_schema_change_wal"
}
