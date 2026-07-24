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

suite("test_row_binlog_tablet_schedule_basic", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_row_binlog_tablet_schedule_basic FORCE"

    sql """
        CREATE TABLE test_row_binlog_tablet_schedule_basic (
            k1 INT,
            k2 INT,
            v1 INT,
            v2 STRING
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "3",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """
        INSERT INTO test_row_binlog_tablet_schedule_basic VALUES
            (1, 1, 10, '10'),
            (2, 2, 20, '20')
    """
    sql "SET enable_unique_key_partial_update = true"
    sql "INSERT INTO test_row_binlog_tablet_schedule_basic(k1, k2, v2) VALUES (2, 2, '200')"
    sql "INSERT INTO test_row_binlog_tablet_schedule_basic(k1, k2, v1) VALUES (2, 2, 2000)"
    sql "SET enable_unique_key_partial_update = false"
    sql "DELETE FROM test_row_binlog_tablet_schedule_basic WHERE k1 = 1 AND k2 = 1"

    def baseRows = sql_return_maparray """
        SELECT COUNT(*) AS row_count
        FROM test_row_binlog_tablet_schedule_basic
    """
    assertEquals(1, baseRows[0].values().first() as int)

    def binlogRows = sql_return_maparray """
        SELECT COUNT(*) AS binlog_count
        FROM binlog("table" = "test_row_binlog_tablet_schedule_basic")
    """
    assertTrue((binlogRows[0].values().first() as int) >= 5,
            "row binlog should contain insert, partial update, and delete records")

    def opRows = sql_return_maparray """
        SELECT __DORIS_BINLOG_OP__ AS op, COUNT(*) AS op_count
        FROM binlog("table" = "test_row_binlog_tablet_schedule_basic")
        GROUP BY __DORIS_BINLOG_OP__
    """
    assertTrue(opRows.size() >= 2, "row binlog should expose more than one operation type")

    def tablets = sql_return_maparray "SHOW TABLETS FROM test_row_binlog_tablet_schedule_basic"
    assertTrue(tablets.size() >= 1, "base tablets should be visible for the replicated row-binlog table")
    tablets.each {
        assertTrue(it.BackendId != null, "SHOW TABLETS should expose BackendId")
        assertTrue(it.PathHash != null, "SHOW TABLETS should expose PathHash")
    }
}
