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

// A failed INSERT INTO ... SELECT FROM stream must not advance the stream
// offset. A later successful retry must consume the same rows exactly once.
suite("test_table_stream_consume_failure_rollback", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_consume_failure_rollback_db"
    sql "CREATE DATABASE test_table_stream_consume_failure_rollback_db"
    sql "USE test_table_stream_consume_failure_rollback_db"

    sql """
        CREATE TABLE consume_failure_base (
            id INT,
            value VARCHAR(16)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    sql """
        CREATE STREAM consume_failure_stream ON TABLE consume_failure_base
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """

    sql "INSERT INTO consume_failure_base VALUES (1, 'a'), (2, 'b')"
    sql "sync"
    sleep(1200)

    def normalizeRows = { rows ->
        rows.collect { row -> row.collect { value -> value == null ? "null" : value.toString() } }
    }
    def expectedRows = [["1", "a"], ["2", "b"]]
    assertEquals(expectedRows, normalizeRows(sql("SELECT id, value FROM consume_failure_stream ORDER BY id")))

    def consumptionBefore = sql """
        SELECT UNIT, CONSUMPTION_STATUS, LAG, LAST_CONSUMPTION_TIME
        FROM information_schema.table_stream_consumption
        WHERE DB_NAME = 'test_table_stream_consume_failure_rollback_db'
          AND STREAM_NAME = 'consume_failure_stream'
        ORDER BY UNIT
    """

    test {
        sql """
            INSERT INTO consume_failure_missing_target
            SELECT id, value FROM consume_failure_stream
        """
        exception "consume_failure_missing_target"
    }

    assertEquals(expectedRows, normalizeRows(sql("SELECT id, value FROM consume_failure_stream ORDER BY id")))
    def consumptionAfterFailure = sql """
        SELECT UNIT, CONSUMPTION_STATUS, LAG, LAST_CONSUMPTION_TIME
        FROM information_schema.table_stream_consumption
        WHERE DB_NAME = 'test_table_stream_consume_failure_rollback_db'
          AND STREAM_NAME = 'consume_failure_stream'
        ORDER BY UNIT
    """
    assertEquals(consumptionBefore, consumptionAfterFailure)

    sql """
        CREATE TABLE consume_failure_target (
            id INT,
            value VARCHAR(16)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO consume_failure_target
        SELECT id, value FROM consume_failure_stream
    """
    sql "sync"

    assertEquals(0, sql("SELECT id, value FROM consume_failure_stream").size())
    assertEquals(expectedRows, normalizeRows(sql("SELECT id, value FROM consume_failure_target ORDER BY id")))

    def lagRows = sql """
        SELECT LAG
        FROM information_schema.table_stream_consumption
        WHERE DB_NAME = 'test_table_stream_consume_failure_rollback_db'
          AND STREAM_NAME = 'consume_failure_stream'
    """
    assertTrue(lagRows.size() > 0)
    lagRows.each { row -> assertEquals("0", row[0].toString()) }
}
