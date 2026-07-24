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

// After creating a stream, make its base table unavailable (DROP / RENAME)
// and verify how the stream is reflected in information_schema.table_streams.
suite("test_table_stream_base_table_unavailable") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_base_unavail_db"
    sql "CREATE DATABASE test_table_stream_base_unavail_db"
    sql "USE test_table_stream_base_unavail_db"

    def db = "test_table_stream_base_unavail_db"

    def fetchRow = { streamName ->
        def rows = sql """
            select BASE_TABLE_NAME, BASE_TABLE_DB, BASE_TABLE_CTL, BASE_TABLE_TYPE,
                   IS_STALE, STALE_REASON
            from information_schema.table_streams
            where DB_NAME = '${db}' and STREAM_NAME = '${streamName}'
        """
        assertEquals(1, rows.size())
        return rows[0]
    }

    // --- A. drop the base table; the stream is still listed but its base
    //      table is unreachable and reported as N/A. ---
    sql """
        CREATE TABLE base_drop (
            k1 INT, v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE STREAM s_drop ON TABLE base_drop
        PROPERTIES ("type" = "append_only")
    """

    assertEquals("base_drop", fetchRow("s_drop")[0])

    sql "DROP TABLE base_drop FORCE"
    def afterDrop = fetchRow("s_drop")
    assertEquals("N/A", afterDrop[0])
    assertEquals("N/A", afterDrop[1])
    assertEquals("N/A", afterDrop[2])
    assertEquals("N/A", afterDrop[3])

    test {
        sql "SELECT k1, v1 FROM s_drop"
        exception "Unknown base table"
    }

    // --- B. rename the base table; the stream must reflect the new name. ---
    sql """
        CREATE TABLE base_rename (
            k1 INT, v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE STREAM s_rename ON TABLE base_rename
        PROPERTIES ("type" = "append_only")
    """

    assertEquals("base_rename", fetchRow("s_rename")[0])
    sql "ALTER TABLE base_rename RENAME base_renamed"
    assertEquals("base_renamed", fetchRow("s_rename")[0])

    // Rename must preserve the base-table identity used by the stream.
    sql "INSERT INTO base_renamed VALUES (1, 10)"
    sql "sync"
    sleep(1200)
    assertEquals([["1", "10"]], sql("SELECT k1, v1 FROM s_rename ORDER BY k1").collect {
        row -> row.collect { value -> value.toString() }
    })

}
