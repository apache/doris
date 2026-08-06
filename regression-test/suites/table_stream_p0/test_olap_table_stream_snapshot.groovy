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

suite("test_olap_table_stream_snapshot", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_olap_table_stream_snapshot_db"
    sql "CREATE DATABASE test_olap_table_stream_snapshot_db"
    sql "USE test_olap_table_stream_snapshot_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "SET show_hidden_columns=false"

    def normRows = { rows ->
        rows.collect { row -> row.collect { value -> value == null ? "null" : value.toString() } }
    }
    def checkRows = { expected, query ->
        assertEquals(expected, normRows(sql(query)))
    }
    def waitVisible = {
        sql "sync"
        sleep(1200)
    }
    def streamConsumption = { streamName ->
        normRows(sql("""
            SELECT UNIT, CONSUMPTION_STATUS, LAG, LAST_CONSUMPTION_TIME
            FROM information_schema.table_stream_consumption
            WHERE DB_NAME = 'test_olap_table_stream_snapshot_db'
              AND STREAM_NAME = '${streamName}'
            ORDER BY UNIT
        """))
    }
    def checkNoConsumptionChange = { streamName, action ->
        def before = streamConsumption(streamName)
        action()
        def after = streamConsumption(streamName)
        assertEquals(before, after)
    }
    def checkStreamVirtualColumnsHidden = { streamName ->
        test {
            sql "SELECT __DORIS_STREAM_CHANGE_TYPE_COL__ FROM ${streamName}@snapshot()"
            exception "__DORIS_STREAM_CHANGE_TYPE_COL__"
        }
        test {
            sql "SELECT __DORIS_STREAM_SEQUENCE_COL__ FROM ${streamName}@snapshot()"
            exception "__DORIS_STREAM_SEQUENCE_COL__"
        }
    }

    // 1) DUP + append_only + show_initial_rows=true + non-partitioned table.
    // snapshot reads the stream creation snapshot and does not advance the stream offset.
    sql """
        CREATE TABLE dup_np_true (
            id INT,
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql "INSERT INTO dup_np_true VALUES (1, 10), (2, 20)"
    waitVisible()
    sql """
        CREATE STREAM s_dup_np_true ON TABLE dup_np_true
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "true"
        )
    """
    sql "INSERT INTO dup_np_true VALUES (3, 30), (4, 40)"
    waitVisible()
    checkRows([],
            "SELECT id, v FROM s_dup_np_true@snapshot() ORDER BY id")
    checkStreamVirtualColumnsHidden("s_dup_np_true")
    sql """
        CREATE TABLE snapshot_sink_dup_true (
            id INT,
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    checkNoConsumptionChange("s_dup_np_true", {
        sql "INSERT INTO snapshot_sink_dup_true SELECT id, v FROM s_dup_np_true@snapshot()"
    })
    checkRows([["1", "10"], ["2", "20"], ["3", "30"], ["4", "40"]],
            "SELECT id, v FROM s_dup_np_true ORDER BY id")

    // 2) DUP + append_only + show_initial_rows=false + non-partitioned table.
    // After ordinary stream consumption, snapshot moves to the consumed offset and remains
    // independent from later unconsumed appends.
    sql """
        CREATE TABLE dup_np_false (
            id INT,
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql "INSERT INTO dup_np_false VALUES (1, 10), (2, 20)"
    waitVisible()
    sql """
        CREATE STREAM s_dup_np_false ON TABLE dup_np_false
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO dup_np_false VALUES (3, 30)"
    waitVisible()
    checkRows([["1", "10"], ["2", "20"]],
            "SELECT id, v FROM s_dup_np_false@snapshot() ORDER BY id")
    checkRows([["3", "30"]],
            "SELECT id, v FROM s_dup_np_false ORDER BY id")
    sql """
        CREATE TABLE snapshot_consume_dup_false (
            id INT,
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO snapshot_consume_dup_false SELECT id, v FROM s_dup_np_false"
    waitVisible()
    assertEquals(0, sql("SELECT id, v FROM s_dup_np_false ORDER BY id").size())
    sql "INSERT INTO dup_np_false VALUES (4, 40)"
    waitVisible()
    checkRows([["1", "10"], ["2", "20"], ["3", "30"]],
            "SELECT id, v FROM s_dup_np_false@snapshot() ORDER BY id")
    checkRows([["4", "40"]],
            "SELECT id, v FROM s_dup_np_false ORDER BY id")

    // 3) MOW + min_delta + show_initial_rows=true + non-partitioned table.
    // snapshot must reconstruct the old image across update/delete/insert changes.
    sql """
        CREATE TABLE mow_md_true (
            id INT,
            v INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO mow_md_true VALUES (1, 10), (2, 20), (3, 30)"
    waitVisible()
    sql """
        CREATE STREAM s_mow_md_true ON TABLE mow_md_true
        PROPERTIES (
            "type" = "min_delta",
            "show_initial_rows" = "true"
        )
    """
    sql "INSERT INTO mow_md_true VALUES (1, 11)"
    sql "DELETE FROM mow_md_true WHERE id = 2"
    sql "INSERT INTO mow_md_true VALUES (4, 40)"
    waitVisible()
    checkRows([],
            "SELECT id, v FROM s_mow_md_true@snapshot() ORDER BY id")

    // 4) MOW + append_only + show_initial_rows=false + non-partitioned table.
    // snapshot is a table-image read and is not affected by append-only change filtering.
    sql """
        CREATE TABLE mow_append_false (
            id INT,
            v INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO mow_append_false VALUES (1, 10), (2, 20)"
    waitVisible()
    sql """
        CREATE STREAM s_mow_append_false ON TABLE mow_append_false
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO mow_append_false VALUES (1, 11)"
    sql "DELETE FROM mow_append_false WHERE id = 2"
    sql "INSERT INTO mow_append_false VALUES (3, 30)"
    waitVisible()
    checkRows([["1", "10"], ["2", "20"]],
            "SELECT id, v FROM s_mow_append_false@snapshot() ORDER BY id")
    checkRows([["3", "30"]],
            "SELECT id, v FROM s_mow_append_false ORDER BY id")

    // 5) DUP + append_only + show_initial_rows=true + range partitions.
    // snapshot includes the historical partition image but excludes post-stream partition data.
    sql """
        CREATE TABLE dup_part_true (
            id INT,
            v INT
        )
        DUPLICATE KEY(id)
        PARTITION BY RANGE(id)
        (
            PARTITION p1 VALUES LESS THAN (10),
            PARTITION p2 VALUES [(10), (20))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql "INSERT INTO dup_part_true VALUES (1, 10)"
    waitVisible()
    sql """
        CREATE STREAM s_dup_part_true ON TABLE dup_part_true
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "true"
        )
    """
    sql "INSERT INTO dup_part_true VALUES (11, 110)"
    waitVisible()
    checkRows([],
            "SELECT id, v FROM s_dup_part_true@snapshot() ORDER BY id")
    sql """
        CREATE TABLE snapshot_sink_part_true (
            id INT,
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    checkNoConsumptionChange("s_dup_part_true", {
        sql "INSERT INTO snapshot_sink_part_true SELECT id, v FROM s_dup_part_true@snapshot()"
    })
    checkRows([["1", "10"], ["11", "110"]],
            "SELECT id, v FROM s_dup_part_true ORDER BY id")
    sql "DROP DATABASE IF EXISTS test_olap_table_stream_snapshot_db"
}
