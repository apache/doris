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

suite("test_row_ttl_basic") {
    sql "DROP TABLE IF EXISTS row_ttl_dup_basic"
    sql """
        CREATE TABLE row_ttl_dup_basic (
            k INT,
            event_time DATETIMEV2(6) NULL,
            v STRING
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "function_column.enable_row_ttl" = "true",
            "function_column.ttl_col" = "event_time",
            "function_column.ttl" = "1 day",
            "function_column.ttl_time_zone" = "+08:00"
        )
    """
    test {
        sql "SELECT k FROM row_ttl_dup_basic FOR VERSION AS OF 1"
        exception "FOR VERSION/TIME AS OF is not supported on tables with row TTL"
    }
    test {
        sql "SELECT k FROM row_ttl_dup_basic@incr()"
        exception "INCR query is not supported on tables with row TTL"
    }
    sql """
        INSERT INTO row_ttl_dup_basic(k, event_time, v) VALUES
            (1, now(6) - INTERVAL 2 DAY, 'expired'),
            (2, NULL, 'persistent'),
            (3, now(6), 'live')
    """
    order_qt_dup_filter "SELECT k, v FROM row_ttl_dup_basic ORDER BY k"
    test {
        sql "SELECT k, v FROM row_ttl_dup_basic /*+PREAGGOPEN*/"
        exception "PREAGGOPEN hint is not supported on tables with row TTL"
    }

    test {
        sql "CREATE MATERIALIZED VIEW row_ttl_sync_mv AS SELECT k, v FROM row_ttl_dup_basic"
        exception "synchronous materialized views do not support tables with row ttl"
    }
    test {
        sql """
            CREATE MATERIALIZED VIEW row_ttl_async_mv
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            AS SELECT k, v FROM row_ttl_dup_basic
        """
        exception "asynchronous materialized views do not support base tables with row ttl"
    }

    sql "SET show_hidden_columns = true"
    order_qt_source_time_copy """
        SELECT k, __DORIS_TTL_COL__ <=> event_time
        FROM row_ttl_dup_basic
        ORDER BY k
    """
    sql "SET show_hidden_columns = false"

    sql "DELETE FROM row_ttl_dup_basic WHERE k = 3"
    order_qt_dup_delete "SELECT k, v FROM row_ttl_dup_basic ORDER BY k"

    sql "DROP TABLE IF EXISTS row_ttl_mow_basic"
    sql """
        CREATE TABLE row_ttl_mow_basic (
            k INT,
            event_time DATETIMEV2(6) NULL DEFAULT "2099-01-01 00:00:00.000000",
            v STRING
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "light_schema_change" = "true",
            "function_column.enable_row_ttl" = "true",
            "function_column.ttl_col" = "event_time",
            "function_column.ttl" = "1 day",
            "function_column.ttl_time_zone" = "+08:00"
        )
    """
    sql "INSERT INTO row_ttl_mow_basic(k, event_time, v) VALUES (1, now(6), 'original')"
    sql "SET enable_unique_key_partial_update = true"
    sql "INSERT INTO row_ttl_mow_basic(k, v) VALUES (1, 'keep-expiration')"
    order_qt_partial_preserve "SELECT k, v FROM row_ttl_mow_basic ORDER BY k"
    sql "INSERT INTO row_ttl_mow_basic(k, event_time) VALUES (1, now(6) - INTERVAL 2 DAY)"
    order_qt_partial_recompute "SELECT k, v FROM row_ttl_mow_basic ORDER BY k"
    qt_point_query_expired "SELECT v FROM row_ttl_mow_basic WHERE k = 1"
    sql "INSERT INTO row_ttl_mow_basic(k, v) VALUES (2, 'new-default-source')"
    order_qt_partial_new_key "SELECT k, v FROM row_ttl_mow_basic ORDER BY k"
    sql "SET enable_unique_key_partial_update = false"

    // Keep rollup coverage separate because this rollup does not contain the skip-bitmap
    // hidden column required by flexible partial update.
    sql "DROP TABLE IF EXISTS row_ttl_mow_rollup_basic"
    sql """
        CREATE TABLE row_ttl_mow_rollup_basic (
            k INT,
            sub_k INT,
            event_time DATETIMEV2(6) NULL DEFAULT "2099-01-01 00:00:00.000000",
            v STRING
        ) UNIQUE KEY(k, sub_k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "function_column.enable_row_ttl" = "true",
            "function_column.ttl_col" = "event_time",
            "function_column.ttl" = "1 day",
            "function_column.ttl_time_zone" = "+08:00"
        )
    """
    sql "ALTER TABLE row_ttl_mow_rollup_basic ADD ROLLUP row_ttl_mow_rollup(sub_k, k, v)"
    waitingMVTaskFinishedByMvName(context.dbName, "row_ttl_mow_rollup_basic", "row_ttl_mow_rollup")
    sql """
        INSERT INTO row_ttl_mow_rollup_basic(k, sub_k, event_time, v) VALUES
            (1, 1, now(6) - INTERVAL 2 DAY, 'expired'),
            (2, 2, DEFAULT, 'new-default-source')
    """
    order_qt_partial_new_key_rollup """
        SELECT /*+ USE_MV(row_ttl_mow_rollup_basic.row_ttl_mow_rollup) */ k, v
        FROM row_ttl_mow_rollup_basic ORDER BY k
    """

    sql "DROP TABLE IF EXISTS row_ttl_direct_basic"
    test {
        sql """
            CREATE TABLE row_ttl_direct_basic (
                k INT,
                v STRING
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.enable_row_ttl" = "true"
            )
        """
        exception "direct row ttl is not supported"
    }

    sql "INSERT INTO row_ttl_mow_basic(k, event_time, v) VALUES (3, now(6), 'flex-original')"
    streamLoad {
        table "row_ttl_mow_basic"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        inputStream new ByteArrayInputStream('{"k":3,"v":"flex-keep-expiration"}\n'.getBytes())
        time 10000
    }
    order_qt_flexible_preserve "SELECT k, v FROM row_ttl_mow_basic ORDER BY k"
    streamLoad {
        table "row_ttl_mow_basic"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        inputStream new ByteArrayInputStream(
                '{"k":3,"event_time":"2000-01-01 00:00:00.000000"}\n'
                        .getBytes())
        time 10000
    }
    order_qt_flexible_recompute "SELECT k, v FROM row_ttl_mow_basic ORDER BY k"
    streamLoad {
        table "row_ttl_mow_basic"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        inputStream new ByteArrayInputStream('{"k":4,"v":"flex-new-default-source"}\n'.getBytes())
        time 10000
    }
    order_qt_flexible_new_key "SELECT k, v FROM row_ttl_mow_basic ORDER BY k"
    sql """
        INSERT INTO row_ttl_mow_rollup_basic(k, sub_k, event_time, v)
        VALUES (4, 4, DEFAULT, 'flex-new-default-source')
    """
    order_qt_flexible_new_key_rollup """
        SELECT /*+ USE_MV(row_ttl_mow_rollup_basic.row_ttl_mow_rollup) */ k, v
        FROM row_ttl_mow_rollup_basic ORDER BY k
    """

    test {
        sql """
            ALTER TABLE row_ttl_dup_basic ENABLE FEATURE "ROW_TTL"
            WITH PROPERTIES (
                "function_column.ttl_col" = "event_time",
                "function_column.ttl" = "1 day"
            )
        """
        exception "unknown feature name: ROW_TTL"
    }
    test {
        sql """
            ALTER TABLE row_ttl_mow_basic ENABLE FEATURE "SEQUENCE_LOAD"
            WITH PROPERTIES (
                "function_column.sequence_type" = "bigint"
            )
        """
        exception "row ttl and sequence column cannot be enabled at the same time"
    }

    sql "ALTER TABLE row_ttl_mow_basic ADD COLUMN extra INT NULL"
    order_qt_schema_change "SELECT k, v, extra FROM row_ttl_mow_basic ORDER BY k"

    test {
        sql "ALTER TABLE row_ttl_mow_basic DROP COLUMN event_time"
        exception "Can not drop a row ttl source or hidden column"
    }
    test {
        sql """
            ALTER TABLE row_ttl_mow_basic SET (
                "function_column.ttl" = "2 day")
        """
        exception "TTL properties can only be specified when creating a table"
    }
    test {
        sql """
            ALTER TABLE row_ttl_mow_basic SET (
                "function_column.enable_row_ttl" = "false")
        """
        exception "TTL properties can only be specified when creating a table"
    }

    test {
        sql """
            CREATE TABLE row_ttl_missing_ttl_rejected (
                k INT,
                event_time DATETIMEV2(6),
                v STRING
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.enable_row_ttl" = "true",
                "function_column.ttl_col" = "event_time"
            )
        """
        exception "function_column.ttl_col and function_column.ttl must be set together"
    }
    test {
        sql """
            CREATE TABLE row_ttl_missing_col_rejected (
                k INT,
                event_time DATETIMEV2(6),
                v STRING
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.enable_row_ttl" = "true",
                "function_column.ttl" = "1 day"
            )
        """
        exception "function_column.ttl_col and function_column.ttl must be set together"
    }
    test {
        sql """
            CREATE TABLE row_ttl_source_key_rejected (
                k INT,
                event_time DATETIMEV2(6),
                v STRING
            ) DUPLICATE KEY(k, event_time)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.enable_row_ttl" = "true",
                "function_column.ttl_col" = "event_time",
                "function_column.ttl" = "1 day"
            )
        """
        exception "row ttl column must be a value column: event_time"
    }
    test {
        sql """
            CREATE TABLE row_ttl_source_type_rejected (
                k INT,
                event_time INT,
                v STRING
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.enable_row_ttl" = "true",
                "function_column.ttl_col" = "event_time",
                "function_column.ttl" = "1 day"
            )
        """
        exception "row ttl column only supports DATE/DATETIME types: event_time"
    }
    test {
        sql """
            CREATE TABLE row_ttl_generated_source_rejected (
                k INT,
                base_time DATETIMEV2(6),
                event_time DATETIMEV2(6) GENERATED ALWAYS AS (base_time),
                v STRING
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.enable_row_ttl" = "true",
                "function_column.ttl_col" = "event_time",
                "function_column.ttl" = "1 day",
                "function_column.ttl_time_zone" = "+08:00"
            )
        """
        exception "row ttl column does not support generated columns: event_time"
    }
    test {
        sql """
            CREATE TABLE row_ttl_missing_time_zone_rejected (
                k INT,
                event_time DATETIMEV2(6),
                v STRING
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.enable_row_ttl" = "true",
                "function_column.ttl_col" = "event_time",
                "function_column.ttl" = "1 day"
            )
        """
        exception "function_column.ttl_time_zone is required"
    }

    test {
        sql """
            CREATE TABLE row_ttl_agg_rejected (
                k INT,
                event_time DATETIMEV2(6) REPLACE,
                v BIGINT SUM
            ) AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.enable_row_ttl" = "true",
                "function_column.ttl_col" = "event_time",
                "function_column.ttl" = "1 day"
            )
        """
        exception "row ttl does not support AGG_KEYS"
    }
}
