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

suite("test_row_ttl_direct") {
    def originalTimeZone = sql("SELECT @@time_zone")[0][0]
    try {
        sql "SET time_zone = '+08:00'"
        sql "DROP TABLE IF EXISTS row_ttl_direct"
        sql """
            CREATE TABLE row_ttl_direct (k INT, v STRING) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "function_column.enable_row_ttl"="true")
        """
        sql """
            INSERT INTO row_ttl_direct(k, v, __DORIS_TTL_COL__) VALUES
                (1, 'expired', 0), (2, 'null', NULL), (3, 'max', 9223372036854775807),
                (4, 'datetime', CAST('2099-01-01 08:00:00.123456' AS DATETIME(6))),
                (5, 'date', CAST('2099-01-02' AS DATE)),
                (6, 'absolute', CAST('2099-01-01 08:00:00.123456+08:00' AS TIMESTAMPTZ(6))),
                (7, 'negative', -1), (8, 'default', DEFAULT)
        """
        sql "INSERT INTO row_ttl_direct VALUES (9, 'omitted')"
        order_qt_visible "SELECT k, v FROM row_ttl_direct ORDER BY k"
        sql "SET show_hidden_columns = true"
        order_qt_stored_epochs "SELECT k, __DORIS_TTL_COL__ FROM row_ttl_direct ORDER BY k"
        sql "SET time_zone = '-07:00'"
        order_qt_query_zone_independent "SELECT k, __DORIS_TTL_COL__ FROM row_ttl_direct ORDER BY k"
        sql "SET show_hidden_columns = false"

        // INSERT SELECT must convert source slots, including nullable temporal columns.
        sql "DROP TABLE IF EXISTS row_ttl_direct_source"
        sql """
            CREATE TABLE row_ttl_direct_source (k INT, expires DATETIME(6)) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num"="1")
        """
        sql """INSERT INTO row_ttl_direct_source VALUES
            (10, '2099-01-01 08:00:00.123456'), (11, NULL), (12, '1969-12-31 23:59:59.999999')"""
        sql "SET time_zone = '+08:00'"
        sql """INSERT INTO row_ttl_direct(k, v, __DORIS_TTL_COL__)
            SELECT k, 'select', expires FROM row_ttl_direct_source"""
        sql "SET time_zone = 'UTC'"
        sql """INSERT INTO row_ttl_direct(k, v, __DORIS_TTL_COL__)
            SELECT k + 10, 'select-utc', expires FROM row_ttl_direct_source"""
        sql "SET show_hidden_columns = true"
        order_qt_insert_select "SELECT k, __DORIS_TTL_COL__ FROM row_ttl_direct WHERE k >= 10 ORDER BY k"
        sql "SET show_hidden_columns = false"

        // Query time is captured once, with inclusive expiration at microsecond precision.
        qt_equal_boundary """SELECT
            row_ttl_is_visible(row_ttl_expiration(now(6)), CAST(-1 AS BIGINT)),
            row_ttl_is_visible(row_ttl_expiration(now(6)) + 1, CAST(-1 AS BIGINT)),
            row_ttl_is_visible(CAST(NULL AS BIGINT), CAST(-1 AS BIGINT))"""

        streamLoad {
            table "row_ttl_direct"
            set 'column_separator', ','
            set 'columns', 'k,v,expiration,__DORIS_TTL_COL__=cast(expiration as datetime(6))'
            set 'timezone', '+08:00'
            inputStream new ByteArrayInputStream(
                '30,stream,2099-01-01 08:00:00.123456\n31,expired,2000-01-01 00:00:00\n'.getBytes())
            time 10000
        }
        streamLoad {
            table "row_ttl_direct"
            set 'column_separator', ','
            set 'columns', 'k,v,__DORIS_TTL_COL__'
            inputStream new ByteArrayInputStream('32,raw,9223372036854775807\n33,expired,0\n'.getBytes())
            time 10000
        }
        sql "SET show_hidden_columns = true"
        order_qt_stream_epochs "SELECT k, __DORIS_TTL_COL__ FROM row_ttl_direct WHERE k >= 30 ORDER BY k"
        sql "SET show_hidden_columns = false"
        sql "DELETE FROM row_ttl_direct WHERE k = 3"
        qt_delete "SELECT k FROM row_ttl_direct WHERE k = 3 ORDER BY k"
        sql "DROP TABLE IF EXISTS row_ttl_direct_like"
        sql "CREATE TABLE row_ttl_direct_like LIKE row_ttl_direct"
        sql "INSERT INTO row_ttl_direct_like(k, v, __DORIS_TTL_COL__) VALUES (1, 'expired', 0), (2, 'null', NULL)"
        order_qt_like "SELECT k, v FROM row_ttl_direct_like ORDER BY k"

        sql "DROP TABLE IF EXISTS row_ttl_direct_mow"
        sql """
            CREATE TABLE row_ttl_direct_mow (k INT, v STRING) UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (
                "replication_num"="1", "enable_unique_key_merge_on_write"="true",
                "enable_unique_key_skip_bitmap_column"="true",
                "function_column.enable_row_ttl"="true")
        """
        sql "INSERT INTO row_ttl_direct_mow(k,v,__DORIS_TTL_COL__) VALUES (1,'original',4070908800123456)"
        sql "SET enable_unique_key_partial_update = true"
        sql "INSERT INTO row_ttl_direct_mow(k,v) VALUES (1,'preserved'),(2,'new-immortal')"
        sql "SET show_hidden_columns = true"
        order_qt_partial_preserve "SELECT k,v,__DORIS_TTL_COL__ FROM row_ttl_direct_mow ORDER BY k"
        sql "SET show_hidden_columns = false"
        sql "INSERT INTO row_ttl_direct_mow(k,__DORIS_TTL_COL__) VALUES (1,NULL)"
        sql "SET show_hidden_columns = true"
        order_qt_partial_persist "SELECT k,v,__DORIS_TTL_COL__ FROM row_ttl_direct_mow ORDER BY k"
        sql "SET show_hidden_columns = false"
        sql "INSERT INTO row_ttl_direct_mow(k,__DORIS_TTL_COL__) VALUES (1,0)"
        qt_partial_expired_point "SELECT k,v FROM row_ttl_direct_mow WHERE k=1 ORDER BY k"
        sql "SET enable_unique_key_partial_update = false"
        // Replacing the whole row without TTL clears its previous expiration.
        sql "INSERT INTO row_ttl_direct_mow(k,v,__DORIS_TTL_COL__) VALUES (3,'old',4070908800123456)"
        sql "INSERT INTO row_ttl_direct_mow VALUES (3,'full-immortal')"
        sql "SET show_hidden_columns = true"
        order_qt_full_omitted "SELECT k,v,__DORIS_TTL_COL__ FROM row_ttl_direct_mow ORDER BY k"
        sql "SET show_hidden_columns = false"

        streamLoad {
            table "row_ttl_direct_mow"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'partial_columns', 'true'
            set 'columns', 'k,__DORIS_TTL_COL__'
            inputStream new ByteArrayInputStream('{"k":2,"__DORIS_TTL_COL__":4070908800123456}\n'.getBytes())
            time 10000
        }
        streamLoad {
            table "row_ttl_direct_mow"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream(
                ('{"k":2,"v":"flex-preserve"}\n'
                + '{"k":3,"__DORIS_TTL_COL__":0}\n'
                + '{"k":4,"v":"flex-new","__DORIS_TTL_COL__":4070908800123456}\n'
                + '{"k":5,"v":"flex-immortal"}\n').getBytes())
            time 10000
        }
        sql "SET show_hidden_columns = true"
        order_qt_flexible_update "SELECT k,v,__DORIS_TTL_COL__ FROM row_ttl_direct_mow ORDER BY k"
        sql "SET show_hidden_columns = false"
        streamLoad {
            table "row_ttl_direct_mow"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream('{"k":2,"__DORIS_TTL_COL__":null}\n'.getBytes())
            time 10000
        }
        sql "UPDATE row_ttl_direct_mow SET v='updated' WHERE k=4"
        sql "SET show_hidden_columns = true"
        order_qt_flexible_clear_and_update "SELECT k,v,__DORIS_TTL_COL__ FROM row_ttl_direct_mow ORDER BY k"
        sql "SET show_hidden_columns = false"
        sql "UPDATE row_ttl_direct_mow SET __DORIS_TTL_COL__=NULL WHERE k=4"
        sql "SET show_hidden_columns = true"
        order_qt_sql_clear_expiration "SELECT k,v,__DORIS_TTL_COL__ FROM row_ttl_direct_mow WHERE k=4 ORDER BY k"
        sql "SET show_hidden_columns = false"
        sql "DELETE FROM row_ttl_direct_mow WHERE k=4"
        order_qt_mow_delete "SELECT k,v FROM row_ttl_direct_mow ORDER BY k"

        sql "DROP TABLE IF EXISTS row_ttl_invalid_zone"
        test {
            sql """CREATE TABLE row_ttl_invalid_zone(k INT) DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num"="1",
                "function_column.enable_row_ttl"="true", "function_column.ttl_time_zone"="UTC")"""
            exception "requires function_column.ttl_col"
        }
        sql "DROP TABLE IF EXISTS row_ttl_invalid_duration"
        test {
            sql """CREATE TABLE row_ttl_invalid_duration(k INT) DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num"="1",
                "function_column.enable_row_ttl"="true", "function_column.ttl"="1 day")"""
            exception "function_column.ttl_col and function_column.ttl must be set together"
        }
    } finally {
        sql "SET show_hidden_columns = false"
        sql "SET enable_unique_key_partial_update = false"
        sql "SET time_zone = '${originalTimeZone}'"
    }
}
