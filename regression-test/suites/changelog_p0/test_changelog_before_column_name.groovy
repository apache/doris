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

// The changelog keeps the before-image of a value column <col> in __DORIS_CHANGELOG_BEFORE_<col>__.
// The name is under the reserved __DORIS_ prefix, so no user column can clash with it, and every
// check on it is a prefix match. These shapes broke while the before-image was __BEFORE__<col>__:
//   1. a user column named __BEFORE__v1__ next to v1: as a value column, as a key column, and once
//      v1 is dropped;
//   2. a user column whose name contains __BEFORE__ and that is the last value column, followed by
//      ADD COLUMN;
//   3. a user column ending in _COL feeding an incremental materialized view, whose before-image
//      name then ends in _COL__ like a common hidden column.
suite("test_changelog_before_column_name", "nonConcurrent") {
    // Read timestamps from FE so that the window uses the same clock and timezone
    // as SQL parsing. Leave a whole second after each group of committed writes.
    def nextBoundary = {
        sleep(1200)
        sql("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')")[0][0].toString()
    }

    // A user column can not take the before-image name itself.
    sql "DROP TABLE IF EXISTS test_changelog_before_name_reserved FORCE"
    test {
        sql """
            CREATE TABLE test_changelog_before_name_reserved (
                k INT,
                v1 INT,
                `__DORIS_CHANGELOG_BEFORE_v1__` INT
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "changelog.enable" = "true",
                "changelog.need_historical_value" = "true"
            )
        """
        exception "Disable to create table column with name start with __DORIS_"
    }

    // ============================================================
    // 1. A user value column named __BEFORE__v1__ next to v1.
    // ============================================================
    sql "DROP TABLE IF EXISTS test_changelog_before_name_value FORCE"
    sql """
        CREATE TABLE test_changelog_before_name_value (
            k INT,
            v1 INT,
            `__BEFORE__v1__` INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO test_changelog_before_name_value VALUES (1, 10, 100), (2, 20, 200)"
    def valueStart = nextBoundary()
    sql "INSERT INTO test_changelog_before_name_value VALUES (1, 11, 101)"
    sql "UPDATE test_changelog_before_name_value SET v1 = 22, `__BEFORE__v1__` = 202 WHERE k = 2"
    sql "DELETE FROM test_changelog_before_name_value WHERE k = 1"
    sql "INSERT INTO test_changelog_before_name_value VALUES (2, 23, 203)"
    sql "sync"

    // UPDATE_BEFORE rows carry the old value of both v1 and __BEFORE__v1__.
    order_qt_value_detail """
        SELECT __DORIS_CHANGELOG_OP__, k, v1, `__BEFORE__v1__`
        FROM test_changelog_before_name_value@incr("incrementType" = "DETAIL")
    """
    order_qt_value_min_delta """
        SELECT __DORIS_CHANGELOG_OP__, k, v1, `__BEFORE__v1__`
        FROM test_changelog_before_name_value@incr('startTimestamp' = '${valueStart}',
            'incrementType' = 'MIN_DELTA')
    """
    // SELECT * keeps the user column __BEFORE__v1__.
    order_qt_value_select_star """
        SELECT *
        FROM test_changelog_before_name_value@incr('startTimestamp' = '${valueStart}',
            'incrementType' = 'MIN_DELTA')
    """
    // The user column and both before-image columns can be named in the changelog TVF.
    order_qt_value_tvf """
        SELECT __DORIS_CHANGELOG_OP__, k, v1, `__BEFORE__v1__`,
               __DORIS_CHANGELOG_BEFORE_v1__, `__DORIS_CHANGELOG_BEFORE___BEFORE__v1____`
        FROM changelog("table" = "test_changelog_before_name_value")
    """

    // 1.1 The same name as a key column.
    sql "DROP TABLE IF EXISTS test_changelog_before_name_key FORCE"
    sql """
        CREATE TABLE test_changelog_before_name_key (
            k INT,
            `__BEFORE__v1__` INT,
            v1 INT
        )
        UNIQUE KEY(k, `__BEFORE__v1__`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO test_changelog_before_name_key VALUES (1, 100, 10), (2, 200, 20)"
    sql "INSERT INTO test_changelog_before_name_key VALUES (1, 100, 11)"
    sql "INSERT INTO test_changelog_before_name_key VALUES (2, 200, 21)"
    sql "sync"

    order_qt_key_detail """
        SELECT __DORIS_CHANGELOG_OP__, k, `__BEFORE__v1__`, v1
        FROM test_changelog_before_name_key@incr("incrementType" = "DETAIL")
    """
    order_qt_key_tvf """
        SELECT __DORIS_CHANGELOG_OP__, k, `__BEFORE__v1__`, v1, __DORIS_CHANGELOG_BEFORE_v1__
        FROM changelog("table" = "test_changelog_before_name_key")
    """

    // 1.2 Dropping v1 keeps the user column __BEFORE__v1__ writable.
    sql "DROP TABLE IF EXISTS test_changelog_before_name_drop FORCE"
    sql """
        CREATE TABLE test_changelog_before_name_drop (
            k INT,
            v1 INT,
            `__BEFORE__v1__` INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO test_changelog_before_name_drop VALUES (1, 10, 100)"
    sql "ALTER TABLE test_changelog_before_name_drop DROP COLUMN v1"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName = 'test_changelog_before_name_drop'
               ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    sql "INSERT INTO test_changelog_before_name_drop VALUES (2, 200)"
    sql "INSERT INTO test_changelog_before_name_drop VALUES (1, 101)"
    sql "sync"

    order_qt_drop_base "SELECT k, `__BEFORE__v1__` FROM test_changelog_before_name_drop"
    order_qt_drop_detail """
        SELECT __DORIS_CHANGELOG_OP__, k, `__BEFORE__v1__`
        FROM test_changelog_before_name_drop@incr("incrementType" = "DETAIL")
    """
    order_qt_drop_tvf """
        SELECT __DORIS_CHANGELOG_OP__, k, `__BEFORE__v1__`, `__DORIS_CHANGELOG_BEFORE___BEFORE__v1____`
        FROM changelog("table" = "test_changelog_before_name_drop")
    """

    // ============================================================
    // 2. A user column x__BEFORE__y that is the last value column, then ADD COLUMN v2. Every row
    //    written after the ALTER must keep x__BEFORE__y and v2 in their own columns.
    // ============================================================
    sql "DROP TABLE IF EXISTS test_changelog_before_name_last FORCE"
    sql """
        CREATE TABLE test_changelog_before_name_last (
            k INT,
            v1 INT,
            `x__BEFORE__y` INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO test_changelog_before_name_last VALUES (1, 10, 1000), (2, 20, 2000)"
    sql "ALTER TABLE test_changelog_before_name_last ADD COLUMN v2 INT"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName = 'test_changelog_before_name_last'
               ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    def lastStart = nextBoundary()
    sql "INSERT INTO test_changelog_before_name_last VALUES (3, 30, 3000, 7)"
    sql "INSERT INTO test_changelog_before_name_last VALUES (1, 11, 1001, 8)"
    sql "UPDATE test_changelog_before_name_last SET v2 = 9 WHERE k = 2"
    sql "DELETE FROM test_changelog_before_name_last WHERE k = 3"
    sql "sync"

    order_qt_last_base "SELECT k, v1, `x__BEFORE__y`, v2 FROM test_changelog_before_name_last"
    order_qt_last_tvf """
        SELECT __DORIS_CHANGELOG_OP__, k, v1, `x__BEFORE__y`, v2,
               __DORIS_CHANGELOG_BEFORE_v1__, `__DORIS_CHANGELOG_BEFORE_x__BEFORE__y__`,
               __DORIS_CHANGELOG_BEFORE_v2__
        FROM changelog("table" = "test_changelog_before_name_last")
    """
    order_qt_last_detail """
        SELECT __DORIS_CHANGELOG_OP__, k, v1, `x__BEFORE__y`, v2
        FROM test_changelog_before_name_last@incr("incrementType" = "DETAIL")
    """
    order_qt_last_min_delta """
        SELECT __DORIS_CHANGELOG_OP__, k, v1, `x__BEFORE__y`, v2
        FROM test_changelog_before_name_last@incr('startTimestamp' = '${lastStart}',
            'incrementType' = 'MIN_DELTA')
    """

    // 2.1 The same with a VARCHAR x__BEFORE__y, which used to fail every later load with a bad cast.
    sql "DROP TABLE IF EXISTS test_changelog_before_name_last_str FORCE"
    sql """
        CREATE TABLE test_changelog_before_name_last_str (
            k INT,
            v1 INT,
            `x__BEFORE__y` VARCHAR(20)
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO test_changelog_before_name_last_str VALUES (1, 10, 'x1000'), (2, 20, 'x2000')"
    sql "ALTER TABLE test_changelog_before_name_last_str ADD COLUMN v2 INT"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName = 'test_changelog_before_name_last_str'
               ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    def lastStrStart = nextBoundary()
    sql "INSERT INTO test_changelog_before_name_last_str VALUES (3, 30, 'x3000', 7)"
    sql "INSERT INTO test_changelog_before_name_last_str VALUES (1, 11, 'x1001', 8)"
    sql "UPDATE test_changelog_before_name_last_str SET v2 = 9 WHERE k = 2"
    sql "DELETE FROM test_changelog_before_name_last_str WHERE k = 3"
    sql "sync"

    order_qt_last_str_base "SELECT k, v1, `x__BEFORE__y`, v2 FROM test_changelog_before_name_last_str"
    order_qt_last_str_tvf """
        SELECT __DORIS_CHANGELOG_OP__, k, v1, `x__BEFORE__y`, v2,
               __DORIS_CHANGELOG_BEFORE_v1__, `__DORIS_CHANGELOG_BEFORE_x__BEFORE__y__`,
               __DORIS_CHANGELOG_BEFORE_v2__
        FROM changelog("table" = "test_changelog_before_name_last_str")
    """
    order_qt_last_str_detail """
        SELECT __DORIS_CHANGELOG_OP__, k, v1, `x__BEFORE__y`, v2
        FROM test_changelog_before_name_last_str@incr("incrementType" = "DETAIL")
    """
    order_qt_last_str_min_delta """
        SELECT __DORIS_CHANGELOG_OP__, k, v1, `x__BEFORE__y`, v2
        FROM test_changelog_before_name_last_str@incr('startTimestamp' = '${lastStrStart}',
            'incrementType' = 'MIN_DELTA')
    """

    // ============================================================
    // 3. A user column v_COL feeding an incremental materialized view. The MV keeps a changelog
    //    with historical values too, so both the base table and the MV carry
    //    __DORIS_CHANGELOG_BEFORE_v_COL__, which is not a common hidden column.
    // ============================================================
    sql "DROP MATERIALIZED VIEW IF EXISTS test_changelog_before_name_col_mv"
    sql "DROP TABLE IF EXISTS test_changelog_before_name_col_base FORCE"
    sql """
        CREATE TABLE test_changelog_before_name_col_base (
            k INT,
            v_COL INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO test_changelog_before_name_col_base VALUES (1, 10), (2, 20)"
    sql """
        CREATE MATERIALIZED VIEW test_changelog_before_name_col_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
        AS SELECT k, v_COL FROM test_changelog_before_name_col_base
    """
    sql "REFRESH MATERIALIZED VIEW test_changelog_before_name_col_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("test_changelog_before_name_col_mv")

    sql "INSERT INTO test_changelog_before_name_col_base VALUES (1, 11)"
    sql "DELETE FROM test_changelog_before_name_col_base WHERE k = 2"
    sql "INSERT INTO test_changelog_before_name_col_base VALUES (3, 30)"
    sql "REFRESH MATERIALIZED VIEW test_changelog_before_name_col_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_changelog_before_name_col_mv")

    // A refresh that only ran the incremental rewrite leaves RefreshMode unset, and an unset column
    // comes back as the literal two-character string "\N", so fold it into a printable token.
    order_qt_col_refresh_mode """
        SELECT CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                    THEN RefreshMode ELSE 'NONE' END
        FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'test_changelog_before_name_col_mv'
        ORDER BY CreateTime DESC LIMIT 1
    """
    order_qt_col_mv "SELECT k, v_COL FROM test_changelog_before_name_col_mv"
    order_qt_col_mv_changelog """
        SELECT __DORIS_CHANGELOG_OP__, k, v_COL, __DORIS_CHANGELOG_BEFORE_v_COL__
        FROM changelog("table" = "test_changelog_before_name_col_mv")
    """
}
