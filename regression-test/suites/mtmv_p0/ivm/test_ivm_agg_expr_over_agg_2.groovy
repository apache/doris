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

suite("test_ivm_agg_expr_over_agg_2") {

    // =========================================================
    // Refresh shapes around the wrapped-aggregate state carrier
    // that need their own setup: the same shape under
    // ivm_use_full_keys, two aggregate values consumed by one
    // expression, an aggregate whose result is clamped into the
    // MV key column, and a view that aliases its own expression
    // to the aggregate's generated column name.
    // =========================================================

    // =========================================================
    // Part 5: the same wrapped shape under ivm_use_full_keys,
    // where normalize also materializes hidden identity-key
    // columns into the same projection as the state carrier.
    // Created after the windows above so its inserts stay a
    // clean incremental window.
    // =========================================================

    def refreshIncremental = { mv ->
        sql """REFRESH MATERIALIZED VIEW ${mv} INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName(mv)
    }

    sql """drop materialized view if exists test_ivm_expr_over_agg_full_keys;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_alias_collision;"""
    sql """drop table if exists test_ivm_expr_over_agg_alias_base;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_cast_collision;"""
    sql """drop table if exists test_ivm_expr_over_agg_cast_base;"""
    sql """drop table if exists test_ivm_expr_over_agg_2_base;"""

    sql """
        CREATE TABLE test_ivm_expr_over_agg_2_base (
            id INT,
            k INT,
            v INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO test_ivm_expr_over_agg_2_base VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30);"""

    sql """CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_full_keys
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1', 'ivm_use_full_keys' = 'true')
        AS SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_2_base GROUP BY k;"""
    sql """set show_hidden_columns=true"""
    qt_full_keys_desc """DESC test_ivm_expr_over_agg_full_keys"""
    sql """set show_hidden_columns=false"""

    refreshIncremental("test_ivm_expr_over_agg_full_keys")
    order_qt_full_keys_initial """SELECT k, s100 FROM test_ivm_expr_over_agg_full_keys"""
    order_qt_full_keys_initial_source """
        SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_2_base GROUP BY k"""

    sql """INSERT INTO test_ivm_expr_over_agg_2_base VALUES (11, 1, 60);"""
    refreshIncremental("test_ivm_expr_over_agg_full_keys")
    order_qt_full_keys_after_insert """SELECT k, s100 FROM test_ivm_expr_over_agg_full_keys"""
    order_qt_full_keys_after_insert_source """
        SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_2_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_full_keys COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_full_keys")
    order_qt_full_keys_complete """SELECT k, s100 FROM test_ivm_expr_over_agg_full_keys"""
    order_qt_full_keys_complete_source """
        SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_2_base GROUP BY k"""

    // =========================================================
    // Part 6: two aggregate values consumed by one expression.
    // SUM(x) * SUM(y) needs one carrier per SUM state, while
    // AVG(x) * AVG(y) needs none: AVG derives its visible value
    // from hidden SUM/COUNT states, which are always persisted.
    // =========================================================

    sql """drop materialized view if exists test_ivm_expr_over_agg_sumx_sumy;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_avgx_avgy;"""
    sql """drop table if exists test_ivm_expr_over_agg_pair_base;"""

    sql """
        CREATE TABLE test_ivm_expr_over_agg_pair_base (
            id INT,
            k INT,
            x INT,
            y INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO test_ivm_expr_over_agg_pair_base VALUES
        (1, 1, 10, 3), (2, 1, 20, 5), (3, 2, 30, 7), (4, 2, 40, NULL), (5, 3, NULL, 9);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_sumx_sumy
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, SUM(x) * SUM(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_sumx_sumy_desc """DESC test_ivm_expr_over_agg_sumx_sumy"""
    sql """set show_hidden_columns=false"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_avgx_avgy
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, AVG(x) * AVG(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_avgx_avgy_desc """DESC test_ivm_expr_over_agg_avgx_avgy"""
    sql """set show_hidden_columns=false"""

    refreshIncremental("test_ivm_expr_over_agg_sumx_sumy")
    refreshIncremental("test_ivm_expr_over_agg_avgx_avgy")
    order_qt_pair_initial_sumx_sumy """SELECT k, p FROM test_ivm_expr_over_agg_sumx_sumy"""
    order_qt_pair_initial_sumx_sumy_source """
        SELECT k, SUM(x) * SUM(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""
    order_qt_pair_initial_avgx_avgy """SELECT k, p FROM test_ivm_expr_over_agg_avgx_avgy"""
    order_qt_pair_initial_avgx_avgy_source """
        SELECT k, AVG(x) * AVG(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""

    // Update id=2 into (25, 6): both aggregate inputs of group k=1 change in one window.
    sql """INSERT INTO test_ivm_expr_over_agg_pair_base VALUES (2, 1, 25, 6);"""
    refreshIncremental("test_ivm_expr_over_agg_sumx_sumy")
    refreshIncremental("test_ivm_expr_over_agg_avgx_avgy")
    order_qt_pair_after_update_sumx_sumy """SELECT k, p FROM test_ivm_expr_over_agg_sumx_sumy"""
    order_qt_pair_after_update_sumx_sumy_source """
        SELECT k, SUM(x) * SUM(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""
    order_qt_pair_after_update_avgx_avgy """SELECT k, p FROM test_ivm_expr_over_agg_avgx_avgy"""
    order_qt_pair_after_update_avgx_avgy_source """
        SELECT k, AVG(x) * AVG(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""

    // Delete the row whose y is NULL (id=4) together with a dirty insert into k=2.
    sql """DELETE FROM test_ivm_expr_over_agg_pair_base WHERE id = 4;"""
    sql """INSERT INTO test_ivm_expr_over_agg_pair_base VALUES (6, 2, 50, 11);"""
    refreshIncremental("test_ivm_expr_over_agg_sumx_sumy")
    refreshIncremental("test_ivm_expr_over_agg_avgx_avgy")
    order_qt_pair_after_delete_sumx_sumy """SELECT k, p FROM test_ivm_expr_over_agg_sumx_sumy"""
    order_qt_pair_after_delete_sumx_sumy_source """
        SELECT k, SUM(x) * SUM(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""
    order_qt_pair_after_delete_avgx_avgy """SELECT k, p FROM test_ivm_expr_over_agg_avgx_avgy"""
    order_qt_pair_after_delete_avgx_avgy_source """
        SELECT k, AVG(x) * AVG(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""

    // Group k=3 held a single row with x NULL; give it a second row with both inputs set.
    sql """INSERT INTO test_ivm_expr_over_agg_pair_base VALUES (7, 3, 30, 4);"""
    refreshIncremental("test_ivm_expr_over_agg_sumx_sumy")
    refreshIncremental("test_ivm_expr_over_agg_avgx_avgy")
    order_qt_pair_after_resurrect_sumx_sumy """SELECT k, p FROM test_ivm_expr_over_agg_sumx_sumy"""
    order_qt_pair_after_resurrect_sumx_sumy_source """
        SELECT k, SUM(x) * SUM(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""
    order_qt_pair_after_resurrect_avgx_avgy """SELECT k, p FROM test_ivm_expr_over_agg_avgx_avgy"""
    order_qt_pair_after_resurrect_avgx_avgy_source """
        SELECT k, AVG(x) * AVG(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_sumx_sumy COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_sumx_sumy")
    order_qt_pair_complete_sumx_sumy """SELECT k, p FROM test_ivm_expr_over_agg_sumx_sumy"""
    order_qt_pair_complete_sumx_sumy_source """
        SELECT k, SUM(x) * SUM(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_avgx_avgy COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_avgx_avgy")
    order_qt_pair_complete_avgx_avgy """SELECT k, p FROM test_ivm_expr_over_agg_avgx_avgy"""
    order_qt_pair_complete_avgx_avgy_source """
        SELECT k, AVG(x) * AVG(y) AS p FROM test_ivm_expr_over_agg_pair_base GROUP BY k"""

    // =========================================================
    // Part 8: an aggregate whose result is the MV's key column
    // and is therefore clamped to VARCHAR(65533). The refresh
    // plan wraps the aggregate output in binder projects that
    // rename it and coerce it back into the MV column, so the
    // state column must keep its own name instead of acquiring a
    // new carrier that the MV does not have.
    // =========================================================

    sql """drop materialized view if exists test_ivm_expr_over_agg_clamped_key;"""
    sql """drop table if exists test_ivm_expr_over_agg_str_base;"""

    sql """
        CREATE TABLE test_ivm_expr_over_agg_str_base (
            id INT,
            s STRING
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO test_ivm_expr_over_agg_str_base VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc');"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_clamped_key
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT MIN(s) AS m FROM test_ivm_expr_over_agg_str_base;
    """
    sql """set show_hidden_columns=true"""
    qt_clamped_key_desc """DESC test_ivm_expr_over_agg_clamped_key"""
    sql """set show_hidden_columns=false"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_clamped_key COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_clamped_key")
    order_qt_clamped_key """SELECT m FROM test_ivm_expr_over_agg_clamped_key"""
    order_qt_clamped_key_source """SELECT MIN(s) AS m FROM test_ivm_expr_over_agg_str_base"""

    // Delete the current minimum so the complete refresh has to rebuild the state.
    sql """DELETE FROM test_ivm_expr_over_agg_str_base WHERE id = 1;"""
    sql """INSERT INTO test_ivm_expr_over_agg_str_base VALUES (4, 'aaaa');"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_clamped_key COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_clamped_key")
    order_qt_clamped_key_after_delete """SELECT m FROM test_ivm_expr_over_agg_clamped_key"""
    order_qt_clamped_key_after_delete_source """SELECT MIN(s) AS m FROM test_ivm_expr_over_agg_str_base"""

    // =========================================================
    // Part 9: a view that aliases its own expression to the
    // aggregate's generated column name. The MV column with that
    // name holds a derived value, so the raw state still needs a
    // materialized carrier; a same-named output is not enough.
    // =========================================================

    sql """drop materialized view if exists test_ivm_expr_over_agg_alias_collision;"""
    sql """drop table if exists test_ivm_expr_over_agg_alias_base;"""

    sql """
        CREATE TABLE test_ivm_expr_over_agg_alias_base (
            id INT,
            k INT,
            v INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO test_ivm_expr_over_agg_alias_base VALUES (1, 1, 2);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_alias_collision
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, SUM(v) * 100 AS `sum(v)` FROM test_ivm_expr_over_agg_alias_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_alias_collision_desc """DESC test_ivm_expr_over_agg_alias_collision"""
    sql """set show_hidden_columns=false"""

    refreshIncremental("test_ivm_expr_over_agg_alias_collision")
    order_qt_alias_collision_initial """SELECT k, `sum(v)` FROM test_ivm_expr_over_agg_alias_collision"""
    order_qt_alias_collision_initial_source """
        SELECT k, SUM(v) * 100 AS `sum(v)` FROM test_ivm_expr_over_agg_alias_base GROUP BY k"""

    // The alias holds 200 for SUM(v)=2; merging through it as if it were the raw state would persist 20300.
    sql """INSERT INTO test_ivm_expr_over_agg_alias_base VALUES (2, 1, 3);"""
    refreshIncremental("test_ivm_expr_over_agg_alias_collision")
    order_qt_alias_collision_after_insert """SELECT k, `sum(v)` FROM test_ivm_expr_over_agg_alias_collision"""
    order_qt_alias_collision_after_insert_source """
        SELECT k, SUM(v) * 100 AS `sum(v)` FROM test_ivm_expr_over_agg_alias_base GROUP BY k"""

    sql """DELETE FROM test_ivm_expr_over_agg_alias_base WHERE id = 1;"""
    sql """INSERT INTO test_ivm_expr_over_agg_alias_base VALUES (3, 1, 7);"""
    refreshIncremental("test_ivm_expr_over_agg_alias_collision")
    order_qt_alias_collision_after_delete """SELECT k, `sum(v)` FROM test_ivm_expr_over_agg_alias_collision"""
    order_qt_alias_collision_after_delete_source """
        SELECT k, SUM(v) * 100 AS `sum(v)` FROM test_ivm_expr_over_agg_alias_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_alias_collision COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_alias_collision")
    order_qt_alias_collision_complete """SELECT k, `sum(v)` FROM test_ivm_expr_over_agg_alias_collision"""
    order_qt_alias_collision_complete_source """
        SELECT k, SUM(v) * 100 AS `sum(v)` FROM test_ivm_expr_over_agg_alias_base GROUP BY k"""

    // =========================================================
    // Part 10: a lossy CAST whose output takes the aggregate's
    // generated column name. The cast is value-preserving for the
    // type checker but not for the value, so the column can never
    // stand in for the raw state.
    // =========================================================

    sql """drop materialized view if exists test_ivm_expr_over_agg_cast_collision;"""
    sql """drop table if exists test_ivm_expr_over_agg_cast_base;"""

    sql """
        CREATE TABLE test_ivm_expr_over_agg_cast_base (
            id INT,
            k INT,
            d DECIMAL(10, 2)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO test_ivm_expr_over_agg_cast_base VALUES (1, 1, 1.55);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_cast_collision
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, CAST(SUM(d) AS DECIMAL(20, 0)) AS `sum(d)` FROM test_ivm_expr_over_agg_cast_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_cast_collision_desc """DESC test_ivm_expr_over_agg_cast_collision"""
    sql """set show_hidden_columns=false"""

    refreshIncremental("test_ivm_expr_over_agg_cast_collision")
    order_qt_cast_collision_initial """SELECT k, `sum(d)` FROM test_ivm_expr_over_agg_cast_collision"""
    order_qt_cast_collision_initial_source """
        SELECT k, CAST(SUM(d) AS DECIMAL(20, 0)) AS `sum(d)` FROM test_ivm_expr_over_agg_cast_base GROUP BY k"""

    // Merging from the rounded alias (2) instead of the raw state (1.55) persisted 157 here.
    sql """INSERT INTO test_ivm_expr_over_agg_cast_base VALUES (2, 1, 1.55);"""
    refreshIncremental("test_ivm_expr_over_agg_cast_collision")
    order_qt_cast_collision_after_insert """SELECT k, `sum(d)` FROM test_ivm_expr_over_agg_cast_collision"""
    order_qt_cast_collision_after_insert_source """
        SELECT k, CAST(SUM(d) AS DECIMAL(20, 0)) AS `sum(d)` FROM test_ivm_expr_over_agg_cast_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_cast_collision COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_cast_collision")
    order_qt_cast_collision_complete """SELECT k, `sum(d)` FROM test_ivm_expr_over_agg_cast_collision"""
    order_qt_cast_collision_complete_source """
        SELECT k, CAST(SUM(d) AS DECIMAL(20, 0)) AS `sum(d)` FROM test_ivm_expr_over_agg_cast_base GROUP BY k"""
}
