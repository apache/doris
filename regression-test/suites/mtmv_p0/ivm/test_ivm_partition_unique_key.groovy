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

suite("test_ivm_partition_unique_key") {
    // This suite verifies that IVM MVs build a replayable logical layout while
    // still satisfying ordinary UNIQUE/MOW partition and distribution rules.
    sql """drop materialized view if exists mv_ivm_partition_key;"""
    sql """drop materialized view if exists mv_ivm_partition_auto_key;"""
    sql """drop materialized view if exists mv_ivm_partition_hash_partition_col;"""
    sql """drop materialized view if exists mv_ivm_partition_bad_key;"""
    sql """drop materialized view if exists mv_ivm_partition_bad_expr;"""
    sql """drop materialized view if exists mv_ivm_partition_bad_dist;"""
    sql """drop materialized view if exists mv_ivm_partition_agg_key_subset;"""
    sql """drop materialized view if exists mv_ivm_partition_agg_key_subset_replay;"""
    sql """drop materialized view if exists mv_ivm_partition_bad_agg_dist_key;"""
    sql """drop materialized view if exists mv_ivm_partition_bad_agg_value_key;"""
    sql """drop table if exists t_ivm_partition_key_base;"""

    sql """
        CREATE TABLE t_ivm_partition_key_base (
            id INT,
            dt DATE,
            v INT
        )
        UNIQUE KEY(id, dt)
        PARTITION BY RANGE(dt) (
            PARTITION p20260601 VALUES [('2026-06-01'), ('2026-06-02')),
            PARTITION p20260602 VALUES [('2026-06-02'), ('2026-06-03'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO t_ivm_partition_key_base VALUES
            (1, '2026-06-01', 10),
            (2, '2026-06-01', 20),
            (3, '2026-06-02', 30);
    """

    // A valid explicit-key case: the explicit partition column dt is already a
    // key column, and RANDOM distribution is rewritten to internal row-id HASH.
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_partition_key
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(id, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT id, dt, v FROM t_ivm_partition_key_base;
    """

    def descResult = sql """desc mv_ivm_partition_key all"""
    assertTrue(descResult.toString().contains("UNIQUE_KEYS"))

    sql """REFRESH MATERIALIZED VIEW mv_ivm_partition_key COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_partition_key")
    advance_ivm_stream_offset("mv_ivm_partition_key")
    order_qt_partition_key_complete """SELECT dt, id, v FROM mv_ivm_partition_key"""

    sql """INSERT INTO t_ivm_partition_key_base VALUES (2, '2026-06-01', 22);"""
    sql """REFRESH MATERIALIZED VIEW mv_ivm_partition_key INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_partition_key")
    order_qt_partition_key_incremental """SELECT dt, id, v FROM mv_ivm_partition_key"""

    // A valid system-key case: no user key/distribution is specified, so IVM
    // generates the required UNIQUE key from the partition layout plus row-id.
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_partition_auto_key
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT dt, id, v FROM t_ivm_partition_key_base;
    """

    def autoDescResult = sql """desc mv_ivm_partition_auto_key all"""
    assertTrue(autoDescResult.toString().contains("UNIQUE_KEYS"))

    sql """REFRESH MATERIALIZED VIEW mv_ivm_partition_auto_key COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_partition_auto_key")
    order_qt_auto_key_complete """SELECT dt, id, v FROM mv_ivm_partition_auto_key"""

    // Valid: no user key is specified, so explicit PARTITION BY(dt) and
    // DISTRIBUTED BY HASH(dt) both contribute to the generated key.
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_partition_hash_partition_col
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT dt, id, v FROM t_ivm_partition_key_base;
    """

    // Invalid: when the user explicitly writes KEY(id), IVM will not add the
    // partition column dt into that explicit key. The ordinary MOW partition
    // rule rejects this contradiction.
    test {
        sql """
            CREATE MATERIALIZED VIEW mv_ivm_partition_bad_key
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            KEY(id)
            PARTITION BY(dt)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT id, dt, v FROM t_ivm_partition_key_base;
        """
        exception "partition column must be KEY column"
    }

    // Invalid: IVM only supports direct column partition in this scheme.
    test {
        sql """
            CREATE MATERIALIZED VIEW mv_ivm_partition_bad_expr
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            KEY(id, dt)
            PARTITION BY(date_trunc(dt, 'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT id, dt, v FROM t_ivm_partition_key_base;
        """
        exception "only supports column partition"
    }

    // Invalid: HASH distribution columns must belong to the final key. Because
    // there is no explicit PARTITION BY here, dt is not added to the final key.
    test {
        sql """
            CREATE MATERIALIZED VIEW mv_ivm_partition_bad_dist
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            KEY(id)
            DISTRIBUTED BY HASH(dt) BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT id, dt, v FROM t_ivm_partition_key_base;
        """
        exception "Distribution column[dt] is not key column"
    }

    // Valid: aggregate IVM MOW key may use a stable group-key subset because
    // row-id is the full group-key hash.
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_partition_agg_key_subset
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT id, dt, SUM(v) AS total_v FROM t_ivm_partition_key_base GROUP BY id, dt;
    """

    def aggShowCreateResult = sql """show create materialized view mv_ivm_partition_agg_key_subset"""
    assertTrue(aggShowCreateResult.toString().contains("KEY(`id`)"))
    assertFalse(aggShowCreateResult.toString().contains("__DORIS_IVM_"))
    sql """
        ${aggShowCreateResult[0][1].toString()
            .replace("mv_ivm_partition_agg_key_subset", "mv_ivm_partition_agg_key_subset_replay")}
    """

    sql """REFRESH MATERIALIZED VIEW mv_ivm_partition_agg_key_subset COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_partition_agg_key_subset")
    advance_ivm_stream_offset("mv_ivm_partition_agg_key_subset")
    order_qt_agg_key_subset_complete """
        SELECT dt, id, total_v FROM mv_ivm_partition_agg_key_subset ORDER BY dt, id
    """

    sql """INSERT INTO t_ivm_partition_key_base VALUES (1, '2026-06-01', 15);"""
    sql """REFRESH MATERIALIZED VIEW mv_ivm_partition_agg_key_subset INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_partition_agg_key_subset")
    order_qt_agg_key_subset_incremental """
        SELECT dt, id, total_v FROM mv_ivm_partition_agg_key_subset ORDER BY dt, id
    """

    // Invalid: generated keys from HASH distribution also cannot include
    // aggregate result columns.
    test {
        sql """
            CREATE MATERIALIZED VIEW mv_ivm_partition_bad_agg_dist_key
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY HASH(total_v) BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT dt, id, SUM(v) AS total_v FROM t_ivm_partition_key_base GROUP BY dt, id;
        """
        exception "aggregate result column"
    }

    // Invalid: aggregate result values are mutable state, so they cannot be
    // part of the stable UNIQUE key used by IVM/MOW deduplication.
    test {
        sql """
            CREATE MATERIALIZED VIEW mv_ivm_partition_bad_agg_value_key
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            KEY(dt, id, total_v)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT dt, id, SUM(v) AS total_v FROM t_ivm_partition_key_base GROUP BY dt, id;
        """
        exception "aggregate result column"
    }
}
