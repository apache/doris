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

suite("max_visible_partition") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // ============================================================
    // Case 1: RANGE partition (BIGINT) - prunes to the greatest upper bound with data
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_range;"""
    sql """
        CREATE TABLE mvp_range (
            id BIGINT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ('5'),
            PARTITION `p2` VALUES LESS THAN ('10'),
            PARTITION `p3` VALUES LESS THAN ('15')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_range VALUES(1, 10)"
    sql "INSERT INTO mvp_range VALUES(7, 20)"
    // p3 has no data -> latest visible is p2
    qt_sql_range """select * from mvp_range max_visible_partition();"""
    explain {
        sql "select * from mvp_range max_visible_partition();"
        contains "partitions=1/3 (p2)"
    }

    // ============================================================
    // Case 1b: RANGE partition (DATE) - most common business case
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_range_date;"""
    sql """
        CREATE TABLE mvp_range_date (
            dt DATE,
            val BIGINT
        ) DUPLICATE KEY(`dt`)
        PARTITION BY RANGE(`dt`)
        (
            PARTITION p20260101 VALUES [('2026-01-01'), ('2026-01-02')),
            PARTITION p20260102 VALUES [('2026-01-02'), ('2026-01-03')),
            PARTITION p20260103 VALUES [('2026-01-03'), ('2026-01-04'))
        )
        DISTRIBUTED BY HASH(`dt`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_range_date VALUES('2026-01-01', 100)"
    sql "INSERT INTO mvp_range_date VALUES('2026-01-02', 200)"
    // p20260103 has no data -> latest visible is p20260102
    qt_sql_range_date """select * from mvp_range_date max_visible_partition();"""
    explain {
        sql "select * from mvp_range_date max_visible_partition();"
        contains "partitions=1/3 (p20260102)"
    }

    // ============================================================
    // Case 1c: multi-column RANGE partition (DATETIME + INT)
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_range_multi;"""
    sql """
        CREATE TABLE mvp_range_multi (
            dt DATETIME,
            region INT,
            val BIGINT
        ) DUPLICATE KEY(`dt`, `region`)
        PARTITION BY RANGE(`dt`, `region`)
        (
            PARTITION p_low  VALUES [('2026-01-01 00:00:00', 1),  ('2026-01-02 00:00:00', 10)),
            PARTITION p_mid  VALUES [('2026-01-02 00:00:00', 10), ('2026-01-03 00:00:00', 20)),
            PARTITION p_high VALUES [('2026-01-03 00:00:00', 20), ('2026-01-04 00:00:00', 30))
        )
        DISTRIBUTED BY HASH(`region`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_range_multi VALUES('2026-01-01 12:00:00', 5, 100)"
    sql "INSERT INTO mvp_range_multi VALUES('2026-01-02 12:00:00', 15, 200)"
    // p_high has no data -> latest visible is p_mid
    qt_sql_range_multi """select * from mvp_range_multi max_visible_partition();"""
    explain {
        sql "select * from mvp_range_multi max_visible_partition();"
        contains "partitions=1/3 (p_mid)"
    }

    // ============================================================
    // Case 2: Async materialized view (MTMV)
    // ============================================================
    sql """DROP MATERIALIZED VIEW IF EXISTS mvp_mtmv;"""
    sql """
        CREATE MATERIALIZED VIEW mvp_mtmv
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        PARTITION BY (id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, val FROM mvp_range;
    """
    waitingMTMVTaskFinishedByMvName("mvp_mtmv")
    qt_sql_mtmv """select * from mvp_mtmv max_visible_partition();"""
    explain {
        sql "select * from mvp_mtmv max_visible_partition();"
        contains "partitions=1/"
    }

    // ============================================================
    // Case 3: View is not OLAP-backed -> reject explicitly
    // ============================================================
    sql """DROP VIEW IF EXISTS v_mvp;"""
    sql """CREATE VIEW v_mvp AS SELECT * FROM mvp_range;"""
    test {
        sql "select * from v_mvp max_visible_partition();"
        exception "max_visible_partition() is only supported on OLAP table or materialized view"
    }

    // ============================================================
    // Case 4: FOR VERSION / TIME AS OF conflict -> reject
    // ============================================================
    test {
        sql "SELECT * FROM mvp_range FOR VERSION AS OF 100 max_visible_partition();"
        exception "max_visible_partition() cannot be used with FOR VERSION/TIME AS OF"
    }
    test {
        sql "SELECT * FROM mvp_range FOR TIME AS OF '2026-01-01 00:00:00' max_visible_partition();"
        exception "max_visible_partition() cannot be used with FOR VERSION/TIME AS OF"
    }

    // ============================================================
    // Case 5a: UNPARTITIONED with data -> full data
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_unpart;"""
    sql """
        CREATE TABLE mvp_unpart (
            id BIGINT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_unpart VALUES(1, 10), (2, 20)"
    order_qt_sql_unpart """select * from mvp_unpart max_visible_partition();"""

    // ============================================================
    // Case 5b: UNPARTITIONED with no data -> result empty
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_unpart_empty;"""
    sql """
        CREATE TABLE mvp_unpart_empty (
            id BIGINT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    qt_sql_unpart_empty """select * from mvp_unpart_empty max_visible_partition();"""

    // ============================================================
    // Case 6a: LIST partition (both have data) - pick partition of max key
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_list;"""
    sql """
        CREATE TABLE mvp_list (
            id INT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        PARTITION BY LIST(`id`)
        (
            PARTITION p1 VALUES IN ("1","2","3"),
            PARTITION p2 VALUES IN ("4","5","6")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_list VALUES(1, 10), (5, 50)"
    qt_sql_list """select * from mvp_list max_visible_partition();"""
    explain {
        sql "select * from mvp_list max_visible_partition();"
        contains "partitions=1/2 (p2)"
    }

    // ============================================================
    // Case 6b: LIST - only low partition has data -> pick that one
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_list_low;"""
    sql """
        CREATE TABLE mvp_list_low (
            id INT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        PARTITION BY LIST(`id`)
        (
            PARTITION p1 VALUES IN ("1","2","3"),
            PARTITION p2 VALUES IN ("4","5","6")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_list_low VALUES(2, 20)"
    qt_sql_list_low """select * from mvp_list_low max_visible_partition();"""
    explain {
        sql "select * from mvp_list_low max_visible_partition();"
        contains "partitions=1/2 (p1)"
    }

    // ============================================================
    // Case 6c: LIST - all empty -> result empty
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_list_empty;"""
    sql """
        CREATE TABLE mvp_list_empty (
            id INT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        PARTITION BY LIST(`id`)
        (
            PARTITION p1 VALUES IN ("1","2","3")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    qt_sql_list_empty """select * from mvp_list_empty max_visible_partition();"""

    // ============================================================
    // Case 6d: multi-column LIST partition
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_list_multi;"""
    sql """
        CREATE TABLE mvp_list_multi (
            region VARCHAR(16),
            id INT,
            val BIGINT
        ) DUPLICATE KEY(`region`, `id`)
        PARTITION BY LIST(`region`, `id`)
        (
            PARTITION p_cn_low  VALUES IN (("cn", "1"), ("cn", "2")),
            PARTITION p_cn_high VALUES IN (("cn", "8"), ("cn", "9")),
            PARTITION p_us_low  VALUES IN (("us", "1"), ("us", "2"))
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_list_multi VALUES('cn', 1, 100), ('cn', 8, 200), ('us', 1, 300)"
    // max key ('us','1') -> partition p_us_low
    qt_sql_list_multi """select * from mvp_list_multi max_visible_partition();"""
    explain {
        sql "select * from mvp_list_multi max_visible_partition();"
        contains "partitions=1/3 (p_us_low)"
    }

    // ============================================================
    // Case 6e: LIST with a DEFAULT partition alongside a keyed partition.
    // The default partition is only a last-resort fallback; when a keyed partition
    // has data it always wins, so the default (even with larger values) is not chosen.
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_list_def;"""
    sql """
        CREATE TABLE mvp_list_def (
            id INT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        PARTITION BY LIST(`id`)
        (
            PARTITION p_ex VALUES IN ("1","2","3"),
            PARTITION p_def
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    // 1 -> p_ex, 100 -> p_def(default); keyed p_ex wins over the default fallback
    sql "INSERT INTO mvp_list_def VALUES(1, 10), (100, 999)"
    qt_sql_list_def """select * from mvp_list_def max_visible_partition();"""
    explain {
        sql "select * from mvp_list_def max_visible_partition();"
        contains "partitions=1/2 (p_ex)"
    }

    // ============================================================
    // Case 6f: LIST with DEFAULT partition but only the keyed partition has data.
    // The keyed partition is chosen; the empty default fallback is not needed.
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_list_def_empty;"""
    sql """
        CREATE TABLE mvp_list_def_empty (
            id INT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        PARTITION BY LIST(`id`)
        (
            PARTITION p_ex VALUES IN ("1","2","3"),
            PARTITION p_def
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_list_def_empty VALUES(2, 20)"
    qt_sql_list_def_empty """select * from mvp_list_def_empty max_visible_partition();"""
    explain {
        sql "select * from mvp_list_def_empty max_visible_partition();"
        contains "partitions=1/2 (p_ex)"
    }

    // ============================================================
    // Case 6g: LIST with only a DEFAULT partition holding data -> fall back to default.
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_list_def_only;"""
    sql """
        CREATE TABLE mvp_list_def_only (
            id INT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        PARTITION BY LIST(`id`)
        (
            PARTITION p_def
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_list_def_only VALUES(7, 70)"
    qt_sql_list_def_only """select * from mvp_list_def_only max_visible_partition();"""
    explain {
        sql "select * from mvp_list_def_only max_visible_partition();"
        contains "partitions=1/1 (p_def)"
    }

    // ============================================================
    // Case 7: Short-circuit point query MUST be excluded.
    // Caching a resolved partition into a prepared point-query plan
    // would return stale data as new partitions become visible.
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_sc;"""
    sql """
        CREATE TABLE mvp_sc (
            id BIGINT,
            val BIGINT
        ) UNIQUE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ('5'),
            PARTITION `p2` VALUES LESS THAN ('10')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true"
        );
    """
    sql "INSERT INTO mvp_sc VALUES(1, 10)"
    sql "INSERT INTO mvp_sc VALUES(7, 70)"
    sql "SET enable_short_circuit_query=true"

    // baseline: plain full-key point query short-circuits
    explain {
        sql "select * from mvp_sc where id = 7"
        contains "SHORT-CIRCUIT"
    }
    // with max_visible_partition(): must NOT short-circuit
    explain {
        sql "select * from mvp_sc max_visible_partition() where id = 7"
        notContains "SHORT-CIRCUIT"
    }
    // still resolves to latest visible partition (p2)
    explain {
        sql "select * from mvp_sc max_visible_partition() where id = 7"
        contains "partitions=1/2 (p2)"
    }
    qt_sql_sc_mvp """select * from mvp_sc max_visible_partition() where id = 7;"""

    sql "SET enable_short_circuit_query=false"

    // ============================================================
    // Case 8a: TRUNCATE partition -> version reset -> partition invisible
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_td;"""
    sql """
        CREATE TABLE mvp_td (
            id BIGINT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ('5'),
            PARTITION `p2` VALUES LESS THAN ('10')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_td VALUES(1, 10)"
    sql "INSERT INTO mvp_td VALUES(7, 70)"
    // both partitions have data -> latest visible is p2
    explain {
        sql "select * from mvp_td max_visible_partition();"
        contains "partitions=1/2 (p2)"
    }
    sql "truncate table mvp_td partitions (p2);"
    order_qt_sql_td_truncate """select * from mvp_td max_visible_partition();"""
    explain {
        sql "select * from mvp_td max_visible_partition();"
        contains "partitions=1/2 (p1)"
    }

    // ============================================================
    // Case 8b: DELETE partition -> version still advances -> partition visible but empty
    // ============================================================
    sql """DROP TABLE IF EXISTS mvp_td2;"""
    sql """
        CREATE TABLE mvp_td2 (
            id BIGINT,
            val BIGINT
        ) DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ('5'),
            PARTITION `p2` VALUES LESS THAN ('10')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO mvp_td2 VALUES(1, 10)"
    sql "INSERT INTO mvp_td2 VALUES(7, 70)"
    sql "DELETE FROM mvp_td2 PARTITION p2 WHERE id = 7;"
    explain {
        sql "select * from mvp_td2 max_visible_partition();"
        contains "partitions=1/2 (p2)"
    }
    qt_sql_td2_delete """select * from mvp_td2 max_visible_partition();"""

    // ============================================================
    // Case 9: CTE - modifier INSIDE CTE definition (on the base table) is supported
    // ============================================================
    order_qt_sql_cte_inside """
        WITH t AS (
            SELECT * FROM mvp_range max_visible_partition()
        )
        SELECT * FROM t;
    """
    // nested CTE + outer filter
    order_qt_sql_cte_inside_filter """
        WITH t AS (
            SELECT * FROM mvp_range max_visible_partition()
        )
        SELECT id, val FROM t WHERE val > 0;
    """

    // ============================================================
    // Case 9b: CTE - modifier on the CTE ALIAS is rejected
    // (CTEConsumer holds an already-analyzed subplan, not a base table)
    // ============================================================
    test {
        sql """
            WITH t AS (SELECT * FROM mvp_range)
            SELECT * FROM t max_visible_partition();
        """
        exception "max_visible_partition() is not supported on CTE reference"
    }

    // ============================================================
    // Case 10: JOIN - each side independently prunes to its latest visible partition
    // ============================================================
    order_qt_sql_join_same """
        SELECT a.id, a.val AS a_val, b.val AS b_val
        FROM mvp_range max_visible_partition() a
        JOIN mvp_range max_visible_partition() b
          ON a.id = b.id;
    """
    order_qt_sql_join_diff """
        SELECT r.dt, r.val AS r_val, m.val AS m_val
        FROM mvp_range_date max_visible_partition() r
        JOIN mvp_range max_visible_partition() m
          ON r.val = m.val;
    """

    // ============================================================
    // Case 11: table alias placement - modifier goes between table name and alias
    // ============================================================
    order_qt_sql_alias_bare """
        SELECT r.id, r.val
        FROM mvp_range max_visible_partition() r
        WHERE r.id > 0;
    """
    order_qt_sql_alias_as """
        SELECT x.id, x.val
        FROM mvp_range max_visible_partition() AS x
        WHERE x.id > 0;
    """

    // ============================================================
    // Case 12: subquery / derived table
    // ============================================================
    qt_sql_subquery """
        SELECT COUNT(*) AS cnt, MAX(val) AS max_val
        FROM (
            SELECT * FROM mvp_range max_visible_partition()
        ) sub;
    """

    // ============================================================
    // Case 13: UNION ALL - each branch prunes independently
    // ============================================================
    order_qt_sql_union_all """
        SELECT id, val FROM mvp_range max_visible_partition()
        UNION ALL
        SELECT id, val FROM mvp_sc max_visible_partition() WHERE id = 7;
    """

    // ============================================================
    // Case 14: aggregation / GROUP BY on the pruned partition only
    // ============================================================
    qt_sql_agg """SELECT COUNT(*) AS cnt, SUM(val) AS sum_val FROM mvp_range max_visible_partition();"""
    qt_sql_group_by """
        SELECT id, SUM(val) AS sum_val
        FROM mvp_range max_visible_partition()
        GROUP BY id
        ORDER BY id;
    """

    // ============================================================
    // Case 15: baseline sanity - explicit PARTITION() vs max_visible_partition()
    // ============================================================
    order_qt_sql_explicit_partition """select * from mvp_range PARTITION(p1);"""
    order_qt_sql_max_visible """select * from mvp_range max_visible_partition();"""
}
