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

suite("test_datasketches_hll_union") {

    // sk = new HllSketch(8, HLL_8); for (int i = 0; i < 7; i++) sk.update(i);
    def sk1Base64 = "AgEHCAMIBwjL18IEK/L7BoYv+Q11gWYHgbxdBntl5gj8LUIK"

    // sk = new HllSketch(8, HLL_8); for (int i = 20; i < 30; i++) sk.update(i);
    def sk2Base64 = "AwEHCAUIAAkKAAAAIjvrBcS1nwfGGWoEyHokBO8t9wc1qTEENkcJB7hWqQxZf9QNnuSbGA=="

    sql "DROP TABLE IF EXISTS test_datasketches_hll_union_tbl"
    sql """
        CREATE TABLE test_datasketches_hll_union_tbl (
            id INT,
            grp INT,
            sk STRING
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO test_datasketches_hll_union_tbl VALUES
            (1, 1, from_base64('${sk1Base64}')),
            (2, 1, from_base64('${sk2Base64}')),
            (3, 1, NULL),
            (4, 2, NULL),
            (5, 2, NULL)
    """

    // 1) Basic union + alias consistency with datasketches_hll_union_agg
    order_qt_direct_est """
        SELECT CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT)
        FROM test_datasketches_hll_union_tbl
    """

    order_qt_basic_union """
        SELECT
            CAST(ROUND(datasketches_hll_union_agg(u1)) AS BIGINT),
            CAST(ROUND(datasketches_hll_union_agg(u2)) AS BIGINT)
        FROM (
            SELECT
                datasketches_hll_union(sk) AS u1,
                ds_hll_union(sk) AS u2
            FROM test_datasketches_hll_union_tbl
        ) t
    """

    order_qt_union_then_estimate """
        SELECT
            CAST(ROUND(datasketches_hll_union_agg(sk_u)) AS BIGINT),
            CAST(ROUND(datasketches_hll_union_agg(ds_u)) AS BIGINT)
        FROM (
            SELECT
                grp,
                datasketches_hll_union(sk) AS sk_u,
                ds_hll_union(sk) AS ds_u
            FROM test_datasketches_hll_union_tbl
            GROUP BY grp
        ) g
    """

    // 2) Group-by NULL semantics: grp=2 has only NULL rows, so union returns NULL
    order_qt_group_by_nulls """
        SELECT
            grp,
            datasketches_hll_union(sk) IS NULL AS is_null_union,
            CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT) AS direct_est
        FROM test_datasketches_hll_union_tbl
        GROUP BY grp
        ORDER BY grp
    """

    // 3) Empty table returns NULL (both nullable and not-null input columns)
    sql "DROP TABLE IF EXISTS test_datasketches_hll_union_empty_tbl"
    sql """
        CREATE TABLE test_datasketches_hll_union_empty_tbl (
            id INT NOT NULL,
            sk STRING,
            sk_nn STRING NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    order_qt_empty_table """
        SELECT
            datasketches_hll_union(sk) IS NULL,
            datasketches_hll_union(sk_nn) IS NULL
        FROM test_datasketches_hll_union_empty_tbl
    """

    // 4) lg_max_k and tgt_type verification via datasketches_hll_downsample no-op check
    order_qt_lg_max_k_and_tgt_type """
        SELECT
            datasketches_hll_downsample(u_default, 21, 'HLL_4') = u_default AS is_hll4_default,
            datasketches_hll_downsample(u_k7_hll8, 7, 'HLL_8') = u_k7_hll8 AS is_k7_hll8,
            datasketches_hll_downsample(u_k7_hll8, 7, 'HLL_4') = u_k7_hll8 AS differs_from_hll4
        FROM (
            SELECT
                datasketches_hll_union(sk) AS u_default,
                datasketches_hll_union(sk, 7, 'hll_8') AS u_k7_hll8
            FROM test_datasketches_hll_union_tbl
        ) t
    """

    order_qt_lg_max_k_and_tgt_type_est """
        SELECT
            CAST(ROUND(datasketches_hll_union_agg(u_k7_hll8)) AS BIGINT) AS est_k7_hll8
        FROM (
            SELECT
                datasketches_hll_union(sk, 7, 'hll_8') AS u_k7_hll8
            FROM test_datasketches_hll_union_tbl
        ) t
    """

    // 5) Input type coverage: STRING / VARCHAR / VARBINARY preserve return type
    sql "DROP TABLE IF EXISTS test_datasketches_hll_union_varchar_tbl"
    sql """
        CREATE TABLE test_datasketches_hll_union_varchar_tbl (
            id INT,
            sk VARCHAR(256)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """
        INSERT INTO test_datasketches_hll_union_varchar_tbl VALUES
            (1, from_base64('${sk1Base64}')),
            (2, from_base64('${sk2Base64}'))
    """

    sql "DROP VIEW IF EXISTS test_datasketches_hll_union_string_view"
    sql """
        CREATE VIEW test_datasketches_hll_union_string_view AS
        SELECT datasketches_hll_union(sk) AS u_str FROM test_datasketches_hll_union_tbl
    """
    order_qt_desc_string_view "DESC test_datasketches_hll_union_string_view"

    sql "DROP VIEW IF EXISTS test_datasketches_hll_union_varchar_view"
    sql """
        CREATE VIEW test_datasketches_hll_union_varchar_view AS
        SELECT datasketches_hll_union(sk) AS u_varchar FROM test_datasketches_hll_union_varchar_tbl
    """
    order_qt_desc_varchar_view "DESC test_datasketches_hll_union_varchar_view"

    order_qt_varbinary_non_null """
        SELECT hex(u_bin) IS NOT NULL
        FROM (
            SELECT datasketches_hll_union(sk, 8, 'HLL_6') AS u_bin
            FROM (
                SELECT from_base64_binary('${sk1Base64}') AS sk
                UNION ALL SELECT from_base64_binary('${sk2Base64}')
            ) src
        ) b
    """

    order_qt_varbinary_union """
        SELECT CAST(ROUND(datasketches_hll_union_agg(u_bin)) AS BIGINT)
        FROM (
            SELECT datasketches_hll_union(sk, 8, 'HLL_6') AS u_bin
            FROM (
                SELECT from_base64_binary('${sk1Base64}') AS sk
                UNION ALL SELECT from_base64_binary('${sk2Base64}')
            ) src
        ) b
    """

    // 6) Illegal parameters, DISTINCT, deferred constants, AGG_STATE, and corrupted input
    test {
        sql """SELECT datasketches_hll_union(from_base64('AA=='))"""
        exception "CORRUPTION"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, 6) FROM test_datasketches_hll_union_tbl"""
        exception "requires lg_max_k to be between 7 and 21"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, 22) FROM test_datasketches_hll_union_tbl"""
        exception "requires lg_max_k to be between 7 and 21"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, 4294967308) FROM test_datasketches_hll_union_tbl"""
        exception "requires lg_max_k to be between 7 and 21"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, 8, 'HLL_5') FROM test_datasketches_hll_union_tbl"""
        exception "requires tgt_type to be one of HLL_4, HLL_6, HLL_8"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, id) FROM test_datasketches_hll_union_tbl"""
        exception "requires lg_max_k to be a constant integer"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, NULL) FROM test_datasketches_hll_union_tbl"""
        exception "requires lg_max_k to be a constant integer"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, CAST(NULL AS INT)) FROM test_datasketches_hll_union_tbl"""
        exception "requires lg_max_k to be a constant integer"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, 8, CAST(NULL AS VARCHAR)) FROM test_datasketches_hll_union_tbl"""
        exception "requires tgt_type to be a constant string"
    }
    test {
        sql """SELECT datasketches_hll_union(DISTINCT sk) FROM test_datasketches_hll_union_tbl"""
        exception "does not support DISTINCT"
    }
    test {
        sql """SELECT ds_hll_union(DISTINCT sk, 8, 'HLL_6') FROM test_datasketches_hll_union_tbl"""
        exception "does not support DISTINCT"
    }

    sql "SET enable_fold_constant_by_be = false"
    test {
        sql """SELECT datasketches_hll_union(sk, crc32('')) FROM test_datasketches_hll_union_tbl"""
        exception "requires lg_max_k to be a constant integer"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, 8, regexp_extract_or_null('abc', '(z)', 1)) FROM test_datasketches_hll_union_tbl"""
        exception "requires tgt_type to be a constant string"
    }
    test {
        sql """SELECT datasketches_hll_union(sk, 8, regexp_extract_or_null('abc', '(z)', 1)) FROM test_datasketches_hll_union_empty_tbl"""
        exception "requires tgt_type to be a constant string"
    }
    test {
        sql """SELECT grp, datasketches_hll_union(sk, crc32('')) FROM test_datasketches_hll_union_tbl WHERE grp = 2 GROUP BY grp"""
        exception "requires lg_max_k to be a constant integer"
    }
    sql "SET enable_fold_constant_by_be = true"

    sql "SET enable_agg_state = true"
    test {
        sql """
            CREATE TABLE test_ds_hll_union_agg_state_ddl (
                k INT,
                s AGG_STATE<datasketches_hll_union(STRING)> GENERIC
            ) AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "does not support AGG_STATE"
    }
    test {
        sql """
            CREATE TABLE test_ds_hll_union_alias_agg_state_ddl (
                k INT,
                s AGG_STATE<ds_hll_union(STRING)> GENERIC
            ) AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "does not support AGG_STATE"
    }
    test {
        sql """SELECT datasketches_hll_union_state(sk) FROM test_datasketches_hll_union_tbl"""
        exception "Can not found function"
    }
    test {
        sql """SELECT ds_hll_union_state(sk) FROM test_datasketches_hll_union_tbl"""
        exception "Can not found function"
    }

    // 7) Window OVER clauses are rejected
    test {
        sql """SELECT datasketches_hll_union(sk) OVER () FROM test_datasketches_hll_union_tbl"""
        exception "Window function does not support aggregate function: datasketches_hll_union"
    }
    test {
        sql """SELECT ds_hll_union(sk, 12, 'HLL_8') OVER (PARTITION BY grp) FROM test_datasketches_hll_union_tbl"""
        exception "Window function does not support aggregate function: datasketches_hll_union"
    }

    // 8) FE-validated option literals are bound into the call even when constant folding is skipped
    sql "SET debug_skip_fold_constant = true"
    order_qt_debug_skip_fold_constant """
        SELECT CAST(ROUND(datasketches_hll_union_agg(u)) AS BIGINT)
        FROM (
            SELECT datasketches_hll_union(
                sk,
                IF(CAST(CAST('0.1' AS FLOAT) AS DOUBLE) > CAST('0.1' AS DOUBLE), 6, 7),
                concat('hll_', '8')
            ) AS u
            FROM test_datasketches_hll_union_tbl
        ) t
    """
    sql "SET debug_skip_fold_constant = false"
}
