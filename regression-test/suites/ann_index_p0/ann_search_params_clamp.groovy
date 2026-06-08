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

// VEC-08 / VEC-09: Verify that search params are guarded correctly:
//   1. FE checker rejects ivf_nprobe=0 and hnsw_ef_search=0
//   2. BE clamps nprobe > nlist down to nlist (no FAISS assertion failure)
//   3. BE boosts efSearch to max(ef_search, k) so LIMIT k always returns k results

suite("ann_search_params_clamp", "nonConcurrent") {
    sql "set enable_common_expr_pushdown=true;"
    sql "set enable_ann_index_result_cache=false;"

    // -----------------------------------------------------------------------
    // 1. FE checker: zero values must be rejected
    // -----------------------------------------------------------------------
    test {
        sql "set ivf_nprobe=0"
        exception "ivf_nprobe must be >= 1"
    }

    test {
        sql "set hnsw_ef_search=0"
        exception "hnsw_ef_search must be >= 1"
    }

    // -----------------------------------------------------------------------
    // 2. nprobe > nlist must be clamped, not cause a FAISS error
    //    Table: IVF nlist=8, 400 rows (>= 39*nlist training threshold)
    //    Query: nprobe=99999 -> clamped to 8 -> returns results normally
    // -----------------------------------------------------------------------
    sql "drop table if exists tbl_ann_nprobe_clamp"
    sql """
        CREATE TABLE tbl_ann_nprobe_clamp (
            id INT NOT NULL,
            v ARRAY<FLOAT> NOT NULL,
            INDEX idx_v (v) USING ANN PROPERTIES(
                "index_type" = "ivf",
                "metric_type" = "l2_distance",
                "nlist" = "8",
                "dim" = "4"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    def rows = (1..400).collect { i ->
        "(${i}, [${i}.0, ${i * 2}.0, ${i * 3}.0, ${i * 4}.0])"
    }
    sql "INSERT INTO tbl_ann_nprobe_clamp VALUES ${rows.join(',')};"
    sql "sync"

    sql "set ivf_nprobe=99999;"
    // Clamped to nlist=8; query must succeed and return exactly 10 rows
    qt_nprobe_clamped_returns_10 """
        SELECT count(*) FROM (
            SELECT id FROM tbl_ann_nprobe_clamp
            ORDER BY l2_distance_approximate(v, [1.0, 2.0, 3.0, 4.0])
            LIMIT 10
        ) t;
    """

    // -----------------------------------------------------------------------
    // 3. efSearch boosted to max(ef_search, k): LIMIT k must return k rows
    //    Table: HNSW, 200 rows; ef_search=1 with LIMIT 50
    //    Before fix: FAISS explores 1 candidate, returns 1 result (others -1)
    //    After fix:  efSearch = max(1, 50) = 50, returns exactly 50 results
    // -----------------------------------------------------------------------
    sql "drop table if exists tbl_ann_ef_search_k"
    sql """
        CREATE TABLE tbl_ann_ef_search_k (
            id INT NOT NULL,
            v ARRAY<FLOAT> NOT NULL,
            INDEX idx_v (v) USING ANN PROPERTIES(
                "index_type" = "hnsw",
                "metric_type" = "l2_distance",
                "max_degree" = "16",
                "ef_construction" = "64",
                "dim" = "4"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    def rows2 = (1..200).collect { i ->
        "(${i}, [${i}.0, ${i * 2}.0, ${i * 3}.0, ${i * 4}.0])"
    }
    sql "INSERT INTO tbl_ann_ef_search_k VALUES ${rows2.join(',')};"
    sql "sync"

    sql "set hnsw_ef_search=1;"
    // efSearch boosted to max(1, 50)=50; must return exactly 50 rows
    qt_ef_search_boosted_to_k """
        SELECT count(*) FROM (
            SELECT id FROM tbl_ann_ef_search_k
            ORDER BY l2_distance_approximate(v, [1.0, 2.0, 3.0, 4.0])
            LIMIT 50
        ) t;
    """
}
