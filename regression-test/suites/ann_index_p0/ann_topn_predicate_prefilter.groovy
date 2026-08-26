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

// Regression for an ANN TopN query that carries a residual column predicate (one NOT
// resolvable by zonemap/inverted/bitmap index). Such a predicate must be pre-filtered into a
// candidate bitmap fed to the ANN index as an IDSelector so the query keeps using the index,
// instead of degrading to an O(N) brute-force distance scan.
//
// Observation mechanism (same as ann_index_only_scan_debug_point): with debug point
// "segment_iterator._read_columns_by_index" enabled for column_name="embedding", reading the
// vector column throws "does not need to read data". On the index path the vector column is not
// read, so the query succeeds; on a brute-force fallback it is read, so the debug point fires.
//
// Three cases are covered:
//   1. a small single-batch segment -- basic index-path + top-K correctness check.
//   2. a segment large enough to span several scan batches (batch_size) -- this guards the
//      per-batch column reset in _eager_filter_predicates_into_bitmap. Without that reset,
//      batches after the first are filtered against stale predicate data, so the prefiltered
//      candidate set (and the top-K) is wrong. A single-batch segment never exercises it.
//   3. a segment spanning two condition-cache granules -- ANN TopN must not cache its top-K
//      truncation as the result of the column predicate.
suite("ann_topn_predicate_prefilter", "nonConcurrent") {
    sql "unset variable all;"
    sql "set enable_common_expr_pushdown=true;"
    sql "set experimental_enable_virtual_slot_for_cse=true;"
    sql "set enable_no_need_read_data_opt=true;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set enable_sql_cache=false;"
    sql "set enable_condition_cache=false;"

    // ============================ Case 1: single small batch ============================
    sql "drop table if exists ann_topn_pred_prefilter"
    sql """
        create table ann_topn_pred_prefilter (
            id int not null,
            category int not null,
            embedding array<float> not null,
            index ann_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="8"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    // 7 of 10 rows have category=1 (70% > 30%), so after pre-filtering the candidate set stays
    // above the small-candidate (0.3) fallback threshold and the ANN index path is exercised.
    sql """
        insert into ann_topn_pred_prefilter values
        (0, 1, [39.906116, 10.495334, 54.08394, 88.67262, 55.243687, 10.162686, 36.335983, 38.684258]),
        (1, 1, [62.759315, 97.15586, 25.832521, 39.604908, 88.76715, 72.64085, 9.688437, 17.721428]),
        (2, 1, [15.447449, 59.7771, 65.54516, 12.973712, 99.685135, 72.080734, 85.71118, 99.35976]),
        (3, 1, [72.26747, 46.42257, 32.368374, 80.50209, 5.777631, 98.803314, 7.0915947, 68.62693]),
        (4, 1, [22.098177, 74.10027, 63.634556, 4.710955, 12.405106, 79.39356, 63.014366, 68.67834]),
        (5, 1, [27.53003, 72.1106, 50.891026, 38.459953, 68.30715, 20.610682, 94.806274, 45.181377]),
        (6, 1, [77.73215, 64.42907, 71.50025, 43.85641, 94.42648, 50.04773, 65.12575, 68.58207]),
        (7, 2, [2.1537063, 82.667885, 16.171143, 71.126656, 5.335274, 40.286068, 11.943586, 3.69409]),
        (8, 2, [54.435013, 56.800594, 59.335514, 55.829235, 85.46627, 33.388138, 11.076194, 20.480877]),
        (9, 2, [76.197945, 60.623528, 84.229805, 31.652937, 71.82595, 48.04684, 71.29212, 30.282396]);
    """

    def v = "[26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]"

    // Primary signal: with a category predicate (no index on category, not excluded by zonemap)
    // the index path must NOT read the vector column. If it fell back to brute force it would read
    // embedding and the debug point would throw "does not need to read data", failing the query.
    try {
        GetDebugPoint().enableDebugPointForAllBEs(
                "segment_iterator._read_columns_by_index", [column_name: "embedding"])

        sql """
            select id
            from ann_topn_pred_prefilter
            where category = 1
            order by l2_distance_approximate(embedding, ${v})
            limit 5;
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }

    // Correctness: the index pre-filter path must return the same top-K as the brute-force path
    // (prefilter off => fall back to an exact distance scan over the same filtered rows).
    def topnSql = """
        select id
        from ann_topn_pred_prefilter
        where category = 1
        order by l2_distance_approximate(embedding, ${v})
        limit 5;
    """

    sql "set enable_ann_topn_predicate_prefilter=true;"
    def withPrefilter = sql topnSql

    sql "set enable_ann_topn_predicate_prefilter=false;"
    def bruteForce = sql topnSql

    assertEquals(bruteForce, withPrefilter,
            "ANN pre-filter top-K must match brute-force top-K for the same predicated query")

    // ===================== Case 2: multiple scan batches in one segment =====================
    // Shrink the scan batch so a few hundred rows in a single segment span several batches; this
    // is the path where a missing per-batch reset would filter later batches with stale data.
    sql "set batch_size = 64;"
    // Case 1 left the prefilter disabled (its brute-force leg); re-enable it before the index-path
    // check below, otherwise the column predicate would force a brute-force fallback.
    sql "set enable_ann_topn_predicate_prefilter=true;"
    // Raise ef_search so HNSW searches the full candidate set: the oracle below asserts exact
    // equality against the brute-force path, which only holds once HNSW is not leaving any of
    // these few hundred candidates unvisited at the default ef_search.
    sql "set hnsw_ef_search=4096;"

    sql "drop table if exists ann_topn_pred_prefilter_mb"
    sql """
        create table ann_topn_pred_prefilter_mb (
            id int not null,
            category int not null,
            embedding array<float> not null,
            index ann_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="8"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    // Query vector and a deterministic layout: a handful of category=1 rows are planted very
    // close to the query (increasing distance), all at high ids so they land in batch 2+. One
    // category=2 row is planted equally close to catch a false-positive leak. Every other row is
    // far away, so the true top-5 (category=1, nearest) is exactly the 5 planted category=1 ids.
    def qmb = [10, 10, 10, 10, 10, 10, 10, 10]
    def plantedCat1 = [100: 1, 140: 2, 180: 3, 220: 4, 260: 6]   // id -> first-coord offset (== L2 distance)
    def plantedCat2Id = 200
    def plantedCat2Offset = 5
    def total = 300

    def fmt = { e -> "[" + e.collect { (it as float) }.join(",") + "]" }
    def rows = []
    for (int id = 0; id < total; id++) {
        def cat
        def emb
        if (plantedCat1.containsKey(id)) {
            cat = 1
            emb = [10 + plantedCat1[id], 10, 10, 10, 10, 10, 10, 10]
        } else if (id == plantedCat2Id) {
            cat = 2
            emb = [10 + plantedCat2Offset, 10, 10, 10, 10, 10, 10, 10]
        } else {
            // ~70% category=1 so survivors stay above the 0.3 fallback threshold; far from qmb.
            cat = (id % 10 < 7) ? 1 : 2
            emb = [200 + (id % 7), 201, 202, 203, 204, 205, 206, 207]
        }
        rows.add("(${id}, ${cat}, ${fmt(emb)})")
    }
    // One INSERT => one rowset => one segment, so the rows really span scan batches in a segment.
    sql "insert into ann_topn_pred_prefilter_mb values ${rows.join(",")};"

    def vmb = fmt(qmb)

    // Primary signal again: predicated TopN over a multi-batch segment must stay on the index path.
    try {
        GetDebugPoint().enableDebugPointForAllBEs(
                "segment_iterator._read_columns_by_index", [column_name: "embedding"])

        sql """
            select id
            from ann_topn_pred_prefilter_mb
            where category = 1
            order by l2_distance_approximate(embedding, ${vmb})
            limit 5;
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }

    def topnSqlMb = """
        select id
        from ann_topn_pred_prefilter_mb
        where category = 1
        order by l2_distance_approximate(embedding, ${vmb})
        limit 5;
    """

    sql "set enable_ann_topn_predicate_prefilter=true;"
    def withPrefilterMb = sql topnSqlMb

    sql "set enable_ann_topn_predicate_prefilter=false;"
    def bruteForceMb = sql topnSqlMb

    // Same top-K as the exact path, and exactly the planted category=1 ids in distance order.
    // A stale-batch bug would drop a planted category=1 row or leak the planted category=2 row,
    // changing this result. The explicit id list also rejects a vacuous (e.g. both-empty) pass.
    def idsOf = { res -> res.collect { (it[0] as int) } }
    assertEquals(idsOf(bruteForceMb), idsOf(withPrefilterMb),
            "multi-batch ANN pre-filter top-K must match brute-force top-K")
    assertEquals([100, 140, 180, 220, 260], idsOf(withPrefilterMb),
            "multi-batch ANN pre-filter must return exactly the planted category=1 neighbors")

    // ===================== Case 3: ANN TopN must not poison condition cache =====================
    // Condition cache tracks predicate survivors at a 2048-row granularity. An ANN TopN scan
    // narrows its row bitmap to top-K before the scan loop runs, so it must not populate the
    // predicate cache: a later query with the same predicate would otherwise skip granules that
    // contain predicate matches but no previous top-K result.
    sql "set enable_condition_cache=true;"
    sql "set enable_ann_topn_predicate_prefilter=true;"
    sql "set enable_ann_index_result_cache=false;"
    sql "set enable_sql_cache=false;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set hnsw_ef_search=4096;"

    sql "drop table if exists ann_topn_pred_prefilter_condition_cache"
    sql """
        create table ann_topn_pred_prefilter_condition_cache (
            id int not null,
            category int not null,
            embedding array<float> not null,
            index ann_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="8"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    // Every 2048-row condition-cache granule contains both categories, so zonemap cannot prune
    // category = 1. The first five category=1 rows are uniquely nearest to the zero query vector
    // and all belong to the first granule; category=1 stays above the 30% ANN fallback threshold.
    def conditionCacheRows = []
    for (int id = 0; id < 4096; id++) {
        def category = (id % 10 < 7) ? 1 : 2
        conditionCacheRows.add("(${id}, ${category}, [${id}.0,0,0,0,0,0,0,0])")
    }
    sql "insert into ann_topn_pred_prefilter_condition_cache values ${conditionCacheRows.join(',')};"
    sql "sync"

    def cacheTopnRows = sql """
        select id
        from ann_topn_pred_prefilter_condition_cache
        where category = 1
        order by l2_distance_approximate(embedding, [0,0,0,0,0,0,0,0])
        limit 5;
    """
    assertEquals([0, 1, 2, 3, 4], cacheTopnRows.collect { it[0] as int })

    // This must be the first non-ANN query with category = 1. Before the fix, the preceding ANN
    // TopN query caches only the first granule and this count incorrectly returns 1435.
    def cacheCountRows = sql """
        select count(*)
        from ann_topn_pred_prefilter_condition_cache
        where category = 1;
    """
    assertEquals(2869L, cacheCountRows[0][0] as long,
            "ANN TopN must not poison condition cache for category=1")

    sql "set enable_condition_cache=false;"
    sql "set enable_ann_topn_predicate_prefilter=true;"
}
