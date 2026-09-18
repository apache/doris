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

/**
 * Regression test for enable_local_exchange_before_agg (apache/doris#62438).
 *
 * When enable_local_exchange_before_agg=true (default), BE inserts HASH local exchange
 * before pre-agg operators. When enable_local_shuffle_planner=true, FE does the same.
 * This test verifies correctness under both combinations.
 */
suite("test_enable_local_exchange_before_agg", "p0") {

    sql "DROP TABLE IF EXISTS le_agg_t1"
    sql """
        CREATE TABLE le_agg_t1 (
            k1 INT NOT NULL,
            k2 VARCHAR(32),
            v1 INT,
            v2 BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """

    sql """INSERT INTO le_agg_t1 VALUES
        (1, 'a', 10, 100), (1, 'a', 20, 200), (1, 'b', 30, 300),
        (2, 'a', 40, 400), (2, 'b', 50, 500), (2, 'b', 60, 600),
        (3, 'c', 70, 700), (3, 'c', 80, 800), (3, 'd', 90, 900),
        (4, 'a', 100, 1000), (4, 'd', 110, 1100), (4, 'd', 120, 1200),
        (5, 'e', 130, 1300), (5, 'e', 140, 1400), (5, 'f', 150, 1500)
    """

    def PH = "PLACEHOLDER"

    def beHints = """/*+SET_VAR(
        parallel_pipeline_task_num=4,
        enable_local_shuffle=true,
        ignore_storage_data_distribution=true,
        enable_local_shuffle_planner=false,
        enable_local_exchange_before_agg=true
    )*/"""

    def feHints = """/*+SET_VAR(
        parallel_pipeline_task_num=4,
        enable_local_shuffle=true,
        ignore_storage_data_distribution=true,
        enable_local_shuffle_planner=true,
        enable_local_exchange_before_agg=true
    )*/"""

    def noLeBeforeAggHints = """/*+SET_VAR(
        parallel_pipeline_task_num=4,
        enable_local_shuffle=true,
        ignore_storage_data_distribution=true,
        enable_local_shuffle_planner=true,
        enable_local_exchange_before_agg=false
    )*/"""

    // Forces LOCAL preagg through the non-streaming AggSink / DistinctStreaming paths
    // (instead of the StreamingAgg branch). Combined with enable_local_exchange_before_agg=false,
    // this exercises the phase-aware fix:
    //   - AggSink:           !isMerge() LOCAL phase → base (PASSTHROUGH/NOOP)
    //                        FIRST_MERGE             → HASH (correctness, regardless of flag)
    //   - DistinctStreaming: useStreamingPreagg=true  → base
    //                        useStreamingPreagg=false → HASH (correctness, regardless of flag)
    def disableStreamingHints = """/*+SET_VAR(
        parallel_pipeline_task_num=4,
        enable_local_shuffle=true,
        ignore_storage_data_distribution=true,
        enable_local_shuffle_planner=true,
        enable_local_exchange_before_agg=false,
        disable_streaming_preaggregations=true
    )*/"""

    def disableStreamingFlagOnHints = """/*+SET_VAR(
        parallel_pipeline_task_num=4,
        enable_local_shuffle=true,
        ignore_storage_data_distribution=true,
        enable_local_shuffle_planner=true,
        enable_local_exchange_before_agg=true,
        disable_streaming_preaggregations=true
    )*/"""

    def queries = [
        "simple_agg_bucket_key":
            "SELECT ${PH} k1, SUM(v1), COUNT(*) FROM le_agg_t1 GROUP BY k1 ORDER BY k1",
        "simple_agg_non_bucket_key":
            "SELECT ${PH} k2, SUM(v1), MAX(v2) FROM le_agg_t1 GROUP BY k2 ORDER BY k2",
        "multi_key_agg":
            "SELECT ${PH} k1, k2, SUM(v1) FROM le_agg_t1 GROUP BY k1, k2 ORDER BY k1, k2",
        "distinct_non_bucket":
            "SELECT ${PH} DISTINCT k2 FROM le_agg_t1 ORDER BY k2",
        "count_distinct":
            "SELECT ${PH} k1, COUNT(DISTINCT k2), SUM(v1) FROM le_agg_t1 GROUP BY k1 ORDER BY k1",
        "agg_having":
            "SELECT ${PH} k2, SUM(v1) AS s FROM le_agg_t1 GROUP BY k2 HAVING SUM(v1) > 100 ORDER BY k2",
        "agg_after_join":
            "SELECT ${PH} a.k2, SUM(a.v1) FROM le_agg_t1 a JOIN le_agg_t1 b ON a.k1 = b.k1 GROUP BY a.k2 ORDER BY a.k2",
        "grouping_sets":
            "SELECT ${PH} k1, k2, SUM(v1) FROM le_agg_t1 GROUP BY GROUPING SETS ((k1), (k2), (k1, k2)) ORDER BY k1, k2",
        "window_over_agg":
            "SELECT ${PH} k1, s, SUM(s) OVER (ORDER BY k1) AS running FROM (SELECT k1, SUM(v1) AS s FROM le_agg_t1 GROUP BY k1) t ORDER BY k1",
        "multi_distinct":
            "SELECT ${PH} COUNT(DISTINCT k1), COUNT(DISTINCT k2) FROM le_agg_t1",
    ]

    // Part 1: FE-planned (enable_local_exchange_before_agg=true) vs BE-planned baseline
    logger.info("=== Part 1: FE vs BE with enable_local_exchange_before_agg=true ===")
    queries.each { name, template ->
        def beResult = sql(template.replace(PH, beHints))
        def feResult = sql(template.replace(PH, feHints))
        assertEquals(beResult, feResult, "[${name}] FE-planned differs from BE-planned")
        logger.info("[${name}] PASSED")
    }

    // Part 2: enable_local_exchange_before_agg=false — verify no crash/hang and correct results
    logger.info("=== Part 2: enable_local_exchange_before_agg=false ===")
    queries.each { name, template ->
        def beResult = sql(template.replace(PH, beHints))
        def noLeResult = sql(template.replace(PH, noLeBeforeAggHints))
        assertEquals(beResult, noLeResult, "[${name}] enable_local_exchange_before_agg=false differs from baseline")
        logger.info("[${name}] enable_local_exchange_before_agg=false PASSED")
    }

    // Part 3: disable_streaming_preaggregations=true — forces AggSink / DistinctStreaming
    // non-streaming paths. Combined with the two flag values, exercises the phase-aware
    // fix on both LOCAL (performance, flag controls) and MERGE/non-streaming-dedup
    // (correctness, always HASH) sub-paths.
    logger.info("=== Part 3: disable_streaming_preaggregations=true ===")
    queries.each { name, template ->
        def beResult = sql(template.replace(PH, beHints))
        def disableStreamingFlagOnResult = sql(template.replace(PH, disableStreamingFlagOnHints))
        assertEquals(beResult, disableStreamingFlagOnResult,
                "[${name}] disable_streaming+flag=true differs from baseline")
        def disableStreamingResult = sql(template.replace(PH, disableStreamingHints))
        assertEquals(beResult, disableStreamingResult,
                "[${name}] disable_streaming+flag=false differs from baseline")
        logger.info("[${name}] disable_streaming both flag values PASSED")
    }

    logger.info("=== All enable_local_exchange_before_agg tests completed ===")
}
