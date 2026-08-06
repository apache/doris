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

suite ("testBucketedAggSyncMV") {
    // Regression test: when enable_bucketed_hash_agg=true on a single-BE cluster,
    // the PhysicalBucketedHashAggregate plan for the base table with multi_distinct_count
    // could appear artificially cheap (bypasses one-phase agg ban, requests ANY properties),
    // beating the sync MV path on cost. The fix bans bucketed agg for MultiDistinction
    // functions in SplitAggWithoutDistinct.implementBucketedPhase().

    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"
    sql """set enable_nereids_planner=true;"""
    sql "set disable_nereids_rules='DISTINCT_AGGREGATE_SPLIT';"
    sql "set enable_bucketed_hash_agg = true;"

    sql """ DROP TABLE IF EXISTS bucketed_agg_mv_test; """

    sql """
        CREATE TABLE bucketed_agg_mv_test (
            time_col date NOT NULL,
            advertiser varchar(10),
            dt date NOT NULL,
            channel varchar(10),
            user_id int)
        DUPLICATE KEY(`time_col`, `advertiser`)
        PARTITION BY RANGE (dt)(
            FROM ("2024-07-01") TO ("2024-07-05") INTERVAL 1 DAY)
        DISTRIBUTED BY hash(time_col) BUCKETS 3
        PROPERTIES('replication_num' = '1');
    """

    sql """insert into bucketed_agg_mv_test values("2024-07-01",'a',"2024-07-01",'x',1);"""
    sql """insert into bucketed_agg_mv_test values("2024-07-01",'a',"2024-07-01",'x',1);"""
    sql """insert into bucketed_agg_mv_test values("2024-07-02",'a',"2024-07-02",'y',2);"""
    sql """insert into bucketed_agg_mv_test values("2024-07-02",'b',"2024-07-02",'x',1);"""
    sql """insert into bucketed_agg_mv_test values("2024-07-03",'b',"2024-07-03",'y',3);"""

    createMV("""
        CREATE MATERIALIZED VIEW bucketed_agg_uv_mv AS
        SELECT advertiser AS a1,
               channel AS a2,
               dt AS a3,
               bitmap_union(to_bitmap(user_id)) AS a4
        FROM bucketed_agg_mv_test
        GROUP BY advertiser,
                 channel,
                 dt;
    """)

    sql """insert into bucketed_agg_mv_test values("2024-07-03",'b',"2024-07-03",'y',4);"""

    sql "analyze table bucketed_agg_mv_test with sync;"
    sql """alter table bucketed_agg_mv_test modify column time_col set stats ('row_count'='6');"""

    // Core assertion: with enable_bucketed_hash_agg=true, the sync MV must still be chosen.
    // Before the fix, PhysicalBucketedHashAggregate(multi_distinct_count) would win on cost.
    mv_rewrite_success(
        "SELECT dt, advertiser, count(DISTINCT user_id) FROM bucketed_agg_mv_test GROUP BY dt, advertiser;",
        "bucketed_agg_uv_mv")

    // Verify correctness of results
    order_qt_select_mv """
        SELECT dt, advertiser, count(DISTINCT user_id)
        FROM bucketed_agg_mv_test
        GROUP BY dt, advertiser
        ORDER BY dt, advertiser;
    """

    // Also verify the MV is chosen for bitmap_union_count form
    mv_rewrite_success(
        "SELECT dt, advertiser, bitmap_union_count(to_bitmap(user_id)) FROM bucketed_agg_mv_test GROUP BY dt, advertiser;",
        "bucketed_agg_uv_mv")

    // Verify it still works when bucketed agg is disabled (baseline)
    sql "set enable_bucketed_hash_agg = false;"
    mv_rewrite_success(
        "SELECT dt, advertiser, count(DISTINCT user_id) FROM bucketed_agg_mv_test GROUP BY dt, advertiser;",
        "bucketed_agg_uv_mv")

    order_qt_select_mv_no_bucketed """
        SELECT dt, advertiser, count(DISTINCT user_id)
        FROM bucketed_agg_mv_test
        GROUP BY dt, advertiser
        ORDER BY dt, advertiser;
    """
}
