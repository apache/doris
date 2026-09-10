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

// Regression for GroupJoin fusion over a bucket shuffle join.
//
// PlanFragment.hasBucketShuffleNode looked for HashJoinNode and SetOperationNode only, but a fused
// group join is a PlanNode of its own (GroupJoinNode does not extend HashJoinNode). The fragment of
// a fused join therefore reported no bucket shuffle, UnassignedJobBuilder.shouldAssignByBucket said
// no, and the scan of the bucket side was assigned an UnassignedScanSingleOlapTableJob - while the
// exchange into the join still carried TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED. Running the
// SELECT then failed in DistributePlanner.getDestinationsByBuckets with
//   UnassignedScanSingleOlapTableJob cannot be cast to UnassignedScanBucketOlapTableJob
// EXPLAIN succeeded, because scan assignment only happens when the SELECT runs.
//
// The left table is bucketed by the join key while the right one is not, so the right side is
// shipped to the left table's buckets. enable_bucket_shuffle_join alone does not prove that: the
// plan has to actually report distribution BUCKET_SHUFFLE, which is what the explains below pin
// down, otherwise this suite would silently stop covering the defect path.
suite("test_group_join_fusion_bucket_shuffle") {
    sql "DROP TABLE IF EXISTS gj_bshuf_l"
    sql "DROP TABLE IF EXISTS gj_bshuf_r"

    sql """
        CREATE TABLE gj_bshuf_l (
            k INT NOT NULL,
            id INT NOT NULL,
            v INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k, id)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
        """

    // Bucketed by id rather than by the join key k, so the join cannot be colocated and the right
    // side has to be shuffled into the left table's buckets.
    sql """
        CREATE TABLE gj_bshuf_r (
            id INT NOT NULL,
            k INT NOT NULL,
            v INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
        """

    sql """INSERT INTO gj_bshuf_l VALUES (1,1,10), (1,2,20), (2,3,30)"""
    sql """INSERT INTO gj_bshuf_r VALUES (1,1,7), (2,1,11), (3,2,13)"""

    sql "ANALYZE TABLE gj_bshuf_l WITH SYNC"
    sql "ANALYZE TABLE gj_bshuf_r WITH SYNC"

    def bucketShuffleSetup = { ->
        sql "SET agg_phase = 1"
        sql "SET eager_aggregation_mode = -1"
        sql "SET eager_agg_broadcast_row_count = 0"
        sql "SET enable_bucketed_hash_agg = false"
        sql "SET enable_bucket_shuffle_join = true"
        sql "SET disable_join_reorder = true"
        sql "SET experimental_use_serial_exchange = false"
        sql "SET runtime_filter_mode = 'OFF'"
        sql "SET parallel_pipeline_task_num = 1"
        sql "SET enable_spill = false"
        sql "SET enable_aggregate_cse = true"
        sql "SET enable_sql_cache = false"
        sql "SET query_cache_force_refresh = true"
    }
    bucketShuffleSetup()

    def bucketShuffleQuery = """
        SELECT l.k, COUNT(*), SUM(l.v), SUM(r.v)
        FROM gj_bshuf_l l
        JOIN [shuffle] gj_bshuf_r r ON l.k = r.k
        GROUP BY l.k
        ORDER BY l.k
        """

    // 1. Precondition / positive control: the settings above really do produce a bucket shuffle
    //    into an ordinary hash join, so case 2 exercises the fused variant of that same plan and
    //    not some other distribution.
    sql "SET enable_group_join_fusion = false"
    explain {
        sql bucketShuffleQuery
        contains("VHASH JOIN")
        contains("BUCKET_SHUFFLE")
        notContains("VGROUP JOIN")
    }
    order_qt_bucket_shuffle_fusion_off bucketShuffleQuery
    def fusionOffResult = sql bucketShuffleQuery

    // 2. Regression: the same query fused into a GroupJoin. The distribution must stay
    //    BUCKET_SHUFFLE - that is the shape whose scan assignment used to be computed without the
    //    bucket flag - and the query must return the same rows as the unfused plan.
    sql "SET enable_group_join_fusion = true"
    explain {
        sql bucketShuffleQuery
        contains("VGROUP JOIN")
        contains("BUCKET_SHUFFLE")
    }
    order_qt_bucket_shuffle_fusion_on bucketShuffleQuery
    assertEquals(fusionOffResult, sql(bucketShuffleQuery))

    // 3. Same fused shape, run again: the failure happened in scan assignment, which is computed per
    //    run, so a repeat guards against a plan that only occasionally takes the bucket path.
    order_qt_bucket_shuffle_fusion_on_repeat bucketShuffleQuery

    // Restore defaults so other suites are not affected.
    sql "SET enable_group_join_fusion = false"
    sql "SET agg_phase = 0"
    sql "SET enable_bucket_shuffle_join = true"
    sql "SET disable_join_reorder = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
    sql "SET enable_bucketed_hash_agg = false"
    sql "SET parallel_pipeline_task_num = 0"
}
