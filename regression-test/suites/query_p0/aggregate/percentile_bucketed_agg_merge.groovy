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

// Bucketed hash aggregation merges the per-instance states of the same group in the
// source operator. PERCENTILE states merged there must keep every sample of both sides.
suite("percentile_bucketed_agg_merge") {
    sql "set experimental_enable_agg_state=true"
    sql "set enable_bucketed_hash_agg=true"
    sql "set be_number_for_test=1"
    sql "set agg_phase=1"
    sql "set parallel_pipeline_task_num=4"
    sql "set bucketed_agg_min_input_rows=0"
    sql "set bucketed_agg_max_group_keys=0"
    sql "set bucketed_agg_high_card_threshold=1.0"

    sql "DROP TABLE IF EXISTS percentile_bucketed_agg_merge_t"
    sql """
        CREATE TABLE percentile_bucketed_agg_merge_t (
            id INT NOT NULL,
            shard INT NOT NULL,
            v INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        INSERT INTO percentile_bucketed_agg_merge_t VALUES
            (1, 0, 0), (2, 1, 0), (4, 1, 10),
            (5, 0, 7), (6, 1, 3), (7, 0, 1), (8, 1, 9),
            (9, 0, 5), (10, 1, 2), (11, 0, 8), (12, 1, 6)
    """

    explain {
        sql """
            SELECT shard, PERCENTILE_UNION(PERCENTILE_STATE(v, CAST(0.625 AS DOUBLE)))
            FROM percentile_bucketed_agg_merge_t GROUP BY shard
        """
        contains("BUCKETED AGGREGATE")
    }

    // merged PERCENTILE_UNION states of the same shard come from different instances
    order_qt_union_merge """
        SELECT PERCENTILE_MERGE(s) FROM (
            SELECT shard, PERCENTILE_UNION(PERCENTILE_STATE(v, CAST(0.625 AS DOUBLE))) s
            FROM percentile_bucketed_agg_merge_t GROUP BY shard
        ) q
    """
    order_qt_union_merge_group """
        SELECT shard, PERCENTILE_MERGE(s) FROM (
            SELECT shard, PERCENTILE_UNION(PERCENTILE_STATE(v, CAST(0.625 AS DOUBLE))) s
            FROM percentile_bucketed_agg_merge_t GROUP BY shard
        ) q GROUP BY shard
    """
    // raw (unmerged) update states of the same shard come from different instances
    order_qt_percentile_group """
        SELECT shard, PERCENTILE(v, 0.625), PERCENTILE_ARRAY(v, [0, 0.25, 0.5, 1])
        FROM percentile_bucketed_agg_merge_t GROUP BY shard
    """

    // controls: the same queries without bucketed hash aggregation
    sql "set enable_bucketed_hash_agg=false"
    order_qt_union_merge_no_bucketed """
        SELECT PERCENTILE_MERGE(s) FROM (
            SELECT shard, PERCENTILE_UNION(PERCENTILE_STATE(v, CAST(0.625 AS DOUBLE))) s
            FROM percentile_bucketed_agg_merge_t GROUP BY shard
        ) q
    """
    order_qt_union_merge_group_no_bucketed """
        SELECT shard, PERCENTILE_MERGE(s) FROM (
            SELECT shard, PERCENTILE_UNION(PERCENTILE_STATE(v, CAST(0.625 AS DOUBLE))) s
            FROM percentile_bucketed_agg_merge_t GROUP BY shard
        ) q GROUP BY shard
    """
    order_qt_percentile_group_no_bucketed """
        SELECT shard, PERCENTILE(v, 0.625), PERCENTILE_ARRAY(v, [0, 0.25, 0.5, 1])
        FROM percentile_bucketed_agg_merge_t GROUP BY shard
    """
}
