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

suite("merge_topn_prefix_key") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET runtime_filter_mode=OFF"
    sql "SET topn_lazy_materialization_threshold=-1"

    sql "DROP TABLE IF EXISTS merge_topn_prefix_key"

    sql """
        CREATE TABLE merge_topn_prefix_key (
            id INT,
            a INT,
            b INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO merge_topn_prefix_key VALUES
            (11, 1, 1),
            (12, 1, 2),
            (13, 1, 3),
            (21, 2, 1),
            (22, 2, 2),
            (31, 3, 1)
    """

    qt_merge_child_more_order_keys_shape """
        EXPLAIN SHAPE PLAN
        SELECT id, a, b FROM (
            SELECT id, a, b FROM merge_topn_prefix_key ORDER BY a, b LIMIT 4
        ) t ORDER BY a LIMIT 2 OFFSET 1
    """

    qt_merge_child_more_order_keys """
        SELECT * FROM (
            SELECT id, a, b FROM (
                SELECT id, a, b FROM merge_topn_prefix_key ORDER BY a, b LIMIT 4
            ) t ORDER BY a LIMIT 2 OFFSET 1
        ) r ORDER BY a, b, id
    """
}
