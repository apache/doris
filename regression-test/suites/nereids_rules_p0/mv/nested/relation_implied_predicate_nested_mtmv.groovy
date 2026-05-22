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

suite("relation_implied_predicate_nested_mtmv") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_materialized_view_nest_rewrite=true"
    sql "SET pre_materialized_view_rewrite_strategy=NOT_IN_RBO"

    sql """DROP MATERIALIZED VIEW IF EXISTS rip_mv_join"""
    sql """DROP MATERIALIZED VIEW IF EXISTS rip_mv_fact"""
    sql """DROP TABLE IF EXISTS rip_fact"""
    sql """DROP TABLE IF EXISTS rip_dim"""

    sql """
    CREATE TABLE rip_fact (
        k1 BIGINT,
        dim_id BIGINT,
        sku_type VARCHAR(8),
        amount BIGINT
    )
    DUPLICATE KEY(k1)
    DISTRIBUTED BY HASH(k1) BUCKETS 1
    PROPERTIES ("replication_num" = "1")
    """

    sql """
    CREATE TABLE rip_dim (
        dim_id BIGINT,
        tag VARCHAR(16)
    )
    DUPLICATE KEY(dim_id)
    DISTRIBUTED BY HASH(dim_id) BUCKETS 1
    PROPERTIES ("replication_num" = "1")
    """

    sql """
    INSERT INTO rip_fact VALUES
        (1, 10, '1', 100),
        (2, 10, '2', 200),
        (3, 20, '1', 300)
    """

    sql """
    INSERT INTO rip_dim VALUES
        (10, 'A'),
        (20, 'B')
    """

    sql """ANALYZE TABLE rip_fact WITH SYNC"""
    sql """ANALYZE TABLE rip_dim WITH SYNC"""
    sql """ALTER TABLE rip_fact MODIFY COLUMN amount SET STATS ('row_count'='3')"""
    sql """ALTER TABLE rip_dim MODIFY COLUMN tag SET STATS ('row_count'='2')"""

    def query = """
        SELECT f.k1, f.dim_id, f.amount, d.tag
        FROM rip_fact f
        INNER JOIN rip_dim d ON f.dim_id = d.dim_id
        WHERE f.sku_type = '1'
        ORDER BY 1, 2, 3, 4
    """

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_before "${query}"

    sql "SET enable_materialized_view_rewrite=true"
    create_async_mv(db, "rip_mv_fact", """
        SELECT k1, dim_id, amount
        FROM rip_fact
        WHERE sku_type = '1'
    """)
    create_async_mv(db, "rip_mv_join", """
        SELECT f.k1, f.dim_id, f.amount, d.tag
        FROM rip_mv_fact f
        INNER JOIN rip_dim d ON f.dim_id = d.dim_id
    """)

    mv_rewrite_success(query, "rip_mv_join")
    order_qt_query_after "${query}"
}
