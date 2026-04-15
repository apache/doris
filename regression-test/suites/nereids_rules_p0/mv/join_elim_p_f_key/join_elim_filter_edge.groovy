package mv.join_elim_p_f_key
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

suite("join_elim_filter_edge") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_join_elim_filter_edge"""
    sql """DROP TABLE IF EXISTS join_elim_filter_edge_t1"""
    sql """DROP TABLE IF EXISTS join_elim_filter_edge_t2"""

    sql """CREATE TABLE join_elim_filter_edge_t1 (
        id INT,
        name VARCHAR(50)
    )
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""

    sql """CREATE TABLE join_elim_filter_edge_t2 (
        id INT,
        value INT
    )
    UNIQUE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
    );"""

    sql """ALTER TABLE join_elim_filter_edge_t2 ADD CONSTRAINT uk_id UNIQUE (id)"""

    sql """INSERT INTO join_elim_filter_edge_t1 VALUES
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie');"""

    sql """INSERT INTO join_elim_filter_edge_t2 VALUES
        (1, 10),
        (2, -5),
        (4, 20);"""

    sql """ANALYZE TABLE join_elim_filter_edge_t1 WITH SYNC;"""
    sql """ANALYZE TABLE join_elim_filter_edge_t2 WITH SYNC;"""
    sql """ALTER TABLE join_elim_filter_edge_t1 MODIFY COLUMN name SET STATS ('row_count'='3');"""
    sql """ALTER TABLE join_elim_filter_edge_t2 MODIFY COLUMN value SET STATS ('row_count'='3');"""

    create_async_mv(db, "mv_join_elim_filter_edge", """
        SELECT
            t1.id,
            t1.name
        FROM join_elim_filter_edge_t1 t1
        LEFT JOIN join_elim_filter_edge_t2 t2
            ON t1.id = t2.id AND t2.id = 1
    """)

    def compare_res = { def stmt, int orderByColumns = 1 ->
        sql "SET enable_materialized_view_rewrite=false"
        def orderStmt = " order by " + (1..orderByColumns).join(", ")
        def origin_res = sql stmt + orderStmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt + orderStmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    def querySql = """SELECT id, name FROM join_elim_filter_edge_t1"""
    mv_rewrite_success_without_check_chosen(querySql, "mv_join_elim_filter_edge")
    compare_res(querySql, 2)
    order_qt_join_elim_filter_edge """SELECT id, name FROM join_elim_filter_edge_t1 ORDER BY 1, 2"""
}
