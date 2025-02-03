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

suite("nested_mtmv_rewrite_switch") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_materialized_view_rewrite=true"

    sql """
    drop table if exists orders_2
    """
    sql """
        CREATE TABLE `orders_2` (
        `o_orderkey` BIGINT,
        `o_custkey` int,
        `o_orderstatus` VARCHAR(1),
        `o_totalprice` DECIMAL(15, 2),
        `o_orderpriority` VARCHAR(15),
        `o_clerk` VARCHAR(15),
        `o_shippriority` int,
        `o_comment` VARCHAR(79),
        `o_orderdate` DATE
        ) ENGINE=olap
        
        PROPERTIES (
        "replication_num" = "1"
        
        );
    """

    sql """
    drop table if exists lineitem_2
    """
    sql """
        CREATE TABLE `lineitem_2` (
        `l_orderkey` BIGINT,
        `l_linenumber` INT,
        `l_partkey` INT,
        `l_suppkey` INT,
        `l_quantity` DECIMAL(15, 2),
        `l_extendedprice` DECIMAL(15, 2),
        `l_discount` DECIMAL(15, 2),
        `l_tax` DECIMAL(15, 2),
        `l_returnflag` VARCHAR(1),
        `l_linestatus` VARCHAR(1),
        `l_commitdate` DATE,
        `l_receiptdate` DATE,
        `l_shipinstruct` VARCHAR(25),
        `l_shipmode` VARCHAR(10),
        `l_comment` VARCHAR(44),
        `l_shipdate` DATE
        ) ENGINE=olap
        
        PROPERTIES (
        "replication_num" = "1"
        
        );
    """

    sql """
    insert into orders_2 values
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19');
    """

    sql"""
    insert into lineitem_2 values
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """analyze table orders_2 with sync;"""
    sql """analyze table lineitem_2 with sync;"""

    sql """alter table orders_2 modify column o_orderdate set stats ('row_count'='10');"""
    sql """alter table lineitem_2 modify column l_shipdate set stats ('row_count'='7');"""


    def compare_res = { def stmt ->
        sql "SET enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }


    // create base first level mv
    create_async_mv(db, "join_mv1", """
    SELECT l_orderkey, l_linenumber, l_partkey, o_orderkey, o_custkey
    FROM lineitem_2 INNER JOIN orders_2
    ON l_orderkey = o_orderkey;
    """)

    // create second level mv based on first level mv
    create_async_mv(db, "agg_mv2", """
    SELECT
    l_orderkey,
    l_linenumber,
    o_orderkey,
    sum(l_partkey) AS total_revenue,
    max(o_custkey) AS max_discount
    FROM join_mv1
    GROUP BY l_orderkey, l_linenumber, o_orderkey;
    """)

    // create third level mv based on second level mv
    create_async_mv(db, "join_agg_mv3", """
    SELECT
    l_orderkey,
    sum(total_revenue) AS total_revenue,
    max(max_discount) AS max_discount
    FROM agg_mv2
    GROUP BY l_orderkey;
    """)

    def query = """
    SELECT l_orderkey, sum(l_partkey) AS total_revenue, max(o_custkey) AS max_discount FROM lineitem_2 INNER JOIN orders_2 ON l_orderkey = o_orderkey GROUP BY l_orderkey
    """

    sql """set enable_materialized_view_nest_rewrite = false;"""
    // Just first level mv rewrite successfully, second and third level mv should rewriten fail
    mv_rewrite_fail(query, "agg_mv2")
    mv_rewrite_fail(query, "join_agg_mv3")
    mv_rewrite_success(query, "join_mv1")
    compare_res(query + " order by 1,2,3")


    sql """set enable_materialized_view_nest_rewrite = true;"""
    // All mv rewrite successfully but only thirst level mv can be chosen by cbo
    mv_rewrite_success_without_check_chosen(query, "join_mv1")
    mv_rewrite_success_without_check_chosen(query, "agg_mv2")
    mv_rewrite_success(query, "join_agg_mv3")
    compare_res(query + " order by 1,2,3")


}
