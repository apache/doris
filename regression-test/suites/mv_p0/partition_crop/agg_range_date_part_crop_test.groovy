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

suite("agg_range_date_part_crop", "mv_part_crop") {
    String db = context.config.getDbNameByFile(context.file)
    String orders_tb = "agg_orders_range_date_crop_part"

    sql """
    drop table if exists ${orders_tb}
    """

    sql """CREATE TABLE `${orders_tb}` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT not NULL,
      `o_orderdate` DATE not null,
      `o_orderstatus` VARCHAR(1) replace,
      `o_totalprice` DECIMAL(15, 2) sum,
      `o_orderpriority` VARCHAR(15) replace,
      `o_clerk` VARCHAR(15) replace,
      `o_shippriority` INT sum,
      `o_comment` VARCHAR(79) replace
    ) ENGINE=OLAP
    aggregate KEY(`o_orderkey`, `o_custkey`, `o_orderdate`)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into ${orders_tb} values 
    (null, 1, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'), 
    (2, 1, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-18', 'o', 109.2, 'c','d',2, 'mm'),
    (3, 3, '2023-10-19', null, 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-20', 'o', null, 'a', 'b', 1, 'yy');
    """

    sql """alter table ${orders_tb} modify column o_orderkey set stats ('row_count'='40');"""
    sql """alter table ${orders_tb} modify column o_custkey set stats ('row_count'='40');"""
    sql """alter table ${orders_tb} modify column o_orderdate set stats ('row_count'='40');"""
    sql """alter table ${orders_tb} modify column o_orderstatus set stats ('row_count'='40');"""
    sql """alter table ${orders_tb} modify column o_totalprice set stats ('row_count'='40');"""
    sql """alter table ${orders_tb} modify column o_orderpriority set stats ('row_count'='40');"""
    sql """alter table ${orders_tb} modify column o_clerk set stats ('row_count'='40');"""
    sql """alter table ${orders_tb} modify column o_shippriority set stats ('row_count'='40');"""
    sql """alter table ${orders_tb} modify column o_comment set stats ('row_count'='40');"""

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

    def mv_name = "agg_rang_date_crop_part_mtmv"
    def mv_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        group by o_orderkey, o_custkey, o_orderdate
        """
    createMV(getMVStmt(mv_name, mv_sql))

    def query_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        where o_orderdate = '2023-10-17'
        group by o_orderkey, o_custkey, o_orderdate
        """
    explain {
        sql("${query_sql}")
        contains "partitions=1/4 (p20231017000000)"
    }
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3")

    query_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        where o_orderdate >= '2023-10-17' and o_orderdate < '2023-10-18'
        group by o_orderkey, o_custkey, o_orderdate
        """
    explain {
        sql("${query_sql}")
        contains "partitions=1/4 (p20231017000000)"
    }
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3")

    query_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        where o_orderdate <> '2023-10-17'
        group by o_orderkey, o_custkey, o_orderdate
        """
    explain {
        sql("${query_sql}")
        contains "partitions=3/4"
    }
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3")

}
