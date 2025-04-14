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

suite("mv_filter_equal_or_notequal_case_agg", "partition_mv_rewrite_dimension") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """
    drop table if exists orders_filter_agg
    """

    sql """CREATE TABLE `orders_filter_agg` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderdate` DATE not NULL,
      `o_orderstatus` VARCHAR(1) replace,
      `o_totalprice` DECIMAL(15, 2) sum,
      `o_orderpriority` VARCHAR(15) replace,
      `o_clerk` VARCHAR(15) replace,
      `o_shippriority` INT sum,
      `o_comment` VARCHAR(79) replace
    ) ENGINE=OLAP
    aggregate KEY(`o_orderkey`, `o_custkey`, `o_orderdate`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_filter_agg
    """

    sql """CREATE TABLE `lineitem_filter_agg` (
      `l_orderkey` BIGINT NULL,
      `l_linenumber` INT NULL,
      `l_partkey` INT NULL,
      `l_suppkey` INT NULL,
      `l_shipdate` DATE not NULL,
      `l_quantity` DECIMAL(15, 2) sum,
      `l_extendedprice` DECIMAL(15, 2) sum,
      `l_discount` DECIMAL(15, 2) sum,
      `l_tax` DECIMAL(15, 2) sum,
      `l_returnflag` VARCHAR(1) replace,
      `l_linestatus` VARCHAR(1) replace,
      `l_commitdate` DATE replace,
      `l_receiptdate` DATE replace,
      `l_shipinstruct` VARCHAR(25) replace,
      `l_shipmode` VARCHAR(10) replace,
      `l_comment` VARCHAR(44) replace
    ) ENGINE=OLAP
    aggregate KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate )
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_filter_agg values 
    (null, 1, '2023-10-17', 'o', 99.5, 'a', 'b', 1, 'yy'),
    (1, null, '2023-10-17', 'k', 109.2, 'c','d',2, 'mm'),
    (3, 3, '2023-10-19', null, 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-20', 'o', null, 'a', 'b', 1, 'yy'),
    (2, 3, '2023-10-21', 'k', 109.2, null,'d',2, 'mm'),
    (3, 1, '2023-10-22', 'o', 99.5, 'a', null, 1, 'yy'),
    (1, 3, '2023-10-19', 'k', 99.5, 'a', 'b', null, 'yy'),
    (2, 1, '2023-10-18', 'o', 109.2, 'c','d',2, null),
    (3, 2, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (4, 5, '2023-10-19', 'o', 99.5, 'a', 'b', 1, 'yy');
    """

    sql """
    insert into lineitem_filter_agg values 
    (null, 1, 2, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, null, 3, 1, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 3, null, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (1, 2, 3, null, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 3, 2, 1, '2023-10-18', 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 1, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx'),
    (1, 3, 2, 2, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """


    sql """alter table orders_filter_agg modify column o_orderkey set stats ('row_count'='50');"""
    sql """alter table orders_filter_agg modify column o_custkey set stats ('row_count'='50');"""
    sql """alter table orders_filter_agg modify column o_orderdate set stats ('row_count'='50');"""
    sql """alter table orders_filter_agg modify column o_orderstatus set stats ('row_count'='50');"""
    sql """alter table orders_filter_agg modify column o_totalprice set stats ('row_count'='50');"""
    sql """alter table orders_filter_agg modify column o_orderpriority set stats ('row_count'='50');"""
    sql """alter table orders_filter_agg modify column o_clerk set stats ('row_count'='50');"""
    sql """alter table orders_filter_agg modify column o_shippriority set stats ('row_count'='50');"""
    sql """alter table orders_filter_agg modify column o_comment set stats ('row_count'='50');"""

    sql """alter table lineitem_filter_agg modify column l_orderkey set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_linenumber set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_partkey set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_suppkey set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_quantity set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_extendedprice set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_discount set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_tax set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_returnflag set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_linestatus set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_commitdate set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_receiptdate set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_shipinstruct set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_shipmode set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_comment set stats ('row_count'='50');"""
    sql """alter table lineitem_filter_agg modify column l_shipdate set stats ('row_count'='50');"""


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

    def tb_lineitem = "lineitem_filter_agg"

    def mv_name = "mv1"
    def mv_sql = """
        select l_shipdate, l_partkey, l_suppkey
        from lineitem_filter_agg 
        where l_shipdate = '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """

    createMV(getMVStmt(mv_name, mv_sql))

    // mv equal and sql equal
    def query_sql = """
        select l_shipdate, l_partkey, l_suppkey  
        from lineitem_filter_agg 
        where l_shipdate = '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    // mv equal and sql not equal
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate != '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // mv equal and sql equal and number is difference
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        left join orders_filter_agg 
        on lineitem_filter_agg.l_orderkey = orders_filter_agg.o_orderkey
        where l_shipdate = '2023-10-19'
        group by l_shipdate, o_orderdate, l_partkey, l_suppkey 
        """
    mv_rewrite_fail(query_sql, mv_name)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} ON lineitem_filter_agg;"""

    mv_name = "mv2"
    mv_sql = """
        select l_shipdate, l_partkey, l_suppkey  
        from lineitem_filter_agg 
        where l_shipdate != '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """

    createMV(getMVStmt(mv_name, mv_sql))

    // mv not equal and sql equal
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate = '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // mv not equal and sql not equal
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate != '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} ON lineitem_filter_agg;"""

    mv_name = "mv3"
    mv_sql = """
        select l_shipdate, l_partkey, l_suppkey   
        from lineitem_filter_agg 
        where l_shipdate > '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """

    createMV(getMVStmt(mv_name, mv_sql))

    // mv is range and sql equal and filter not in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate = '2023-10-16'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // mv is range and sql equal and filter in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate = '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // mv is range and sql equal and filter in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey
        from lineitem_filter_agg
        where l_shipdate = '2023-10-19'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is range and sql is range and sql range is bigger than mv
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate > '2023-10-16'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // mv is range and sql is range and sql range is not in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate < '2023-10-16'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // mv is range and sql is range and sql range is bigger than mv
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate > '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is range and sql is range and sql range is not in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate < '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // mv is range and sql is range and sql range is bigger than mv
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey
        from lineitem_filter_agg
        where l_shipdate > '2023-10-18'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is range and sql is range and sql range is not in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate < '2023-10-19'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // mv is range and sql is range and sql range is bigger than mv
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey  
        from lineitem_filter_agg 
        where l_shipdate > '2023-10-16' and l_shipdate < '2023-10-17'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // mv is range and sql is range and sql range is not in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate > '2023-10-17' and l_shipdate < '2023-10-19'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    // sql range is not in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate in ('2023-10-17')
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // sql range is in mv range
    // single value
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey
        from lineitem_filter_agg
        where l_shipdate in ('2023-10-18')
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    // multi value
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey
        from lineitem_filter_agg
        where l_shipdate in ('2023-10-18', '2023-11-18')
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    // sql range like mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate like "%2023-10-18%"
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // sql range is null
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate is null
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)

    // sql range is not null
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey  
        from lineitem_filter_agg 
        where l_shipdate is not null
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} ON lineitem_filter_agg;"""

    mv_name = "mv4"
    mv_sql = """
        select l_shipdate, l_partkey, l_suppkey  
        from lineitem_filter_agg 
        where l_shipdate > '2023-10-16' and l_shipdate < '2023-10-19'
        group by l_shipdate, l_partkey, l_suppkey
        """

    createMV(getMVStmt(mv_name, mv_sql))

    // mv is range and sql is range and filter not in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey
        from lineitem_filter_agg
        where l_shipdate > '2023-10-16' and l_shipdate < '2023-10-19'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")


    query_sql = """
        select l_shipdate, l_partkey, l_suppkey
        from lineitem_filter_agg
        where l_shipdate > '2023-10-17' and l_shipdate < '2023-10-19'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} ON lineitem_filter_agg;"""

    mv_name = "mv5"
    mv_sql = """
        select l_shipdate, l_partkey, l_suppkey   
        from lineitem_filter_agg 
        where l_shipdate in ('2023-10-17', '2023-10-18', '2023-10-19')
        group by l_shipdate, l_partkey, l_suppkey
        """

    createMV(getMVStmt(mv_name, mv_sql))

    // mv is in range and sql is in range and filter is in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate in ('2023-10-17', '2023-10-18', '2023-10-19')
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is in range and sql is in range and filter is not in mv range
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate not in ('2023-10-17', '2023-10-18', '2023-10-19')
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_fail(query_sql, mv_name)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} ON lineitem_filter_agg;"""

    mv_name = "mv6"
    mv_sql = """
        select l_shipdate, l_partkey, l_suppkey   
        from lineitem_filter_agg 
        where l_shipdate like "%2023-10-17%"
        group by l_shipdate, l_partkey, l_suppkey
        """

    createMV(getMVStmt(mv_name, mv_sql))

    // mv is like filter and sql is like filter and filter number is equal
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate like "%2023-10-17%"
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is like filter and sql is not like filter
    query_sql = """
        select l_shipdate, l_partkey, l_suppkey 
        from lineitem_filter_agg 
        where l_shipdate like "%2023-10-17%"
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} ON lineitem_filter_agg;"""

    // between .. and ..
    mv_name = "mv7"
    mv_sql = """
        select l_shipdate, l_partkey, l_suppkey
        from lineitem_filter_agg
        where l_shipdate between '2023-10-16' and '2023-10-19'
        group by l_shipdate, l_partkey, l_suppkey
        """

    createMV(getMVStmt(mv_name, mv_sql))

    query_sql = """
        select l_shipdate, l_partkey, l_suppkey
        from lineitem_filter_agg
        where l_shipdate between '2023-10-17' and '2023-10-19'
        group by l_shipdate, l_partkey, l_suppkey
        """
    mv_rewrite_success(query_sql, mv_name)
    compare_res(query_sql + " order by 1,2,3,4")

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
}
