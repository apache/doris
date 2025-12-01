package mv.limit
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

suite("limit_rewrite") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    // this maybe fail when set NOT_IN_RBO fuzzy in session variable, so set manually
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"

    sql """
    drop table if exists orders
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      o_orderkey       INTEGER NOT NULL,
      o_custkey        INTEGER NOT NULL,
      o_orderstatus    CHAR(1) NOT NULL,
      o_totalprice     DECIMALV3(15,2) NOT NULL,
      o_orderdate      DATE NOT NULL,
      o_orderpriority  CHAR(15) NOT NULL,  
      o_clerk          CHAR(15) NOT NULL, 
      o_shippriority   INTEGER NOT NULL,
      O_COMMENT        VARCHAR(79) NOT NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate) (
    PARTITION `day_2` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_4` VALUES LESS THAN ("2023-12-30")
    )
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    drop table if exists lineitem
    """

    sql"""
    CREATE TABLE IF NOT EXISTS lineitem (
      l_orderkey    INTEGER NOT NULL,
      l_partkey     INTEGER NOT NULL,
      l_suppkey     INTEGER NOT NULL,
      l_linenumber  INTEGER NOT NULL,
      l_quantity    DECIMALV3(15,2) NOT NULL,
      l_extendedprice  DECIMALV3(15,2) NOT NULL,
      l_discount    DECIMALV3(15,2) NOT NULL,
      l_tax         DECIMALV3(15,2) NOT NULL,
      l_returnflag  CHAR(1) NOT NULL,
      l_linestatus  CHAR(1) NOT NULL,
      l_shipdate    DATE NOT NULL,
      l_commitdate  DATE NOT NULL,
      l_receiptdate DATE NOT NULL,
      l_shipinstruct CHAR(25) NOT NULL,
      l_shipmode     CHAR(10) NOT NULL,
      l_comment      VARCHAR(44) NOT NULL
    )
    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
    PARTITION BY RANGE(l_shipdate) (
    PARTITION `day_1` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_2` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-30"))
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    drop table if exists partsupp
    """

    sql """
    CREATE TABLE IF NOT EXISTS partsupp (
      ps_partkey     INTEGER NOT NULL,
      ps_suppkey     INTEGER NOT NULL,
      ps_availqty    INTEGER NOT NULL,
      ps_supplycost  DECIMALV3(15,2)  NOT NULL,
      ps_comment     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(ps_partkey, ps_suppkey)
    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx'),
    (6, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-13', '2023-12-13', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
    """

    sql """
    insert into orders values
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (6, 2, 'o', 1.2, '2023-12-13', 'c','d',2, 'mi'),
    (6, 2, 'o', 1.2, '2023-12-13', 'c','d',2, 'mi'),
    (6, 2, 'o', 1.2, '2023-12-13', 'c','d',2, 'mi'),
    (6, 2, 'o', 1.2, '2023-12-13', 'c','d',2, 'mi'),
    (6, 2, 'o', 1.2, '2023-12-13', 'c','d',2, 'mi'),
    (6, 2, 'o', 1.2, '2023-12-13', 'c','d',2, 'mi'),
    (6, 2, 'o', 1.2, '2023-12-13', 'c','d',2, 'mi');  
    """

    sql """
    insert into partsupp values
    (2, 3, 9, 5.01, 'supply1'),
    (4, 3, 9, 10.01, 'supply2'),
    (2, 4, 9, 15.01, 'supply3'),
    (3, 3, 9, 20.01, 'supply4');
    """

    sql """analyze table partsupp with sync"""
    sql """analyze table lineitem with sync"""
    sql """analyze table orders with sync"""

    sql """alter table orders modify column O_COMMENT set stats ('row_count'='23');"""
    sql """alter table lineitem modify column l_comment set stats ('row_count'='5');"""
    sql """alter table partsupp modify column ps_comment set stats ('row_count'='1');"""

    // limit can not ensure the return data is stable, so we compare the size before and after the rewrite.
    // each partition data size is different, so this check is effective
    def checkRewriteSize = { query_sql, expect_size ->
        def beforeSize = sql """${query_sql}"""
        assert beforeSize.size() == expect_size
    }

    //  limit + filter(project) + aggregate
    def mv1_0 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 8 offset 1;
            """
    def query1_0 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 4 offset 2;
            """
    def result1_0 = sql """${query1_0}"""
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    checkRewriteSize(query1_0, result1_0.size())
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""

    def mv1_1 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 8 offset 1;
            """
    def query1_1 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-09'
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 4 offset 2;
            """
    // the filter in query and view is different, shoule fail
    async_mv_rewrite_fail(db, mv1_1, query1_1, "mv1_1")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""

    def mv1_2 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 8 offset 1;
            """
    def query1_2 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 4 offset 12;
            """
    // limit and offset in query is larger than the view, should fail
    async_mv_rewrite_fail(db, mv1_2, query1_2, "mv1_2")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""


    //  limit + aggregate
    def mv2_0 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 8 offset 1;
            """
    def query2_0 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 4 offset 2;
            """
    def result2_0 = sql """${query2_0}"""
    async_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    checkRewriteSize(query2_0, result2_0.size())
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 8 offset 1;
            """
    def query2_1 =
            """
            select
            o_orderdate,
            count(o_shippriority),
            count(o_comment),
            l_orderkey,
            count(l_partkey)
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by o_orderdate, l_orderkey
            order by o_orderdate, l_orderkey
            limit 4 offset 12;
            """
    async_mv_rewrite_fail(db, mv2_1, query2_1, "mv2_1")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""



    //  limit + filter(project) + join
    def mv3_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            limit 4 offset 2;
            """
    def query3_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            limit 2 offset 3;
            """
    def result3_0 = sql """${query3_0}"""
    async_mv_rewrite_success(db, mv3_0, query3_0, "mv3_0")
    checkRewriteSize(query3_0, result3_0.size())
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    def mv3_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            limit 4 offset 2;
            """
    def query3_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            limit 2 offset 5;
            """
    async_mv_rewrite_fail(db, mv3_1, query3_1, "mv3_1")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    def mv3_2 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-08'
            limit 4 offset 2;
            """
    def query3_2 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderdate > '2023-12-09'
            limit 4 offset 2;
            """
    async_mv_rewrite_fail(db, mv3_2, query3_2, "mv3_2")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_2"""


    //  limit + join
    def mv4_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            limit 4 offset 2;
            """
    def query4_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            limit 2 offset 3;
            """
    def result4_0 = sql """${query4_0}"""
    async_mv_rewrite_success(db, mv4_0, query4_0, "mv4_0")
    checkRewriteSize(query4_0, result4_0.size())
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    def mv4_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            limit 4 offset 2;
            """
    def query4_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            limit 2 offset 5;
            """
    async_mv_rewrite_fail(db, mv4_1, query4_1, "mv4_1")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_1"""


    //  limit + filter(project) + scan
    def mv5_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderdate > '2023-12-08'
            limit 4 offset 2;
            """
    def query5_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderdate > '2023-12-08'
            limit 2 offset 3;
            """
    def result5_0 = sql """${query5_0}"""
    async_mv_rewrite_success(db, mv5_0, query5_0, "mv5_0")
    checkRewriteSize(query5_0, result5_0.size())
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""


    def mv5_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderdate > '2023-12-08'
            limit 4 offset 2;
            """
    def query5_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderdate > '2023-12-08'
            limit 2 offset 5;
            """
    async_mv_rewrite_success(db, mv5_1, query5_1, "mv5_1")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_1"""


    def mv5_2 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderdate > '2023-12-08'
            limit 4 offset 2;
            """
    def query5_2 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderdate > '2023-12-09'
            limit 4 offset 2;
            """
    async_mv_rewrite_success(db, mv5_2, query5_2, "mv5_2")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_2"""


    //  limit + scan
    def mv6_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            limit 4 offset 2;
            """
    def query6_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            limit 2 offset 3;
            """
    def result6_0 = sql """${query6_0}"""
    async_mv_rewrite_success(db, mv6_0, query6_0, "mv6_0")
    checkRewriteSize(query6_0, result6_0.size())
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_0"""


    def mv6_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            limit 4 offset 2;
            """
    def query6_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            limit 2 offset 5;
            """
    async_mv_rewrite_success(db, mv6_1, query6_1, "mv6_1")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_1"""

}
