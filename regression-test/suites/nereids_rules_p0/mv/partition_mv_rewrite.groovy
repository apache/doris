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

suite("partition_mv_rewrite") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"
    // tmp disable to rewrite, will be removed in the future
    sql "SET disable_nereids_rules = 'INFER_PREDICATES, ELIMINATE_OUTER_JOIN'"

    sql """
    drop table if exists orders
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      o_orderkey       integer not null,
      o_custkey        integer not null,
      o_orderstatus    char(1) not null,
      o_totalprice     decimalv3(15,2) not null,
      o_orderdate      date not null,
      o_orderpriority  char(15) not null,  
      o_clerk          char(15) not null, 
      o_shippriority   integer not null,
      o_comment        varchar(79) not null
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate)(
    FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY
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
      l_orderkey    integer not null,
      l_partkey     integer not null,
      l_suppkey     integer not null,
      l_linenumber  integer not null,
      l_quantity    decimalv3(15,2) not null,
      l_extendedprice  decimalv3(15,2) not null,
      l_discount    decimalv3(15,2) not null,
      l_tax         decimalv3(15,2) not null,
      l_returnflag  char(1) not null,
      l_linestatus  char(1) not null,
      l_shipdate    date not null,
      l_commitdate  date not null,
      l_receiptdate date not null,
      l_shipinstruct char(25) not null,
      l_shipmode     char(10) not null,
      l_comment      varchar(44) not null
    )
    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
    PARTITION BY RANGE(l_shipdate) 
    (FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql"""
    insert into orders values 
    (1, 1, 'ok', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'); 
    """

    sql """
    insert into lineitem values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """


    def mv_sql_def = "select l_shipdate, o_orderdate, l_partkey,\n" +
            "l_suppkey, sum(o_totalprice) as sum_total\n" +
            "from lineitem\n" +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate\n" +
            "group by\n" +
            "l_shipdate,\n" +
            "o_orderdate,\n" +
            "l_partkey,\n" +
            "l_suppkey;"

    sql """
    CREATE MATERIALIZED VIEW mv1_0
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        partition by(l_shipdate)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        ${mv_sql_def}
    """

    def all_partition_sql = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total\n" +
            "from lineitem\n" +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate\n" +
            "group by\n" +
            "l_shipdate,\n" +
            "o_orderdate,\n" +
            "l_partkey,\n" +
            "l_suppkey;"


    def partition_sql = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total\n" +
            "from lineitem\n" +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate\n" +
            "where (l_shipdate>= '2023-10-18' and l_shipdate <= '2023-10-19')\n" +
            "group by\n" +
            "l_shipdate,\n" +
            "o_orderdate,\n" +
            "l_partkey,\n" +
            "l_suppkey;"

    def check_rewrite = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            contains "(${mv_name})"
        }
    }

    def check_not_match = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            notContains "(${mv_name})"
        }
    }

    waitingMTMVTaskFinished(getJobName(db, "mv1_0"))

    check_rewrite(mv_sql_def, all_partition_sql, "mv1_0")
    check_rewrite(mv_sql_def, partition_sql, "mv1_0")

    // partition is invalid, so can use partition 2023-10-17 to rewrite
    sql """
    insert into lineitem values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """

    // wait partition is invalid
    sleep(3000)
    check_not_match(mv_sql_def, all_partition_sql, "mv1_0")
    check_rewrite(mv_sql_def, partition_sql, "mv1_0")

    sql """
    Refresh MATERIALIZED VIEW mv1_0;
    """
    waitingMTMVTaskFinished(getJobName(db, "mv1_0"))

    check_rewrite(mv_sql_def, all_partition_sql, "mv1_0")
    check_rewrite(mv_sql_def, partition_sql, "mv1_0")
}
