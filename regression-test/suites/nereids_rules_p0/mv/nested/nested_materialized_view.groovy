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

suite("nested_materialized_view") {


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

    // ssb_sf1_p1 is writted to test unique key table merge correctly.
    // It creates unique key table and sets bucket num to 1 in order to make sure that
    // many rowsets will be created during loading and then the merge process will be triggered.

    def tables = ["customer", "lineorder", "part", "date", "supplier"]
    def columns = ["""c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,no_use""",
                   """lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,
                    lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,
                    lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy""",
                   """p_partkey,p_name,p_mfgr,p_category,p_brand,p_color,p_type,p_size,p_container,p_dummy""",
                   """d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,
                    d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,
                    d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl,d_dummy""",
                   """s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone,s_dummy"""]

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text
        sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    }
    def i = 0
    for (String tableName in tables) {
        streamLoad {
            // a default db 'regression_test' is specified in
            // ${DORIS_HOME}/conf/regression-conf.groovy
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', columns[i]


            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/regression/ssb/sf1/${tableName}.tbl.gz"""

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
        i++
    }
    sql """ sync """


    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_materialized_view_nest_rewrite = true"

    def create_mtmv = { db_name, mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS ${mv_sql}
        """

        def job_name = getJobName(db_name, mv_name);
        waitingMTMVTaskFinished(job_name)
    }

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
      o_comment        VARCHAR(79) NOT NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate) (PARTITION `day_2` VALUES LESS THAN ('2023-12-30'))
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
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
    PARTITION BY RANGE(l_shipdate) (PARTITION `day_1` VALUES LESS THAN ('2023-12-30'))
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

    sql """ insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
    """

    sql """
    insert into orders values
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi');
    """

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    // simple nested materialized view
    def mv1_0_inner_mv = """
            select
            l_linenumber,
            o_custkey,
            o_orderkey,
            o_orderstatus,
            l_partkey,
            l_suppkey,
            l_orderkey
            from lineitem
            inner join orders on lineitem.l_orderkey = orders.o_orderkey;
    """

    def mv1_0 =
            """
            select
            l_linenumber,
            o_custkey,
            o_orderkey,
            o_orderstatus,
            l_partkey,
            l_suppkey,
            l_orderkey,
            ps_availqty
            from mv1_0_inner_mv
            inner join partsupp on l_partkey = ps_partkey AND l_suppkey = ps_suppkey;
            """
    def query1_0 = """
            select lineitem.l_linenumber
            from lineitem
            inner join orders on l_orderkey = o_orderkey
            inner join partsupp on  l_partkey = ps_partkey AND l_suppkey = ps_suppkey
            where o_orderstatus = 'o'
            """
    order_qt_query1_0_before "${query1_0}"
    create_mtmv(db, "mv1_0_inner_mv", mv1_0_inner_mv)
    check_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0_inner_mv"""
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    sql "SET enable_materialized_view_nest_rewrite = false"

    order_qt_query1_1_before "${query1_0}"
    create_mtmv(db, "mv1_0_inner_mv", mv1_0_inner_mv)
    check_mv_rewrite_fail(db, mv1_0, query1_0, "mv1_0")

    explain {
        sql("${query1_0}")
        contains("mv1_0_inner_mv(mv1_0_inner_mv)")
    }
    order_qt_query1_1_after "${query1_0}"


    // complex nest mv rewrite
    create_mtmv(db, "mv1_a", """
    select
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value1
    FROM
        lineorder
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19930423
    group by
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate;""")

    create_mtmv(db, "mv2_a", """
    select
        lo_custkey,
        lo_orderdate,
        sum(lo_revenue * lo_discount) as sum_value2
    FROM
        lineorder
            inner join customer on lo_custkey = c_custkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19930423
    group by
        lo_custkey,
        lo_orderdate;""")

    create_mtmv(db, "mv4_a", """
    select
        lo_partkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value4
    FROM
        lineorder
            inner join supplier on lo_suppkey = s_suppkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19930423
    group by
        lo_partkey,
        lo_orderdate;""")

    create_mtmv(db, "mv_all_6_a", """
    select
  '测试1' as nm,
  '测试2' as t_nm,
  t1.sum_value1,
  t1.sum_value1 - t2.sum_value2,
  t1.sum_value1 - t3.sum_value4,
  t1.sum_value1 - t4.sum_value4,
  t1.lo_custkey,
  t5.p_name,
  t6.d_yearmonthnum
from
  mv1_a t1
  left join mv2_a t2 on t1.lo_custkey = t2.lo_custkey
  and t1.lo_orderdate = t2.lo_orderdate
  left join mv4_a t3 on t1.lo_partkey = t3.lo_partkey
  and t1.lo_orderdate = t3.lo_orderdate
  left join mv4_a t4 on t1.lo_partkey = t4.lo_partkey
  and t1.lo_orderdate = t4.lo_orderdate
  left join part t5 on t1.lo_partkey = t5.p_partkey
  and t5.p_name = 'forest chiffon'
  left join date t6 on t1.lo_orderdate = t6.d_datekey
  and t6.d_sellingseason = 'Spring';
    """)

    create_mtmv(db, "mv1_b", """
select
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value1
    FROM
        lineorder
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19980421
    group by
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate;""")

    create_mtmv(db, "mv2_b", """
    select
        lo_custkey,
        lo_orderdate,
        sum(lo_revenue * lo_discount) as sum_value2
    FROM
        lineorder
            inner join customer on lo_custkey = c_custkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19980421
    group by
        lo_custkey,
        lo_orderdate;""")

    create_mtmv(db, "mv4_b", """
    select
        lo_partkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value4
    FROM
        lineorder
            inner join supplier on lo_suppkey = s_suppkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19980421
    group by
        lo_partkey,
        lo_orderdate;""")

    create_mtmv(db, "mv_all_6_b", """
    select
  '测试1' as nm,
  '测试2' as t_nm,
  t1.sum_value1,
  t1.sum_value1 - t2.sum_value2,
  t1.sum_value1 - t3.sum_value4,
  t1.sum_value1 - t4.sum_value4,
  t1.lo_custkey,
  t5.p_name,
  t6.d_yearmonthnum
from
  mv1_b t1
  left join mv2_b t2 on t1.lo_custkey = t2.lo_custkey
  and t1.lo_orderdate = t2.lo_orderdate
  left join mv4_b t3 on t1.lo_partkey = t3.lo_partkey
  and t1.lo_orderdate = t3.lo_orderdate
  left join mv4_b t4 on t1.lo_partkey = t4.lo_partkey
  and t1.lo_orderdate = t4.lo_orderdate
  left join part t5 on t1.lo_partkey = t5.p_partkey
  and t5.p_name = 'forest chiffon'
  left join date t6 on t1.lo_orderdate = t6.d_datekey
  and t6.d_sellingseason = 'Spring';
    """)


    create_mtmv(db, "mv1_c", """
    select
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value1
    FROM
        lineorder
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19940413
    group by
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate;""")

    create_mtmv(db, "mv2_c", """
    select
        lo_custkey,
        lo_orderdate,
        sum(lo_revenue * lo_discount) as sum_value2
    FROM
        lineorder
            inner join customer on lo_custkey = c_custkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19940413
    group by
        lo_custkey,
        lo_orderdate;""")

    create_mtmv(db, "mv4_c", """
    select
        lo_partkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value4
    FROM
        lineorder
            inner join supplier on lo_suppkey = s_suppkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19940413
    group by
        lo_partkey,
        lo_orderdate;""")

    create_mtmv(db, "mv_all_6_c", """
    select
  '测试1' as nm,
  '测试2' as t_nm,
  t1.sum_value1,
  t1.sum_value1 - t2.sum_value2,
  t1.sum_value1 - t3.sum_value4,
  t1.sum_value1 - t4.sum_value4,
  t1.lo_custkey,
  t5.p_name,
  t6.d_yearmonthnum
from
  mv1_c t1
  left join mv2_c t2 on t1.lo_custkey = t2.lo_custkey
  and t1.lo_orderdate = t2.lo_orderdate
  left join mv4_c t3 on t1.lo_partkey = t3.lo_partkey
  and t1.lo_orderdate = t3.lo_orderdate
  left join mv4_c t4 on t1.lo_partkey = t4.lo_partkey
  and t1.lo_orderdate = t4.lo_orderdate
  left join part t5 on t1.lo_partkey = t5.p_partkey
  and t5.p_name = 'forest chiffon'
  left join date t6 on t1.lo_orderdate = t6.d_datekey
  and t6.d_sellingseason = 'Spring';
    """)


    create_mtmv(db, "mv1_d", """
    select
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value1
    FROM
        lineorder
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19940218
    group by
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate;""")

    create_mtmv(db, "mv2_d", """
    select
        lo_custkey,
        lo_orderdate,
        sum(lo_revenue * lo_discount) as sum_value2
    FROM
        lineorder
            inner join customer on lo_custkey = c_custkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19940218
    group by
        lo_custkey,
        lo_orderdate;""")

    create_mtmv(db, "mv4_d", """
    select
        lo_partkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value4
    FROM
        lineorder
            inner join supplier on lo_suppkey = s_suppkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = 19940218
    group by
        lo_partkey,
        lo_orderdate;""")

    create_mtmv(db, "mv_all_6_d", """
  select
  '测试1' as nm,
  '测试2' as t_nm,
  t1.sum_value1,
  t1.sum_value1 - t2.sum_value2,
  t1.sum_value1 - t3.sum_value4,
  t1.sum_value1 - t4.sum_value4,
  t1.lo_custkey,
  t5.p_name,
  t6.d_yearmonthnum
from
  mv1_d t1
  left join mv2_d t2 on t1.lo_custkey = t2.lo_custkey
  and t1.lo_orderdate = t2.lo_orderdate
  left join mv4_d t3 on t1.lo_partkey = t3.lo_partkey
  and t1.lo_orderdate = t3.lo_orderdate
  left join mv4_d t4 on t1.lo_partkey = t4.lo_partkey
  and t1.lo_orderdate = t4.lo_orderdate
  left join part t5 on t1.lo_partkey = t5.p_partkey
  and t5.p_name = 'forest chiffon'
  left join date t6 on t1.lo_orderdate = t6.d_datekey
  and t6.d_sellingseason = 'Spring';
    """)

    def query2_0 = """
select * from ( 
    select
      '测试1' as nm,
      '测试2' as t_nm,
      t1.sum_value1,
      t1.sum_value1 - t2.sum_value2,
      t1.sum_value1 - t3.sum_value3,
      t1.sum_value1 - t4.sum_value4,
      t1.lo_custkey,
      t5.p_name,
      t6.d_yearmonthnum
    from
      (
        select
          lo_custkey,
          lo_partkey,
          lo_orderkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value1
        FROM
          lineorder
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19930423
        group by
          lo_custkey,
          lo_partkey,
          lo_orderkey,
          lo_orderdate
      ) t1
      left join (
        select
          lo_custkey,
          lo_orderdate,
          sum(lo_revenue * lo_discount) as sum_value2
        FROM
          lineorder
          inner join customer on lo_custkey = c_custkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19930423
        group by
          lo_custkey,
          lo_orderdate
      ) t2 on t1.lo_custkey = t2.lo_custkey
      and t1.lo_orderdate = t2.lo_orderdate
      left join (
        select
          lo_partkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value3
        FROM
          lineorder
          inner join supplier on lo_suppkey = s_suppkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19930423
        group by
          lo_partkey,
          lo_orderdate
      ) t3 on t1.lo_partkey = t3.lo_partkey
      and t1.lo_orderdate = t3.lo_orderdate
      left join (
        select
          lo_partkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value4
        FROM
          lineorder
          inner join supplier on lo_suppkey = s_suppkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19930423
        group by
          lo_partkey,
          lo_orderdate
      ) t4 on t1.lo_partkey = t4.lo_partkey
      and t1.lo_orderdate = t4.lo_orderdate
      left join part t5 on t1.lo_partkey = t5.p_partkey
      and t5.p_name = 'forest chiffon'
      left join date t6 on t1.lo_orderdate = t6.d_datekey
      and t6.d_sellingseason = 'Spring'
    union all
    select
      '测试1' as nm,
      '测试2' as t_nm,
      t1.sum_value1,
      t1.sum_value1 - t2.sum_value2,
      t1.sum_value1 - t3.sum_value3,
      t1.sum_value1 - t4.sum_value4,
      t1.lo_custkey,
      t5.p_name,
      t6.d_yearmonthnum
    from
      (
        select
          lo_custkey,
          lo_partkey,
          lo_orderkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value1
        FROM
          lineorder
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19980421
        group by
          lo_custkey,
          lo_partkey,
          lo_orderkey,
          lo_orderdate
      ) t1
      left join (
        select
          lo_custkey,
          lo_orderdate,
          sum(lo_revenue * lo_discount) as sum_value2
        FROM
          lineorder
          inner join customer on lo_custkey = c_custkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19980421
        group by
          lo_custkey,
          lo_orderdate
      ) t2 on t1.lo_custkey = t2.lo_custkey
      and t1.lo_orderdate = t2.lo_orderdate
      left join (
        select
          lo_partkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value3
        FROM
          lineorder
          inner join supplier on lo_suppkey = s_suppkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19980421
        group by
          lo_partkey,
          lo_orderdate
      ) t3 on t1.lo_partkey = t3.lo_partkey
      and t1.lo_orderdate = t3.lo_orderdate
      left join (
        select
          lo_partkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value4
        FROM
          lineorder
          inner join supplier on lo_suppkey = s_suppkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19980421
        group by
          lo_partkey,
          lo_orderdate
      ) t4 on t1.lo_partkey = t4.lo_partkey
      and t1.lo_orderdate = t4.lo_orderdate
      left join part t5 on t1.lo_partkey = t5.p_partkey
      and t5.p_name = 'forest chiffon'
      left join date t6 on t1.lo_orderdate = t6.d_datekey
      and t6.d_sellingseason = 'Spring'
    union ALL
    select
      '测试1' as nm,
      '测试2' as t_nm,
      t1.sum_value1,
      t1.sum_value1 - t2.sum_value2,
      t1.sum_value1 - t3.sum_value3,
      t1.sum_value1 - t4.sum_value4,
      t1.lo_custkey,
      t5.p_name,
      t6.d_yearmonthnum
    from
      (
        select
          lo_custkey,
          lo_partkey,
          lo_orderkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value1
        FROM
          lineorder
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19940413
        group by
          lo_custkey,
          lo_partkey,
          lo_orderkey,
          lo_orderdate
      ) t1
      left join (
        select
          lo_custkey,
          lo_orderdate,
          sum(lo_revenue * lo_discount) as sum_value2
        FROM
          lineorder
          inner join customer on lo_custkey = c_custkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19940413
        group by
          lo_custkey,
          lo_orderdate
      ) t2 on t1.lo_custkey = t2.lo_custkey
      and t1.lo_orderdate = t2.lo_orderdate
      left join (
        select
          lo_partkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value3
        FROM
          lineorder
          inner join supplier on lo_suppkey = s_suppkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19940413
        group by
          lo_partkey,
          lo_orderdate
      ) t3 on t1.lo_partkey = t3.lo_partkey
      and t1.lo_orderdate = t3.lo_orderdate
      left join (
        select
          lo_partkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value4
        FROM
          lineorder
          inner join supplier on lo_suppkey = s_suppkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19940413
        group by
          lo_partkey,
          lo_orderdate
      ) t4 on t1.lo_partkey = t4.lo_partkey
      and t1.lo_orderdate = t4.lo_orderdate
      left join part t5 on t1.lo_partkey = t5.p_partkey
      and t5.p_name = 'forest chiffon'
      left join date t6 on t1.lo_orderdate = t6.d_datekey
      and t6.d_sellingseason = 'Spring'
    UNION ALL
    select
      '测试1' as nm,
      '测试2' as t_nm,
      t1.sum_value1,
      t1.sum_value1 - t2.sum_value2,
      t1.sum_value1 - t3.sum_value3,
      t1.sum_value1 - t4.sum_value4,
      t1.lo_custkey,
      t5.p_name,
      t6.d_yearmonthnum
    from
      (
        select
          lo_custkey,
          lo_partkey,
          lo_orderkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value1
        FROM
          lineorder
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19940218
        group by
          lo_custkey,
          lo_partkey,
          lo_orderkey,
          lo_orderdate
      ) t1
      left join (
        select
          lo_custkey,
          lo_orderdate,
          sum(lo_revenue * lo_discount) as sum_value2
        FROM
          lineorder
          inner join customer on lo_custkey = c_custkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19940218
        group by
          lo_custkey,
          lo_orderdate
      ) t2 on t1.lo_custkey = t2.lo_custkey
      and t1.lo_orderdate = t2.lo_orderdate
      left join (
        select
          lo_partkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value3
        FROM
          lineorder
          inner join supplier on lo_suppkey = s_suppkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19940218
        group by
          lo_partkey,
          lo_orderdate
      ) t3 on t1.lo_partkey = t3.lo_partkey
      and t1.lo_orderdate = t3.lo_orderdate
      left join (
        select
          lo_partkey,
          lo_orderdate,
          sum(lo_extendedprice * lo_discount) as sum_value4
        FROM
          lineorder
          inner join supplier on lo_suppkey = s_suppkey
          inner join date on lo_orderdate = d_datekey
        where
          d_daynuminweek > 0
          and lo_orderdate = 19940218
        group by
          lo_partkey,
          lo_orderdate
      ) t4 on t1.lo_partkey = t4.lo_partkey
      and t1.lo_orderdate = t4.lo_orderdate
      left join part t5 on t1.lo_partkey = t5.p_partkey
      and t5.p_name = 'forest chiffon'
      left join date t6 on t1.lo_orderdate = t6.d_datekey
      and t6.d_sellingseason = 'Spring'
) t order by 1,2,3,4,5,6,7,8,9;
   """

    sql "SET enable_materialized_view_rewrite= true"
    sql "SET enable_materialized_view_nest_rewrite = true"
    explain {
        sql("${query2_0}")
        check {result ->
            result.contains("mv_all_6_a(mv_all_6_a)") && result.contains("mv_all_6_b(mv_all_6_b)")
            && result.contains("mv_all_6_c(mv_all_6_c)") && result.contains("mv_all_6_d(mv_all_6_d)")
        }
    }
    // Compare result when before and after mv rewrite
    compare_res(query2_0)
}
