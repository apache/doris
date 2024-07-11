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

// Most of the cases are copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

// syntax error:
// q06 q13 q15
// Test 23 suites, failed 3 suites

// Note: To filter out tables from sql files, use the following one-liner comamnd
// sed -nr 's/.*tables: (.*)$/\1/gp' /path/to/*.sql | sed -nr 's/,/\n/gp' | sort | uniq
suite("mv_tpch_test") {
    def tables = [customer: ["c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment,temp"],
                  lineitem: ["l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,temp"],
                  nation  : ["n_nationkey, n_name, n_regionkey, n_comment, temp"],
                  orders  : ["o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, temp"],
                  part    : ["p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp"],
                  partsupp: ["ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment,temp"],
                  region  : ["r_regionkey, r_name, r_comment,temp"],
                  supplier: ["s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment,temp"]]

    tables.forEach { tableName, columns ->
        sql new File("""${context.file.parent}/ddl/${tableName}.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName}_delete.sql""").text
        streamLoad {
            // a default db 'regression_test' is specified in
            // ${DORIS_HOME}/conf/regression-conf.groovy
            table "${tableName}"

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', "${columns[0]}"

            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/regression/tpch/sf0.1/${tableName}.tbl.gz"""

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

        sql """ ANALYZE TABLE $tableName WITH SYNC """
    }

    def table = "revenue1"
    sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    sql """ sync """

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    // mv8 Nereids cost too much time ( > 5s )
    sql "SET enable_nereids_timeout=false"

    def mv1 = """
            SELECT
              l_returnflag,
              l_linestatus,
              sum(l_quantity)                                       AS sum_qty,
              sum(l_extendedprice)                                  AS sum_base_price,
              sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
              avg(l_quantity)                                       AS avg_qty,
              avg(l_extendedprice)                                  AS avg_price,
              avg(l_discount)                                       AS avg_disc,
              count(*)                                              AS count_order
            FROM
              lineitem
            WHERE
              l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
            GROUP BY
            l_returnflag,
            l_linestatus
            ORDER BY
            l_returnflag,
            l_linestatus;
    """
    def query1 = """
            SELECT
              l_returnflag,
              l_linestatus,
              sum(l_quantity)                                       AS sum_qty,
              sum(l_extendedprice)                                  AS sum_base_price,
              sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
              avg(l_quantity)                                       AS avg_qty,
              avg(l_extendedprice)                                  AS avg_price,
              avg(l_discount)                                       AS avg_disc,
              count(*)                                              AS count_order
            FROM
              lineitem
            WHERE
              l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
            GROUP BY
            l_returnflag,
            l_linestatus
            ORDER BY
            l_returnflag,
            l_linestatus;
    """
    order_qt_query1_before "${query1}"
    check_mv_rewrite_success(db, mv1, query1, "mv1")
    order_qt_query1_after "${query1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1"""


    def mv2 = """
            SELECT
              s_acctbal,
              s_name,
              n_name,
              p_partkey,
              p_mfgr,
              s_address,
              s_phone,
              s_comment
            FROM
              part,
              supplier,
              partsupp,
              nation,
              region
            WHERE
              p_partkey = ps_partkey
              AND s_suppkey = ps_suppkey
              AND p_size = 15
              AND p_type LIKE '%BRASS'
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'EUROPE'
              AND ps_supplycost = (
                SELECT min(ps_supplycost)
                FROM
                  partsupp, supplier,
                  nation, region
                WHERE
                  p_partkey = ps_partkey
                  AND s_suppkey = ps_suppkey
                  AND s_nationkey = n_nationkey
                  AND n_regionkey = r_regionkey
                  AND r_name = 'EUROPE'
              )
            ORDER BY
              s_acctbal DESC,
              n_name,
              s_name,
              p_partkey
            LIMIT 100;
    """
    def query2 = """
             SELECT
              s_acctbal,
              s_name,
              n_name,
              p_partkey,
              p_mfgr,
              s_address,
              s_phone,
              s_comment
            FROM
              part,
              supplier,
              partsupp,
              nation,
              region
            WHERE
              p_partkey = ps_partkey
              AND s_suppkey = ps_suppkey
              AND p_size = 15
              AND p_type LIKE '%BRASS'
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'EUROPE'
              AND ps_supplycost = (
                SELECT min(ps_supplycost)
                FROM
                  partsupp, supplier,
                  nation, region
                WHERE
                  p_partkey = ps_partkey
                  AND s_suppkey = ps_suppkey
                  AND s_nationkey = n_nationkey
                  AND n_regionkey = r_regionkey
                  AND r_name = 'EUROPE'
              )
            ORDER BY
              s_acctbal DESC,
              n_name,
              s_name,
              p_partkey
            LIMIT 100;
    """
    // contains limit, doesn't support now
    order_qt_query2_before "${query2}"
    check_mv_rewrite_fail(db, mv2, query2, "mv2")
    order_qt_query2_after "${query2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2"""



    def mv3 = """
            SELECT
              l_orderkey,
              sum(l_extendedprice * (1 - l_discount)) AS revenue,
              o_orderdate,
              o_shippriority
            FROM
              customer,
              orders,
              lineitem
            WHERE
              c_mktsegment = 'BUILDING'
              AND c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate < DATE '1995-03-15'
              AND l_shipdate > DATE '1995-03-15'
            GROUP BY
              l_orderkey,
              o_orderdate,
              o_shippriority
            ORDER BY
              revenue DESC,
              o_orderdate
            LIMIT 10;
    """
    def query3 = """
            SELECT
              l_orderkey,
              sum(l_extendedprice * (1 - l_discount)) AS revenue,
              o_orderdate,
              o_shippriority
            FROM
              customer,
              orders,
              lineitem
            WHERE
              c_mktsegment = 'BUILDING'
              AND c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate < DATE '1995-03-15'
              AND l_shipdate > DATE '1995-03-15'
            GROUP BY
              l_orderkey,
              o_orderdate,
              o_shippriority
            ORDER BY
              revenue DESC,
              o_orderdate
            LIMIT 10;   
    """
    // contains limit, doesn't support now
    order_qt_query3_before "${query3}"
    check_mv_rewrite_fail(db, mv3, query3, "mv3")
    order_qt_query3_after "${query3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3"""


    def mv4 = """
            SELECT
              o_orderpriority,
              count(*) AS order_count
            FROM orders
            WHERE
              o_orderdate >= DATE '1993-07-01'
              AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
            AND EXISTS (
            SELECT *
            FROM lineitem
            WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
            )
            GROUP BY
            o_orderpriority
            ORDER BY
            o_orderpriority        
    """
    def query4 = """
            SELECT
              o_orderpriority,
              count(*) AS order_count
            FROM orders
            WHERE
              o_orderdate >= DATE '1993-07-01'
              AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
            AND EXISTS (
            SELECT *
            FROM lineitem
            WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
            )
            GROUP BY
            o_orderpriority
            ORDER BY
            o_orderpriority             
    """
    // contains subquery, doesn't support now
    order_qt_query4_before "${query4}"
    check_mv_rewrite_success(db, mv4, query4, "mv4")
    order_qt_query4_after "${query4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4"""


    def mv5 = """
            SELECT
              n_name,
              sum(l_extendedprice * (1 - l_discount)) AS revenue
            FROM
              customer,
              orders,
              lineitem,
              supplier,
              nation,
              region
            WHERE
              c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND l_suppkey = s_suppkey
              AND c_nationkey = s_nationkey
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'ASIA'
              AND o_orderdate >= DATE '1994-01-01'
              AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            GROUP BY
            n_name
            ORDER BY
            revenue DESC        
    """
    def query5 = """
            SELECT
              n_name,
              sum(l_extendedprice * (1 - l_discount)) AS revenue
            FROM
              customer,
              orders,
              lineitem,
              supplier,
              nation,
              region
            WHERE
              c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND l_suppkey = s_suppkey
              AND c_nationkey = s_nationkey
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'ASIA'
              AND o_orderdate >= DATE '1994-01-01'
              AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            GROUP BY
            n_name
            ORDER BY
            revenue DESC            
    """
    order_qt_query5_before "${query5}"
    check_mv_rewrite_success(db, mv5, query5, "mv5")
    order_qt_query5_after "${query5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5"""


    def mv6 = """
            SELECT sum(l_extendedprice * l_discount) AS revenue
            FROM
              lineitem
            WHERE
              l_shipdate >= DATE '1994-01-01'
              AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            AND l_discount BETWEEN 0.06 - 0.01 AND .06 + 0.01
            AND l_quantity < 24
"""
    def query6 = """
            SELECT sum(l_extendedprice * l_discount) AS revenue
            FROM
              lineitem
            WHERE
              l_shipdate >= DATE '1994-01-01'
              AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            AND l_discount BETWEEN 0.06 - 0.01 AND .06 + 0.01
            AND l_quantity < 24            
    """
    order_qt_query6_before "${query6}"
    check_mv_rewrite_success(db, mv6, query6, "mv6")
    order_qt_query6_after "${query6}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6"""


    def mv7 = """
            SELECT
              supp_nation,
              cust_nation,
              l_year,
              sum(volume) AS revenue
            FROM (
                   SELECT
                     n1.n_name                          AS supp_nation,
                     n2.n_name                          AS cust_nation,
                     extract(YEAR FROM l_shipdate)      AS l_year,
                     l_extendedprice * (1 - l_discount) AS volume
                   FROM
                     supplier,
                     lineitem,
                     orders,
                     customer,
                     nation n1,
                     nation n2
                   WHERE
                     s_suppkey = l_suppkey
                     AND o_orderkey = l_orderkey
                     AND c_custkey = o_custkey
                     AND s_nationkey = n1.n_nationkey
                     AND c_nationkey = n2.n_nationkey
                     AND (
                       (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                       OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                     )
                     AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                 ) AS shipping
            GROUP BY
              supp_nation,
              cust_nation,
              l_year
            ORDER BY
              supp_nation,
              cust_nation,
              l_year           
    """
    def query7 = """
            SELECT
              supp_nation,
              cust_nation,
              l_year,
              sum(volume) AS revenue
            FROM (
                   SELECT
                     n1.n_name                          AS supp_nation,
                     n2.n_name                          AS cust_nation,
                     extract(YEAR FROM l_shipdate)      AS l_year,
                     l_extendedprice * (1 - l_discount) AS volume
                   FROM
                     supplier,
                     lineitem,
                     orders,
                     customer,
                     nation n1,
                     nation n2
                   WHERE
                     s_suppkey = l_suppkey
                     AND o_orderkey = l_orderkey
                     AND c_custkey = o_custkey
                     AND s_nationkey = n1.n_nationkey
                     AND c_nationkey = n2.n_nationkey
                     AND (
                       (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                       OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                     )
                     AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                 ) AS shipping
            GROUP BY
              supp_nation,
              cust_nation,
              l_year
            ORDER BY
              supp_nation,
              cust_nation,
              l_year             
    """
    // contains subquery, doesn't support now
    order_qt_query7_before "${query7}"
    check_mv_rewrite_fail(db, mv7, query7, "mv7")
    order_qt_query7_after "${query7}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7"""


    def mv8 = """
            SELECT
              o_year,
              sum(CASE
                  WHEN nation = 'BRAZIL'
                    THEN volume
                  ELSE 0
                  END) / sum(volume) AS mkt_share
            FROM (
                   SELECT
                     extract(YEAR FROM o_orderdate)     AS o_year,
                     l_extendedprice * (1 - l_discount) AS volume,
                     n2.n_name                          AS nation
                   FROM
                     part,
                     supplier,
                     lineitem,
                     orders,
                     customer,
                     nation n1,
                     nation n2,
                     region
                   WHERE
                     p_partkey = l_partkey
                     AND s_suppkey = l_suppkey
                     AND l_orderkey = o_orderkey
                     AND o_custkey = c_custkey
                     AND c_nationkey = n1.n_nationkey
                     AND n1.n_regionkey = r_regionkey
                     AND r_name = 'AMERICA'
                     AND s_nationkey = n2.n_nationkey
                     AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                     AND p_type = 'ECONOMY ANODIZED STEEL'
                 ) AS all_nations
            GROUP BY
              o_year
            ORDER BY
              o_year      
    """
    def query8 = """
            SELECT
              o_year,
              sum(CASE
                  WHEN nation = 'BRAZIL'
                    THEN volume
                  ELSE 0
                  END) / sum(volume) AS mkt_share
            FROM (
                   SELECT
                     extract(YEAR FROM o_orderdate)     AS o_year,
                     l_extendedprice * (1 - l_discount) AS volume,
                     n2.n_name                          AS nation
                   FROM
                     part,
                     supplier,
                     lineitem,
                     orders,
                     customer,
                     nation n1,
                     nation n2,
                     region
                   WHERE
                     p_partkey = l_partkey
                     AND s_suppkey = l_suppkey
                     AND l_orderkey = o_orderkey
                     AND o_custkey = c_custkey
                     AND c_nationkey = n1.n_nationkey
                     AND n1.n_regionkey = r_regionkey
                     AND r_name = 'AMERICA'
                     AND s_nationkey = n2.n_nationkey
                     AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                     AND p_type = 'ECONOMY ANODIZED STEEL'
                 ) AS all_nations
            GROUP BY
              o_year
            ORDER BY
              o_year              
    """
    order_qt_query8_before "${query8}"
    check_mv_rewrite_success(db, mv8, query8, "mv8")
    order_qt_query8_after "${query8}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8"""


    def mv9 = """
            SELECT
              nation,
              o_year,
              sum(amount) AS sum_profit
            FROM (
                   SELECT
                     n_name                                                          AS nation,
                     extract(YEAR FROM o_orderdate)                                  AS o_year,
                     l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                   FROM
                     part,
                     supplier,
                     lineitem,
                     partsupp,
                     orders,
                     nation
                   WHERE
                     s_suppkey = l_suppkey
                     AND ps_suppkey = l_suppkey
                     AND ps_partkey = l_partkey
                     AND p_partkey = l_partkey
                     AND o_orderkey = l_orderkey
                     AND s_nationkey = n_nationkey
                     AND p_name LIKE '%green%'
                 ) AS profit
            GROUP BY
              nation,
              o_year
            ORDER BY
              nation,
              o_year DESC   
    """
    def query9 = """
            SELECT
              nation,
              o_year,
              sum(amount) AS sum_profit
            FROM (
                   SELECT
                     n_name                                                          AS nation,
                     extract(YEAR FROM o_orderdate)                                  AS o_year,
                     l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                   FROM
                     part,
                     supplier,
                     lineitem,
                     partsupp,
                     orders,
                     nation
                   WHERE
                     s_suppkey = l_suppkey
                     AND ps_suppkey = l_suppkey
                     AND ps_partkey = l_partkey
                     AND p_partkey = l_partkey
                     AND o_orderkey = l_orderkey
                     AND s_nationkey = n_nationkey
                     AND p_name LIKE '%green%'
                 ) AS profit
            GROUP BY
              nation,
              o_year
            ORDER BY
              nation,
              o_year DESC             
    """
    order_qt_query9_before "${query9}"
    check_mv_rewrite_success(db, mv9, query9, "mv9")
    order_qt_query9_after "${query9}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9"""


    def mv10 = """
            SELECT
              c_custkey,
              c_name,
              sum(l_extendedprice * (1 - l_discount)) AS revenue,
              c_acctbal,
              n_name,
              c_address,
              c_phone,
              c_comment
            FROM
              customer,
              orders,
              lineitem,
              nation
            WHERE
              c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate >= DATE '1993-10-01'
              AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
              AND l_returnflag = 'R'
              AND c_nationkey = n_nationkey
            GROUP BY
              c_custkey,
              c_name,
              c_acctbal,
              c_phone,
              n_name,
              c_address,
              c_comment
            ORDER BY
              revenue DESC
            LIMIT 20  
    """
    def query10 = """
            SELECT
              c_custkey,
              c_name,
              sum(l_extendedprice * (1 - l_discount)) AS revenue,
              c_acctbal,
              n_name,
              c_address,
              c_phone,
              c_comment
            FROM
              customer,
              orders,
              lineitem,
              nation
            WHERE
              c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate >= DATE '1993-10-01'
              AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
              AND l_returnflag = 'R'
              AND c_nationkey = n_nationkey
            GROUP BY
              c_custkey,
              c_name,
              c_acctbal,
              c_phone,
              n_name,
              c_address,
              c_comment
            ORDER BY
              revenue DESC
            LIMIT 20            
    """
    // contains limit, doesn't support now
    order_qt_query10_before "${query10}"
    check_mv_rewrite_fail(db, mv10, query10, "mv10")
    order_qt_query10_after "${query10}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10"""


    def mv11 = """
            SELECT
              ps_partkey,
              sum(ps_supplycost * ps_availqty) AS value
            FROM
              partsupp,
              supplier,
              nation
            WHERE
              ps_suppkey = s_suppkey
              AND s_nationkey = n_nationkey
              AND n_name = 'GERMANY'
            GROUP BY
              ps_partkey
            HAVING
              sum(ps_supplycost * ps_availqty) > (
                SELECT sum(ps_supplycost * ps_availqty) * 0.0001
                FROM
                  partsupp,
                  supplier,
                  nation
                WHERE
                  ps_suppkey = s_suppkey
                  AND s_nationkey = n_nationkey
                  AND n_name = 'GERMANY'
              )
            ORDER BY
              value DESC  
    """
    def query11 = """
            SELECT
              ps_partkey,
              sum(ps_supplycost * ps_availqty) AS value
            FROM
              partsupp,
              supplier,
              nation
            WHERE
              ps_suppkey = s_suppkey
              AND s_nationkey = n_nationkey
              AND n_name = 'GERMANY'
            GROUP BY
              ps_partkey
            HAVING
              sum(ps_supplycost * ps_availqty) > (
                SELECT sum(ps_supplycost * ps_availqty) * 0.0001
                FROM
                  partsupp,
                  supplier,
                  nation
                WHERE
                  ps_suppkey = s_suppkey
                  AND s_nationkey = n_nationkey
                  AND n_name = 'GERMANY'
              )
            ORDER BY
              value DESC             
    """
    // contains subquery, doesn't support now
    order_qt_query11_before "${query11}"
    check_mv_rewrite_fail(db, mv11, query11, "mv11")
    order_qt_query11_after "${query11}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv11"""


    def mv12 = """
            SELECT
              l_shipmode,
              sum(CASE
                  WHEN o_orderpriority = '1-URGENT'
                       OR o_orderpriority = '2-HIGH'
                    THEN 1
                  ELSE 0
                  END) AS high_line_count,
              sum(CASE
                  WHEN o_orderpriority <> '1-URGENT'
                       AND o_orderpriority <> '2-HIGH'
                    THEN 1
                  ELSE 0
                  END) AS low_line_count
            FROM
              orders,
              lineitem
            WHERE
              o_orderkey = l_orderkey
              AND l_shipmode IN ('MAIL', 'SHIP')
              AND l_commitdate < l_receiptdate
              AND l_shipdate < l_commitdate
              AND l_receiptdate >= DATE '1994-01-01'
              AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            GROUP BY
              l_shipmode
            ORDER BY
              l_shipmode
    """
    def query12 = """
            SELECT
              l_shipmode,
              sum(CASE
                  WHEN o_orderpriority = '1-URGENT'
                       OR o_orderpriority = '2-HIGH'
                    THEN 1
                  ELSE 0
                  END) AS high_line_count,
              sum(CASE
                  WHEN o_orderpriority <> '1-URGENT'
                       AND o_orderpriority <> '2-HIGH'
                    THEN 1
                  ELSE 0
                  END) AS low_line_count
            FROM
              orders,
              lineitem
            WHERE
              o_orderkey = l_orderkey
              AND l_shipmode IN ('MAIL', 'SHIP')
              AND l_commitdate < l_receiptdate
              AND l_shipdate < l_commitdate
              AND l_receiptdate >= DATE '1994-01-01'
              AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            GROUP BY
              l_shipmode
            ORDER BY
              l_shipmode            
    """
    order_qt_query12_before "${query12}"
    check_mv_rewrite_success(db, mv12, query12, "mv12")
    order_qt_query12_after "${query12}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv12"""


    def mv13 = """
            SELECT
              c_count,
              count(*) AS custdist
            FROM (
                   SELECT
                     c_custkey,
                     count(o_orderkey) AS c_count
                   FROM
                     customer
                     LEFT OUTER JOIN orders ON
                                              c_custkey = o_custkey
                                              AND o_comment NOT LIKE '%special%requests%'
                   GROUP BY
                     c_custkey
                 ) AS c_orders
            GROUP BY
              c_count
            ORDER BY
              custdist DESC,
              c_count DESC
    """
    def query13 = """
            SELECT
              c_count,
              count(*) AS custdist
            FROM (
                   SELECT
                     c_custkey,
                     count(o_orderkey) AS c_count
                   FROM
                     customer
                     LEFT OUTER JOIN orders ON
                                              c_custkey = o_custkey
                                              AND o_comment NOT LIKE '%special%requests%'
                   GROUP BY
                     c_custkey
                 ) AS c_orders
            GROUP BY
              c_count
            ORDER BY
              custdist DESC,
              c_count DESC            
    """
    // when aggregate rewrite, should only contains one aggregate
    order_qt_query13_before "${query13}"
    check_mv_rewrite_fail(db, mv13, query13, "mv13")
    order_qt_query13_after "${query13}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13"""


    def mv14 = """
            SELECT 100.00 * sum(CASE
                                WHEN p_type LIKE 'PROMO%'
                                  THEN l_extendedprice * (1 - l_discount)
                                ELSE 0
                                END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM
              lineitem,
              part
            WHERE
              l_partkey = p_partkey
              AND l_shipdate >= DATE '1995-09-01'
              AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
    """
    def query14 = """
            SELECT 100.00 * sum(CASE
                                WHEN p_type LIKE 'PROMO%'
                                  THEN l_extendedprice * (1 - l_discount)
                                ELSE 0
                                END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM
              lineitem,
              part
            WHERE
              l_partkey = p_partkey
              AND l_shipdate >= DATE '1995-09-01'
              AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH            
    """
    order_qt_query14_before "${query14}"
    check_mv_rewrite_success(db, mv14, query14, "mv14")
    order_qt_query14_after "${query14}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv14"""


    def mv15 = """
            SELECT
              s_suppkey,
              s_name,
              s_address,
              s_phone,
              total_revenue
            FROM
              supplier,
              revenue1
            WHERE
              s_suppkey = supplier_no
              AND total_revenue = (
                SELECT max(total_revenue)
                FROM
                  revenue1
              )
            ORDER BY
              s_suppkey;
    """
    def query15 = """
            SELECT
              s_suppkey,
              s_name,
              s_address,
              s_phone,
              total_revenue
            FROM
              supplier,
              revenue1
            WHERE
              s_suppkey = supplier_no
              AND total_revenue = (
                SELECT max(total_revenue)
                FROM
                  revenue1
              )
            ORDER BY
              s_suppkey;            
    """
    // revenue1 in materialized view is view, can not create materialized view support now
//    order_qt_query15_before "${query15}"
//    check_mv_rewrite_fail(db, mv15, query15, "mv15")
//    order_qt_query15_after "${query15}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15"""


    def mv16 = """
            SELECT
              p_brand,
              p_type,
              p_size,
              count(DISTINCT ps_suppkey) AS supplier_cnt
            FROM
              partsupp,
              part
            WHERE
              p_partkey = ps_partkey
              AND p_brand <> 'Brand#45'
              AND p_type NOT LIKE 'MEDIUM POLISHED%'
              AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
              AND ps_suppkey NOT IN (
                SELECT s_suppkey
                FROM
                  supplier
                WHERE
                  s_comment LIKE '%Customer%Complaints%'
              )
            GROUP BY
              p_brand,
              p_type,
              p_size
            ORDER BY
              supplier_cnt DESC,
              p_brand,
              p_type,
              p_size
    """
    def query16 = """
            SELECT
              p_brand,
              p_type,
              p_size,
              count(DISTINCT ps_suppkey) AS supplier_cnt
            FROM
              partsupp,
              part
            WHERE
              p_partkey = ps_partkey
              AND p_brand <> 'Brand#45'
              AND p_type NOT LIKE 'MEDIUM POLISHED%'
              AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
              AND ps_suppkey NOT IN (
                SELECT s_suppkey
                FROM
                  supplier
                WHERE
                  s_comment LIKE '%Customer%Complaints%'
              )
            GROUP BY
              p_brand,
              p_type,
              p_size
            ORDER BY
              supplier_cnt DESC,
              p_brand,
              p_type,
              p_size            
    """
    // contains subquery, doesn't support now
    order_qt_query16_before "${query16}"
    check_mv_rewrite_success(db, mv16, query16, "mv16")
    order_qt_query16_after "${query16}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16"""


    def mv17 = """
            SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
            FROM
              lineitem,
              part
            WHERE
              p_partkey = l_partkey
              AND p_brand = 'Brand#23'
              AND p_container = 'MED BOX'
              AND l_quantity < (
                SELECT 0.2 * avg(l_quantity)
                FROM
                  lineitem
                WHERE
                  l_partkey = p_partkey
              )
    """
    def query17 = """
            SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
            FROM
              lineitem,
              part
            WHERE
              p_partkey = l_partkey
              AND p_brand = 'Brand#23'
              AND p_container = 'MED BOX'
              AND l_quantity < (
                SELECT 0.2 * avg(l_quantity)
                FROM
                  lineitem
                WHERE
                  l_partkey = p_partkey
              )            
    """
    // contains subquery, doesn't support now
    order_qt_query17_before "${query17}"
    check_mv_rewrite_fail(db, mv17, query17, "mv17")
    order_qt_query17_after "${query17}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17"""


    def mv18 = """
            SELECT
              c_name,
              c_custkey,
              o_orderkey,
              o_orderdate,
              o_totalprice,
              sum(l_quantity)
            FROM
              customer,
              orders,
              lineitem
            WHERE
              o_orderkey IN (
                SELECT l_orderkey
                FROM
                  lineitem
                GROUP BY
                  l_orderkey
                HAVING
                  sum(l_quantity) > 300
              )
              AND c_custkey = o_custkey
              AND o_orderkey = l_orderkey
            GROUP BY
              c_name,
              c_custkey,
              o_orderkey,
              o_orderdate,
              o_totalprice
            ORDER BY
              o_totalprice DESC,
              o_orderdate
            LIMIT 100           
    """
    def query18 = """
            SELECT
              c_name,
              c_custkey,
              o_orderkey,
              o_orderdate,
              o_totalprice,
              sum(l_quantity)
            FROM
              customer,
              orders,
              lineitem
            WHERE
              o_orderkey IN (
                SELECT l_orderkey
                FROM
                  lineitem
                GROUP BY
                  l_orderkey
                HAVING
                  sum(l_quantity) > 300
              )
              AND c_custkey = o_custkey
              AND o_orderkey = l_orderkey
            GROUP BY
              c_name,
              c_custkey,
              o_orderkey,
              o_orderdate,
              o_totalprice
            ORDER BY
              o_totalprice DESC,
              o_orderdate
            LIMIT 100               
    """
    // contains limit, doesn't support now
    order_qt_query18_before "${query18}"
    check_mv_rewrite_fail(db, mv18, query18, "mv18")
    order_qt_query18_after "${query18}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18"""


    def mv19 = """
            SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
            FROM
              lineitem,
              part
            WHERE
              (
                p_partkey = l_partkey
                AND p_brand = 'Brand#12'
                AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                AND l_quantity >= 1 AND l_quantity <= 1 + 10
                AND p_size BETWEEN 1 AND 5
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
              )
              OR
              (
                p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND l_quantity >= 10 AND l_quantity <= 10 + 10
                AND p_size BETWEEN 1 AND 10
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
              )
              OR
              (
                p_partkey = l_partkey
                AND p_brand = 'Brand#34'
                AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND l_quantity >= 20 AND l_quantity <= 20 + 10
                AND p_size BETWEEN 1 AND 15
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
              )
    """
    def query19 = """
            SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
            FROM
              lineitem,
              part
            WHERE
              (
                p_partkey = l_partkey
                AND p_brand = 'Brand#12'
                AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                AND l_quantity >= 1 AND l_quantity <= 1 + 10
                AND p_size BETWEEN 1 AND 5
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
              )
              OR
              (
                p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND l_quantity >= 10 AND l_quantity <= 10 + 10
                AND p_size BETWEEN 1 AND 10
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
              )
              OR
              (
                p_partkey = l_partkey
                AND p_brand = 'Brand#34'
                AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND l_quantity >= 20 AND l_quantity <= 20 + 10
                AND p_size BETWEEN 1 AND 15
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
              )           
    """
    // join condition is not conjunctions, doesn't support now
    order_qt_query19_before "${query19}"
    check_mv_rewrite_fail(db, mv19, query19, "mv19")
    order_qt_query19_after "${query19}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19"""


    def mv20 = """
            SELECT
              s_name,
              s_address
            FROM
              supplier, nation
            WHERE
              s_suppkey IN (
                SELECT ps_suppkey
                FROM
                  partsupp
                WHERE
                  ps_partkey IN (
                    SELECT p_partkey
                    FROM
                      part
                    WHERE
                      p_name LIKE 'forest%'
                  )
                  AND ps_availqty > (
                    SELECT 0.5 * sum(l_quantity)
                    FROM
                      lineitem
                    WHERE
                      l_partkey = ps_partkey
                      AND l_suppkey = ps_suppkey
                      AND l_shipdate >= date('1994-01-01')
                      AND l_shipdate < date('1994-01-01') + interval '1' YEAR
            )
            )
            AND s_nationkey = n_nationkey
            AND n_name = 'CANADA'
            ORDER BY s_name
    """
    def query20 = """
            SELECT
              s_name,
              s_address
            FROM
              supplier, nation
            WHERE
              s_suppkey IN (
                SELECT ps_suppkey
                FROM
                  partsupp
                WHERE
                  ps_partkey IN (
                    SELECT p_partkey
                    FROM
                      part
                    WHERE
                      p_name LIKE 'forest%'
                  )
                  AND ps_availqty > (
                    SELECT 0.5 * sum(l_quantity)
                    FROM
                      lineitem
                    WHERE
                      l_partkey = ps_partkey
                      AND l_suppkey = ps_suppkey
                      AND l_shipdate >= date('1994-01-01')
                      AND l_shipdate < date('1994-01-01') + interval '1' YEAR
            )
            )
            AND s_nationkey = n_nationkey
            AND n_name = 'CANADA'
            ORDER BY s_name            
    """
    // contains subquery, doesn't support now
    order_qt_query20_before "${query20}"
    check_mv_rewrite_fail(db, mv20, query20, "mv20")
    order_qt_query20_after "${query20}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20"""


    def mv21 = """
            SELECT
              s_name,
              count(*) AS numwait
            FROM
              supplier,
              lineitem l1,
              orders,
              nation
            WHERE
              s_suppkey = l1.l_suppkey
              AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F'
              AND l1.l_receiptdate > l1.l_commitdate
              AND exists(
                SELECT *
                FROM
                  lineitem l2
                WHERE
                  l2.l_orderkey = l1.l_orderkey
                  AND l2.l_suppkey <> l1.l_suppkey
              )
              AND NOT exists(
                SELECT *
                FROM
                  lineitem l3
                WHERE
                  l3.l_orderkey = l1.l_orderkey
                  AND l3.l_suppkey <> l1.l_suppkey
                  AND l3.l_receiptdate > l3.l_commitdate
              )
              AND s_nationkey = n_nationkey
              AND n_name = 'SAUDI ARABIA'
            GROUP BY
              s_name
            ORDER BY
              numwait DESC,
              s_name
            LIMIT 100  
    """
    def query21 = """
            SELECT
              s_name,
              count(*) AS numwait
            FROM
              supplier,
              lineitem l1,
              orders,
              nation
            WHERE
              s_suppkey = l1.l_suppkey
              AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F'
              AND l1.l_receiptdate > l1.l_commitdate
              AND exists(
                SELECT *
                FROM
                  lineitem l2
                WHERE
                  l2.l_orderkey = l1.l_orderkey
                  AND l2.l_suppkey <> l1.l_suppkey
              )
              AND NOT exists(
                SELECT *
                FROM
                  lineitem l3
                WHERE
                  l3.l_orderkey = l1.l_orderkey
                  AND l3.l_suppkey <> l1.l_suppkey
                  AND l3.l_receiptdate > l3.l_commitdate
              )
              AND s_nationkey = n_nationkey
              AND n_name = 'SAUDI ARABIA'
            GROUP BY
              s_name
            ORDER BY
              numwait DESC,
              s_name
            LIMIT 100             
    """
    // contains limit, doesn't support now
    order_qt_query21_before "${query21}"
    check_mv_rewrite_fail(db, mv21, query21, "mv21")
    order_qt_query21_after "${query21}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv21"""


    def mv22 = """
            SELECT
              cntrycode,
              count(*)       AS numcust,
              sum(c_acctbal) AS totacctbal
            FROM (
                   SELECT
                     substr(c_phone, 1, 2) AS cntrycode,
                     c_acctbal
                   FROM
                     customer
                   WHERE
                     substr(c_phone, 1, 2) IN
                     ('13', '31', '23', '29', '30', '18', '17')
                     AND c_acctbal > (
                       SELECT avg(c_acctbal)
                       FROM
                         customer
                       WHERE
                         c_acctbal > 0.00
                         AND substr(c_phone, 1, 2) IN
                             ('13', '31', '23', '29', '30', '18', '17')
                     )
                     AND NOT exists(
                       SELECT *
                       FROM
                         orders
                       WHERE
                         o_custkey = c_custkey
                     )
                 ) AS custsale
            GROUP BY
              cntrycode
            ORDER BY
              cntrycode
    """
    def query22 = """
            SELECT
              cntrycode,
              count(*)       AS numcust,
              sum(c_acctbal) AS totacctbal
            FROM (
                   SELECT
                     substr(c_phone, 1, 2) AS cntrycode,
                     c_acctbal
                   FROM
                     customer
                   WHERE
                     substr(c_phone, 1, 2) IN
                     ('13', '31', '23', '29', '30', '18', '17')
                     AND c_acctbal > (
                       SELECT avg(c_acctbal)
                       FROM
                         customer
                       WHERE
                         c_acctbal > 0.00
                         AND substr(c_phone, 1, 2) IN
                             ('13', '31', '23', '29', '30', '18', '17')
                     )
                     AND NOT exists(
                       SELECT *
                       FROM
                         orders
                       WHERE
                         o_custkey = c_custkey
                     )
                 ) AS custsale
            GROUP BY
              cntrycode
            ORDER BY
              cntrycode            
    """
    // contains subquery, doesn't support now
    order_qt_query22_before "${query22}"
    check_mv_rewrite_fail(db, mv22, query22, "mv22")
    order_qt_query22_after "${query22}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv22"""
}
