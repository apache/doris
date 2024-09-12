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

// Note: To filter out tables from sql files, use the following one-liner comamnd
// sed -nr 's/.*tables: (.*)$/\1/gp' /path/to/*.sql | sed -nr 's/,/\n/gp' | sort | uniq
suite("mv_ssb_test") {

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
            file """${getS3Url()}/regression/ssb/sf0.1/${tableName}.tbl.gz"""

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
    sql "SET enable_nereids_timeout = false"
    sql "SET BATCH_SIZE = 4064"

    def mv1_1 = """
            SELECT SUM(lo_extendedprice*lo_discount) AS
            REVENUE
            FROM  lineorder, date
            WHERE  lo_orderdate = d_datekey
            AND d_year = 1993
            AND lo_discount BETWEEN 1 AND 3
            AND lo_quantity < 25;
    """
    def query1_1 = """
            SELECT SUM(lo_extendedprice*lo_discount) AS
            REVENUE
            FROM  lineorder, date
            WHERE  lo_orderdate = d_datekey
            AND d_year = 1993
            AND lo_discount BETWEEN 1 AND 3
            AND lo_quantity < 25;
    """
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_success(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    def mv1_2 = """
            SELECT SUM(lo_extendedprice*lo_discount) AS
            REVENUE
            FROM  lineorder, date
            WHERE  lo_orderdate = d_datekey
            AND d_yearmonth = 'Jan1994'
            AND lo_discount BETWEEN 4 AND 6
            AND lo_quantity BETWEEN 26 AND 35;
    """
    def query1_2 = """
            SELECT SUM(lo_extendedprice*lo_discount) AS
            REVENUE
            FROM  lineorder, date
            WHERE  lo_orderdate = d_datekey
            AND d_yearmonth = 'Jan1994'
            AND lo_discount BETWEEN 4 AND 6
            AND lo_quantity BETWEEN 26 AND 35;
    """
    order_qt_query1_2_before "${query1_2}"
    async_mv_rewrite_success(db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""

    def mv1_3 = """
            SELECT SUM(lo_extendedprice*lo_discount) AS
            REVENUE
            FROM  lineorder, date
            WHERE  lo_orderdate = d_datekey
            AND d_weeknuminyear= 6
            AND d_year = 1994
            AND lo_discount BETWEEN  5 AND 7
            AND lo_quantity BETWEEN  26 AND 35;
    """
    def query1_3 = """
            SELECT SUM(lo_extendedprice*lo_discount) AS
            REVENUE
            FROM  lineorder, date
            WHERE  lo_orderdate = d_datekey
            AND d_weeknuminyear= 6
            AND d_year = 1994
            AND lo_discount BETWEEN  5 AND 7
            AND lo_quantity BETWEEN  26 AND 35;
    """
    order_qt_query1_3before "${query1_3}"
    async_mv_rewrite_success(db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""


    def mv2_1 = """
            SELECT SUM(lo_revenue), d_year, p_brand
            FROM lineorder, date, part, supplier
            WHERE lo_orderdate = d_datekey
            AND lo_partkey = p_partkey
            AND lo_suppkey = s_suppkey
            AND p_category = 'MFGR#12'
            AND s_region = 'AMERICA'
            GROUP BY d_year, p_brand
            ORDER BY d_year, p_brand;
    """
    def query2_1 = """
            SELECT SUM(lo_revenue), d_year, p_brand
            FROM lineorder, date, part, supplier
            WHERE lo_orderdate = d_datekey
            AND lo_partkey = p_partkey
            AND lo_suppkey = s_suppkey
            AND p_category = 'MFGR#12'
            AND s_region = 'AMERICA'
            GROUP BY d_year, p_brand
            ORDER BY d_year, p_brand;
    """
    order_qt_query2_1before "${query2_1}"
    async_mv_rewrite_success(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""


    def mv2_2 = """
            SELECT SUM(lo_revenue), d_year, p_brand
            FROM lineorder, date, part, supplier
            WHERE lo_orderdate = d_datekey
            AND lo_partkey = p_partkey
            AND lo_suppkey = s_suppkey
            AND p_brand BETWEEN  'MFGR#2221'
            AND 'MFGR#2228'
            AND s_region = 'ASIA'
            GROUP BY d_year, p_brand
            ORDER BY d_year, p_brand;
    """
    def query2_2 = """
            SELECT SUM(lo_revenue), d_year, p_brand
            FROM lineorder, date, part, supplier
            WHERE lo_orderdate = d_datekey
            AND lo_partkey = p_partkey
            AND lo_suppkey = s_suppkey
            AND p_brand BETWEEN  'MFGR#2221'
            AND 'MFGR#2228'
            AND s_region = 'ASIA'
            GROUP BY d_year, p_brand
            ORDER BY d_year, p_brand;
    """
    order_qt_query2_2before "${query2_2}"
    async_mv_rewrite_success(db, mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""

    def mv2_3 = """
            SELECT SUM(lo_revenue), d_year, p_brand
            FROM lineorder, date, part, supplier
            WHERE  lo_orderdate = d_datekey
            AND lo_partkey = p_partkey
            AND lo_suppkey = s_suppkey
            AND p_brand = 'MFGR#2239'
            AND s_region = 'EUROPE'
            GROUP BY d_year, p_brand
            ORDER BY d_year, p_brand;
    """
    def query2_3 = """
            SELECT SUM(lo_revenue), d_year, p_brand
            FROM lineorder, date, part, supplier
            WHERE  lo_orderdate = d_datekey
            AND lo_partkey = p_partkey
            AND lo_suppkey = s_suppkey
            AND p_brand = 'MFGR#2239'
            AND s_region = 'EUROPE'
            GROUP BY d_year, p_brand
            ORDER BY d_year, p_brand;
    """
    order_qt_query2_3before "${query2_3}"
    async_mv_rewrite_success(db, mv2_3, query2_3, "mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_3"""

    def mv3_1 = """
            SELECT c_nation, s_nation, d_year,
            SUM(lo_revenue)  AS  REVENUE
            FROM customer, lineorder, supplier, date
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_orderdate = d_datekey
            AND c_region = 'ASIA'
            AND s_region = 'ASIA'
            AND d_year >= 1992 AND d_year <= 1997
            GROUP BY c_nation, s_nation, d_year
            ORDER BY d_year ASC,  REVENUE DESC;
    """
    def query3_1 = """
            SELECT c_nation, s_nation, d_year,
            SUM(lo_revenue)  AS  REVENUE
            FROM customer, lineorder, supplier, date
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_orderdate = d_datekey
            AND c_region = 'ASIA'
            AND s_region = 'ASIA'
            AND d_year >= 1992 AND d_year <= 1997
            GROUP BY c_nation, s_nation, d_year
            ORDER BY d_year ASC,  REVENUE DESC;
    """
    order_qt_query3_1before "${query3_1}"
    async_mv_rewrite_success(db, mv3_1, query3_1, "mv3_1")
    order_qt_query3_1_after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    def mv3_2 = """
            SELECT c_city, s_city, d_year, sum(lo_revenue)
            AS  REVENUE
            FROM customer, lineorder, supplier, date
            WHERE  lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_orderdate = d_datekey
            AND c_nation = 'UNITED STATES'
            AND s_nation = 'UNITED STATES'
            AND d_year >= 1992 AND d_year <= 1997
            GROUP BY c_city, s_city, d_year
            ORDER BY d_year ASC,  REVENUE DESC;
    """
    def query3_2 = """
            SELECT c_city, s_city, d_year, sum(lo_revenue)
            AS  REVENUE
            FROM customer, lineorder, supplier, date
            WHERE  lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_orderdate = d_datekey
            AND c_nation = 'UNITED STATES'
            AND s_nation = 'UNITED STATES'
            AND d_year >= 1992 AND d_year <= 1997
            GROUP BY c_city, s_city, d_year
            ORDER BY d_year ASC,  REVENUE DESC;
    """
    order_qt_query3_2before "${query3_2}"
    async_mv_rewrite_success(db, mv3_2, query3_2, "mv3_2")
    order_qt_query3_2_after "${query3_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_2"""


    def mv3_3 = """
            SELECT c_city, s_city, d_year, SUM(lo_revenue)
            AS  REVENUE
            FROM customer, lineorder, supplier, date
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND  lo_orderdate = d_datekey
            AND  (c_city='UNITED KI1'
            OR c_city='UNITED KI5')
            AND (s_city='UNITED KI1'
            OR s_city='UNITED KI5')
            AND d_year >= 1992 AND d_year <= 1997
            GROUP BY c_city, s_city, d_year
            ORDER BY d_year ASC,  REVENUE DESC;
    """
    def query3_3 = """
            SELECT c_city, s_city, d_year, SUM(lo_revenue)
            AS  REVENUE
            FROM customer, lineorder, supplier, date
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND  lo_orderdate = d_datekey
            AND  (c_city='UNITED KI1'
            OR c_city='UNITED KI5')
            AND (s_city='UNITED KI1'
            OR s_city='UNITED KI5')
            AND d_year >= 1992 AND d_year <= 1997
            GROUP BY c_city, s_city, d_year
            ORDER BY d_year ASC,  REVENUE DESC;
    """
    order_qt_query3_3before "${query3_3}"
    async_mv_rewrite_success(db, mv3_3, query3_3, "mv3_3")
    order_qt_query3_3_after "${query3_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_3"""


    def mv3_4 = """
            SELECT c_city, s_city, d_year, SUM(lo_revenue)
            AS  REVENUE
            FROM customer, lineorder, supplier, date
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_orderdate = d_datekey
            AND  (c_city='UNITED KI1'
            OR c_city='UNITED KI5')
            AND (s_city='UNITED KI1'
            OR s_city='UNITED KI5')
            AND d_yearmonth = 'Jul1992'
            GROUP BY c_city, s_city, d_year
            ORDER BY d_year ASC,  REVENUE DESC;
    """
    def query3_4 = """
            SELECT c_city, s_city, d_year, SUM(lo_revenue)
            AS  REVENUE
            FROM customer, lineorder, supplier, date
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_orderdate = d_datekey
            AND  (c_city='UNITED KI1'
            OR c_city='UNITED KI5')
            AND (s_city='UNITED KI1'
            OR s_city='UNITED KI5')
            AND d_yearmonth = 'Jul1992'
            GROUP BY c_city, s_city, d_year
            ORDER BY d_year ASC,  REVENUE DESC;
    """
    order_qt_query3_4before "${query3_4}"
    async_mv_rewrite_success(db, mv3_4, query3_4, "mv3_4")
    order_qt_query3_4_after "${query3_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_4"""


    def mv4_1 = """
            SELECT d_year, c_nation,
            SUM(lo_revenue - lo_supplycost) AS PROFIT
            FROM date, customer, supplier, part, lineorder
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_partkey = p_partkey
            AND lo_orderdate = d_datekey
            AND c_region = 'AMERICA'
            AND s_region = 'AMERICA'
            AND (p_mfgr = 'MFGR#1'
            OR  p_mfgr = 'MFGR#2')
            GROUP BY d_year, c_nation
            ORDER BY d_year, c_nation;
    """
    def query4_1 = """
            SELECT d_year, c_nation,
            SUM(lo_revenue - lo_supplycost) AS PROFIT
            FROM date, customer, supplier, part, lineorder
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_partkey = p_partkey
            AND lo_orderdate = d_datekey
            AND c_region = 'AMERICA'
            AND s_region = 'AMERICA'
            AND (p_mfgr = 'MFGR#1'
            OR  p_mfgr = 'MFGR#2')
            GROUP BY d_year, c_nation
            ORDER BY d_year, c_nation;
    """
    order_qt_query4_1before "${query4_1}"
    async_mv_rewrite_success(db, mv4_1, query4_1, "mv4_1")
    order_qt_query4_1_after "${query4_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_1"""

    def mv4_2 = """
            SELECT d_year, s_nation, p_category,
            SUM(lo_revenue - lo_supplycost) AS PROFIT
            FROM date, customer, supplier, part, lineorder
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_partkey = p_partkey
            AND lo_orderdate = d_datekey
            AND c_region = 'AMERICA'
            AND s_region = 'AMERICA'
            AND (d_year = 1992 OR d_year = 1993)
            AND (p_mfgr = 'MFGR#1'
            OR p_mfgr = 'MFGR#2')
            GROUP BY d_year, s_nation, p_category
            ORDER BY d_year, s_nation, p_category;
    """
    def query4_2 = """
            SELECT d_year, s_nation, p_category,
            SUM(lo_revenue - lo_supplycost) AS PROFIT
            FROM date, customer, supplier, part, lineorder
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_partkey = p_partkey
            AND lo_orderdate = d_datekey
            AND c_region = 'AMERICA'
            AND s_region = 'AMERICA'
            AND (d_year = 1992 OR d_year = 1993)
            AND (p_mfgr = 'MFGR#1'
            OR p_mfgr = 'MFGR#2')
            GROUP BY d_year, s_nation, p_category
            ORDER BY d_year, s_nation, p_category;
    """
    order_qt_query4_2before "${query4_2}"
    async_mv_rewrite_success(db, mv4_2, query4_2, "mv4_2")
    order_qt_query4_2_after "${query4_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_2"""


    def mv4_3 = """
            SELECT d_year, s_city, p_brand,
            SUM(lo_revenue - lo_supplycost) AS PROFIT
            FROM date, customer, supplier, part, lineorder
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_partkey = p_partkey
            AND lo_orderdate = d_datekey
            AND s_nation = 'UNITED STATES'
            AND (d_year = 1992 OR d_year = 1993)
            AND p_category = 'MFGR#14'
            GROUP BY d_year, s_city, p_brand
            ORDER BY d_year, s_city, p_brand;
    """
    def query4_3 = """
            SELECT d_year, s_city, p_brand,
            SUM(lo_revenue - lo_supplycost) AS PROFIT
            FROM date, customer, supplier, part, lineorder
            WHERE lo_custkey = c_custkey
            AND lo_suppkey = s_suppkey
            AND lo_partkey = p_partkey
            AND lo_orderdate = d_datekey
            AND s_nation = 'UNITED STATES'
            AND (d_year = 1992 OR d_year = 1993)
            AND p_category = 'MFGR#14'
            GROUP BY d_year, s_city, p_brand
            ORDER BY d_year, s_city, p_brand;
    """
    order_qt_query4_3before "${query4_3}"
    async_mv_rewrite_success(db, mv4_3, query4_3, "mv4_3")
    order_qt_query4_3_after "${query4_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_3"""
}
