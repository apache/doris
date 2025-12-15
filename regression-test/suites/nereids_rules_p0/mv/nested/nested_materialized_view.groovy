import java.util.logging.Logger

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

    sql '''
alter table customer modify column c_address set stats (
  'ndv'='3013',
  'num_nulls'='0',
  'min_value'='  dcVkxZ,s,9xW ab60a',
  'max_value'='zzB4DRh4Eg3uFygL7UZZMiBa',
  'row_count'='3000'
);
'''

    sql '''
alter table customer modify column c_custkey set stats (
  'ndv'='3014',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='3000',
  'row_count'='3000'
);
'''

    sql '''
alter table customer modify column c_nation set stats (
  'ndv'='25',
  'num_nulls'='0',
  'min_value'='ALGERIA',
  'max_value'='VIETNAM',
  'row_count'='3000'
);
'''

    sql '''
alter table customer modify column c_mktsegment set stats (
  'ndv'='5',
  'num_nulls'='0',
  'min_value'='AUTOMOBILE',
  'max_value'='MACHINERY',
  'row_count'='3000'
);
'''

    sql '''
alter table customer modify column c_name set stats (
  'ndv'='2997',
  'num_nulls'='0',
  'min_value'='Customer#000000001',
  'max_value'='Customer#000003000',
  'row_count'='3000'
);
'''

    sql '''
alter table customer modify column c_region set stats (
  'ndv'='5',
  'num_nulls'='0',
  'min_value'='AFRICA',
  'max_value'='MIDDLE EAST',
  'row_count'='3000'
);
'''

    sql '''
alter table customer modify column c_city set stats (
  'ndv'='250',
  'num_nulls'='0',
  'min_value'='ALGERIA  0',
  'max_value'='VIETNAM  9',
  'row_count'='3000'
);
'''

    sql '''
alter table customer modify column c_phone set stats (
  'ndv'='3003',
  'num_nulls'='0',
  'min_value'='10-109-430-5638',
  'max_value'='34-996-906-1652',
  'row_count'='3000'
);
'''

    sql '''
alter table date modify column d_datekey set stats (
  'ndv'='254',
  'num_nulls'='0',
  'min_value'='19920101',
  'max_value'='19920911',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_daynuminyear set stats (
  'ndv'='257',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='255',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_lastdayinweekfl set stats (
  'ndv'='2',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='1',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_year set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='1992',
  'max_value'='1992',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_yearmonth set stats (
  'ndv'='9',
  'num_nulls'='0',
  'min_value'='Apr1992',
  'max_value'='Sep1992',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_daynuminmonth set stats (
  'ndv'='31',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='31',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_daynuminweek set stats (
  'ndv'='7',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='7',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_holidayfl set stats (
  'ndv'='2',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='1',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_monthnuminyear set stats (
  'ndv'='9',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='9',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_weekdayfl set stats (
  'ndv'='2',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='1',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_yearmonthnum set stats (
  'ndv'='9',
  'num_nulls'='0',
  'min_value'='199201',
  'max_value'='199209',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_month set stats (
  'ndv'='9',
  'num_nulls'='0',
  'min_value'='April',
  'max_value'='September',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_weeknuminyear set stats (
  'ndv'='37',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='37',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_date set stats (
  'ndv'='255',
  'num_nulls'='0',
  'min_value'='April 1, 1992',
  'max_value'='September 9, 1992',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_dayofweek set stats (
  'ndv'='7',
  'num_nulls'='0',
  'min_value'='Friday',
  'max_value'='Wednesday',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_lastdayinmonthfl set stats (
  'ndv'='2',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='1',
  'row_count'='255'
);
'''

    sql '''
alter table date modify column d_sellingseason set stats (
  'ndv'='4',
  'num_nulls'='0',
  'min_value'='Fall',
  'max_value'='Winter',
  'row_count'='255'
);
'''

    sql '''
alter table lineorder modify column lo_custkey set stats (
  'ndv'='2001',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='2999',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_extendedprice set stats (
  'ndv'='128773',
  'num_nulls'='0',
  'min_value'='90100',
  'max_value'='9594950',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_revenue set stats (
  'ndv'='453898',
  'num_nulls'='0',
  'min_value'='81720',
  'max_value'='9569950',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_suppkey set stats (
  'ndv'='201',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='200',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_quantity set stats (
  'ndv'='50',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='50',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_shipmode set stats (
  'ndv'='7',
  'num_nulls'='0',
  'min_value'='AIR',
  'max_value'='TRUCK',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_discount set stats (
  'ndv'='11',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='10',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_partkey set stats (
  'ndv'='19968',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='20000',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_shippriority set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='0',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_orderdate set stats (
  'ndv'='2408',
  'num_nulls'='0',
  'min_value'='19920101',
  'max_value'='19980802',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_tax set stats (
  'ndv'='9',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='8',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_commitdate set stats (
  'ndv'='2445',
  'num_nulls'='0',
  'min_value'='19920131',
  'max_value'='19981031',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_linenumber set stats (
  'ndv'='7',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='7',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_orderkey set stats (
  'ndv'='150431',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='600000',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_orderpriority set stats (
  'ndv'='5',
  'num_nulls'='0',
  'min_value'='1-URGENT',
  'max_value'='5-LOW',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_ordtotalprice set stats (
  'ndv'='150461',
  'num_nulls'='0',
  'min_value'='83340',
  'max_value'='47912921',
  'row_count'='600572'
);
'''

    sql '''
alter table lineorder modify column lo_supplycost set stats (
  'ndv'='2136',
  'num_nulls'='0',
  'min_value'='54060',
  'max_value'='115139',
  'row_count'='600572'
);
'''

    sql '''
alter table part modify column p_category set stats (
  'ndv'='25',
  'num_nulls'='0',
  'min_value'='MFGR#11',
  'max_value'='MFGR#55',
  'row_count'='20000'
);
'''

    sql '''
alter table part modify column p_name set stats (
  'ndv'='7495',
  'num_nulls'='0',
  'min_value'='almond antique',
  'max_value'='yellow white',
  'row_count'='20000'
);
'''

    sql '''
alter table part modify column p_type set stats (
  'ndv'='150',
  'num_nulls'='0',
  'min_value'='ECONOMY ANODIZED BRASS',
  'max_value'='STANDARD POLISHED TIN',
  'row_count'='20000'
);
'''

    sql '''
alter table part modify column p_mfgr set stats (
  'ndv'='5',
  'num_nulls'='0',
  'min_value'='MFGR#1',
  'max_value'='MFGR#5',
  'row_count'='20000'
);
'''

    sql '''
alter table part modify column p_color set stats (
  'ndv'='92',
  'num_nulls'='0',
  'min_value'='almond',
  'max_value'='yellow',
  'row_count'='20000'
);
'''

    sql '''
alter table part modify column p_partkey set stats (
  'ndv'='19968',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='20000',
  'row_count'='20000'
);
'''

    sql '''
alter table part modify column p_size set stats (
  'ndv'='50',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='50',
  'row_count'='20000'
);
'''

    sql '''
alter table part modify column p_container set stats (
  'ndv'='40',
  'num_nulls'='0',
  'min_value'='JUMBO BAG',
  'max_value'='WRAP PKG',
  'row_count'='20000'
);
'''

    sql '''
alter table part modify column p_brand set stats (
  'ndv'='1002',
  'num_nulls'='0',
  'min_value'='MFGR#111',
  'max_value'='MFGR#559',
  'row_count'='20000'
);
'''


    sql '''
alter table supplier modify column s_city set stats (
  'ndv'='143',
  'num_nulls'='0',
  'min_value'='ALGERIA  2',
  'max_value'='VIETNAM  8',
  'row_count'='200'
);
'''

    sql '''
alter table supplier modify column s_region set stats (
  'ndv'='5',
  'num_nulls'='0',
  'min_value'='AFRICA',
  'max_value'='MIDDLE EAST',
  'row_count'='200'
);
'''

    sql '''
alter table supplier modify column s_suppkey set stats (
  'ndv'='201',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='200',
  'row_count'='200'
);
'''

    sql '''
alter table supplier modify column s_name set stats (
  'ndv'='201',
  'num_nulls'='0',
  'min_value'='Supplier#000000001',
  'max_value'='Supplier#000000200',
  'row_count'='200'
);
'''

    sql '''
alter table supplier modify column s_phone set stats (
  'ndv'='200',
  'num_nulls'='0',
  'min_value'='10-127-851-8031',
  'max_value'='34-908-631-4424',
  'row_count'='200'
);
'''

    sql '''
alter table supplier modify column s_address set stats (
  'ndv'='197',
  'num_nulls'='0',
  'min_value'=' 0W7IPdkpWycU',
  'max_value'='zaux5FT',
  'row_count'='200'
);
'''

    sql '''
alter table supplier modify column s_nation set stats (
  'ndv'='25',
  'num_nulls'='0',
  'min_value'='ALGERIA',
  'max_value'='VIETNAM',
  'row_count'='200'
);
'''

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_materialized_view_nest_rewrite = true"

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

    sql """alter table lineitem modify column l_comment set stats ('row_count'='5');"""
    sql """alter table orders modify column o_comment set stats ('row_count'='8');"""
    sql """alter table partsupp modify column ps_comment set stats ('row_count'='2');"""

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
    create_async_mv(db, "mv1_0_inner_mv", mv1_0_inner_mv)
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0_inner_mv"""
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    sql "SET enable_materialized_view_nest_rewrite = false"

    order_qt_query1_1_before "${query1_0}"
    create_async_mv(db, "mv1_0_inner_mv", mv1_0_inner_mv)
    async_mv_rewrite_fail(db, mv1_0, query1_0, "mv1_0")

    mv_rewrite_success(query1_0, "mv1_0_inner_mv")
    order_qt_query1_1_after "${query1_0}"


    // complex nest mv rewrite
    // chose 4 different dates to create 4 groups of nested materialized views
    def fetchFourOrderDates = {
        def dates = []
        def result = sql """
        SELECT DISTINCT lo_orderdate
        FROM lineorder
        INNER JOIN customer ON lo_custkey = c_custkey
        INNER JOIN date ON lo_orderdate = d_datekey
        LIMIT 4;
        """
        result.each { row ->
            dates.add(row[0])
        }
        return dates;
    }

    def date = fetchFourOrderDates();
    logger.info("Fetched order dates for nested mv test: " + date);

    create_async_mv(db, "mv1_a", """
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
            d_daynuminweek > 0 and lo_orderdate = ${date[0]}
    group by
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate;""")

    sql """alter table mv1_a modify column lo_custkey set stats ('row_count'='2384');"""

    create_async_mv(db, "mv2_a", """
    select
        lo_custkey,
        lo_orderdate,
        sum(lo_revenue * lo_discount) as sum_value2
    FROM
        lineorder
            inner join customer on lo_custkey = c_custkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = ${date[0]}
    group by
        lo_custkey,
        lo_orderdate;""")

    sql """alter table mv2_a modify column lo_custkey set stats ('row_count'='580');"""

    create_async_mv(db, "mv4_a", """
    select
        lo_partkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value4
    FROM
        lineorder
            inner join supplier on lo_suppkey = s_suppkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = ${date[0]}
    group by
        lo_partkey,
        lo_orderdate;""")

    sql """alter table mv4_a modify column lo_partkey set stats ('row_count'='2371');"""

    create_async_mv(db, "mv_all_6_a", """
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

    sql """alter table mv_all_6_a modify column lo_custkey set stats ('row_count'='2384');"""

    create_async_mv(db, "mv1_b", """
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
            d_daynuminweek > 0 and lo_orderdate = ${date[1]}
    group by
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate;""")

    sql """alter table mv1_b modify column lo_custkey set stats ('row_count'='2565');"""

    create_async_mv(db, "mv2_b", """
    select
        lo_custkey,
        lo_orderdate,
        sum(lo_revenue * lo_discount) as sum_value2
    FROM
        lineorder
            inner join customer on lo_custkey = c_custkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = ${date[1]}
    group by
        lo_custkey,
        lo_orderdate;""")

    sql """alter table mv2_b modify column lo_custkey set stats ('row_count'='641');"""

    create_async_mv(db, "mv4_b", """
    select
        lo_partkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value4
    FROM
        lineorder
            inner join supplier on lo_suppkey = s_suppkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = ${date[1]}
    group by
        lo_partkey,
        lo_orderdate;""")

    sql """alter table mv4_b modify column lo_partkey set stats ('row_count'='2546');"""

    create_async_mv(db, "mv_all_6_b", """
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

    sql """alter table mv_all_6_b modify column lo_custkey set stats ('row_count'='2565');"""

    create_async_mv(db, "mv1_c", """
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
            d_daynuminweek > 0 and lo_orderdate = ${date[3]}
    group by
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate;""")

    sql """alter table mv1_c modify column lo_custkey set stats ('row_count'='2603');"""

    create_async_mv(db, "mv2_c", """
    select
        lo_custkey,
        lo_orderdate,
        sum(lo_revenue * lo_discount) as sum_value2
    FROM
        lineorder
            inner join customer on lo_custkey = c_custkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = ${date[3]}
    group by
        lo_custkey,
        lo_orderdate;""")

    sql """alter table mv2_c modify column lo_custkey set stats ('row_count'='641');"""

    create_async_mv(db, "mv4_c", """
    select
        lo_partkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value4
    FROM
        lineorder
            inner join supplier on lo_suppkey = s_suppkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = ${date[3]}
    group by
        lo_partkey,
        lo_orderdate;""")

    sql """alter table mv4_c modify column lo_partkey set stats ('row_count'='2581');"""

    create_async_mv(db, "mv_all_6_c", """
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

    sql """alter table mv_all_6_c modify column lo_custkey set stats ('row_count'='2603');"""

    create_async_mv(db, "mv1_d", """
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
            d_daynuminweek > 0 and lo_orderdate = ${date[2]}
    group by
        lo_custkey,
        lo_partkey,
        lo_orderkey,
        lo_orderdate;""")

    sql """alter table mv1_d modify column lo_custkey set stats ('row_count'='2327');"""

    create_async_mv(db, "mv2_d", """
    select
        lo_custkey,
        lo_orderdate,
        sum(lo_revenue * lo_discount) as sum_value2
    FROM
        lineorder
            inner join customer on lo_custkey = c_custkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = ${date[2]}
    group by
        lo_custkey,
        lo_orderdate;""")

    sql """alter table mv2_d modify column lo_custkey set stats ('row_count'='572');"""

    create_async_mv(db, "mv4_d", """
    select
        lo_partkey,
        lo_orderdate,
        sum(lo_extendedprice * lo_discount) as sum_value4
    FROM
        lineorder
            inner join supplier on lo_suppkey = s_suppkey
            inner join date on lo_orderdate = d_datekey
    where
            d_daynuminweek > 0 and lo_orderdate = ${date[2]}
    group by
        lo_partkey,
        lo_orderdate;""")

    sql """alter table mv4_d modify column lo_partkey set stats ('row_count'='2307');"""

    create_async_mv(db, "mv_all_6_d", """
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

    sql """alter table mv_all_6_d modify column lo_custkey set stats ('row_count'='2327');"""

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
          and lo_orderdate = ${date[0]}
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
          and lo_orderdate = ${date[0]}
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
          and lo_orderdate = ${date[0]}
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
          and lo_orderdate = ${date[0]}
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
          and lo_orderdate = ${date[1]}
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
          and lo_orderdate = ${date[1]}
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
          and lo_orderdate = ${date[1]}
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
          and lo_orderdate = ${date[1]}
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
          and lo_orderdate = ${date[3]}
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
          and lo_orderdate = ${date[3]}
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
          and lo_orderdate = ${date[3]}
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
          and lo_orderdate = ${date[3]}
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
          and lo_orderdate = ${date[2]}
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
          and lo_orderdate = ${date[2]}
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
          and lo_orderdate = ${date[2]}
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
          and lo_orderdate = ${date[2]}
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


    sql '''
alter table mv1_b modify column lo_orderkey set stats (
  'ndv'='69',
  'num_nulls'='0',
  'min_value'='16070',
  'max_value'='572579',
  'row_count'='276'
);
'''

    sql '''
alter table mv1_b modify column sum_value1 set stats (
  'ndv'='252',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='85274100',
  'row_count'='276'
);
'''

    sql '''
alter table mv1_b modify column lo_partkey set stats (
  'ndv'='273',
  'num_nulls'='0',
  'min_value'='210',
  'max_value'='19939',
  'row_count'='276'
);
'''

    sql '''
alter table mv1_b modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920604',
  'max_value'='19920604',
  'row_count'='276'
);
'''


    sql '''
alter table mv2_a modify column sum_value2 set stats (
  'ndv'='66',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='185206423',
  'row_count'='66'
);
'''

    sql '''
alter table mv2_a modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920529',
  'max_value'='19920529',
  'row_count'='66'
);
'''

    sql '''
alter table mv1_a modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920529',
  'max_value'='19920529',
  'row_count'='269'
);
'''

    sql '''
alter table mv1_a modify column sum_value1 set stats (
  'ndv'='246',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='81053280',
  'row_count'='269'
);
'''

    sql '''
alter table mv1_a modify column lo_orderkey set stats (
  'ndv'='67',
  'num_nulls'='0',
  'min_value'='8291',
  'max_value'='596935',
  'row_count'='269'
);
'''

    sql '''
alter table mv1_a modify column lo_partkey set stats (
  'ndv'='267',
  'num_nulls'='0',
  'min_value'='323',
  'max_value'='19957',
  'row_count'='269'
);
'''

    sql '''
alter table mv1_d modify column lo_partkey set stats (
  'ndv'='242',
  'num_nulls'='0',
  'min_value'='253',
  'max_value'='19861',
  'row_count'='245'
);
'''

    sql '''
alter table mv1_d modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920530',
  'max_value'='19920530',
  'row_count'='245'
);
'''

    sql '''
alter table mv1_d modify column sum_value1 set stats (
  'ndv'='225',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='86919140',
  'row_count'='245'
);
'''

    sql '''
alter table mv1_d modify column lo_orderkey set stats (
  'ndv'='64',
  'num_nulls'='0',
  'min_value'='16486',
  'max_value'='575302',
  'row_count'='245'
);
'''

    sql '''
alter table mv2_c modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920828',
  'max_value'='19920828',
  'row_count'='54'
);
'''

    sql '''
alter table mv2_c modify column sum_value2 set stats (
  'ndv'='54',
  'num_nulls'='0',
  'min_value'='2195164',
  'max_value'='158761143',
  'row_count'='54'
);
'''

    sql '''
alter table mv4_a modify column sum_value4 set stats (
  'ndv'='245',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='81053280',
  'row_count'='268'
);
'''

    sql '''
alter table mv4_a modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920529',
  'max_value'='19920529',
  'row_count'='268'
);
'''

    sql '''
alter table mv2_b modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920604',
  'max_value'='19920604',
  'row_count'='67'
);
'''

    sql '''
alter table mv2_b modify column sum_value2 set stats (
  'ndv'='67',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='176373155',
  'row_count'='67'
);
'''

    sql '''
alter table mv1_c modify column sum_value1 set stats (
  'ndv'='189',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='83203120',
  'row_count'='213'
);
'''

    sql '''
alter table mv1_c modify column lo_partkey set stats (
  'ndv'='212',
  'num_nulls'='0',
  'min_value'='131',
  'max_value'='19923',
  'row_count'='213'
);
'''

    sql '''
alter table mv1_c modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920828',
  'max_value'='19920828',
  'row_count'='213'
);
'''

    sql '''
alter table mv1_c modify column lo_orderkey set stats (
  'ndv'='54',
  'num_nulls'='0',
  'min_value'='1504',
  'max_value'='587201',
  'row_count'='213'
);
'''

    sql '''
alter table mv4_c modify column sum_value4 set stats (
  'ndv'='188',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='83203120',
  'row_count'='212'
);
'''

    sql '''
alter table mv4_c modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920828',
  'max_value'='19920828',
  'row_count'='212'
);
'''

    sql '''
alter table mv4_b modify column sum_value4 set stats (
  'ndv'='251',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='85274100',
  'row_count'='275'
);
'''

    sql '''
alter table mv4_b modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920604',
  'max_value'='19920604',
  'row_count'='275'
);
'''

    sql '''
alter table mv2_d modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920530',
  'max_value'='19920530',
  'row_count'='62'
);
'''

    sql '''
alter table mv2_d modify column sum_value2 set stats (
  'ndv'='61',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='207242262',
  'row_count'='62'
);
'''

    sql '''
alter table mv4_d modify column sum_value4 set stats (
  'ndv'='224',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='86919140',
  'row_count'='243'
);
'''

    sql '''
alter table mv4_d modify column lo_orderdate set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='19920530',
  'max_value'='19920530',
  'row_count'='243'
);
'''

    sql '''
alter table mv_all_6_a modify column __subtract_5 set stats (
  'ndv'='3',
  'num_nulls'='0',
  'min_value'='-28304424',
  'max_value'='0',
  'row_count'='269'
);
'''

    sql '''
alter table mv_all_6_a modify column __subtract_4 set stats (
  'ndv'='3',
  'num_nulls'='0',
  'min_value'='-28304424',
  'max_value'='0',
  'row_count'='269'
);
'''

    sql '''
alter table mv_all_6_a modify column nm set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='测试1',
  'max_value'='测试1',
  'row_count'='269'
);
'''

    sql '''
alter table mv_all_6_a modify column sum_value1 set stats (
  'ndv'='246',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='81053280',
  'row_count'='269'
);
'''

    sql '''
alter table mv_all_6_a modify column t_nm set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='测试2',
  'max_value'='测试2',
  'row_count'='269'
);
'''

    sql '''
alter table mv_all_6_a modify column __subtract_3 set stats (
  'ndv'='262',
  'num_nulls'='0',
  'min_value'='-183330928',
  'max_value'='2785088',
  'row_count'='269'
);
'''

    sql '''
alter table mv_all_6_b modify column t_nm set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='测试2',
  'max_value'='测试2',
  'row_count'='276'
);
'''

    sql '''
alter table mv_all_6_b modify column nm set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='测试1',
  'max_value'='测试1',
  'row_count'='276'
);
'''

    sql '''
alter table mv_all_6_b modify column __subtract_4 set stats (
  'ndv'='3',
  'num_nulls'='0',
  'min_value'='-4352247',
  'max_value'='0',
  'row_count'='276'
);
'''

    sql '''
alter table mv_all_6_b modify column sum_value1 set stats (
  'ndv'='252',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='85274100',
  'row_count'='276'
);
'''

    sql '''
alter table mv_all_6_b modify column __subtract_3 set stats (
  'ndv'='275',
  'num_nulls'='0',
  'min_value'='-176373155',
  'max_value'='2401722',
  'row_count'='276'
);
'''

    sql '''
alter table mv_all_6_b modify column __subtract_5 set stats (
  'ndv'='3',
  'num_nulls'='0',
  'min_value'='-4352247',
  'max_value'='0',
  'row_count'='276'
);
'''

    sql '''
alter table mv_all_6_c modify column __subtract_3 set stats (
  'ndv'='209',
  'num_nulls'='0',
  'min_value'='-155357379',
  'max_value'='7833780',
  'row_count'='213'
);
'''

    sql '''
alter table mv_all_6_c modify column t_nm set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='测试2',
  'max_value'='测试2',
  'row_count'='213'
);
'''

    sql '''
alter table mv_all_6_c modify column __subtract_4 set stats (
  'ndv'='3',
  'num_nulls'='0',
  'min_value'='-22727760',
  'max_value'='0',
  'row_count'='213'
);
'''

    sql '''
alter table mv_all_6_c modify column nm set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='测试1',
  'max_value'='测试1',
  'row_count'='213'
);
'''

    sql '''
alter table mv_all_6_c modify column sum_value1 set stats (
  'ndv'='189',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='83203120',
  'row_count'='213'
);
'''

    sql '''
alter table mv_all_6_c modify column __subtract_5 set stats (
  'ndv'='3',
  'num_nulls'='0',
  'min_value'='-22727760',
  'max_value'='0',
  'row_count'='213'
);
'''

    sql '''
alter table mv_all_6_d modify column __subtract_3 set stats (
  'ndv'='240',
  'num_nulls'='0',
  'min_value'='-207242262',
  'max_value'='7835790',
  'row_count'='245'
);
'''

    sql '''
alter table mv_all_6_d modify column nm set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='测试1',
  'max_value'='测试1',
  'row_count'='245'
);
'''

    sql '''
alter table mv_all_6_d modify column __subtract_5 set stats (
  'ndv'='4',
  'num_nulls'='0',
  'min_value'='-64365462',
  'max_value'='0',
  'row_count'='245'
);
'''

    sql '''
alter table mv_all_6_d modify column sum_value1 set stats (
  'ndv'='225',
  'num_nulls'='0',
  'min_value'='0',
  'max_value'='86919140',
  'row_count'='245'
);
'''

    sql '''
alter table mv_all_6_d modify column __subtract_4 set stats (
  'ndv'='4',
  'num_nulls'='0',
  'min_value'='-64365462',
  'max_value'='0',
  'row_count'='245'
);
'''

    sql '''
alter table mv_all_6_d modify column t_nm set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='测试2',
  'max_value'='测试2',
  'row_count'='245'
);
'''

    sql "SET enable_materialized_view_rewrite= true"
    sql "SET enable_materialized_view_nest_rewrite = true"
    sql "SET materialized_view_rewrite_duration_threshold_ms = 60000"
    // DP Hyper can not use pre materialized view rewrite
    sql """SET enable_dphyp_optimizer = false"""
    mv_rewrite_all_success_without_check_chosen(query2_0, ["mv_all_6_a", "mv_all_6_b", "mv_all_6_c", "mv_all_6_d"],
            [TRY_IN_RBO, FORCE_IN_RBO])
    mv_rewrite_all_fail(query2_0, ["mv_all_6_a", "mv_all_6_b", "mv_all_6_c", "mv_all_6_d"],
            [NOT_IN_RBO])
    // Compare result when before and after mv rewrite
    // DP Hyper can not use pre materialized view rewrite
    sql """SET enable_dphyp_optimizer = true"""
    compare_res(query2_0)
}
