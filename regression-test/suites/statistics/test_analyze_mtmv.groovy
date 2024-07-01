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

suite("test_analyze_mtmv") {

    def wait_row_count_reported = { db, table, row, column, expected ->
        def result = sql """show frontends;"""
        logger.info("show frontends result origin: " + result)
        def host
        def port
        for (int i = 0; i < result.size(); i++) {
            if (result[i][8] == "true") {
                host = result[i][1]
                port = result[i][4]
            }
        }
        def tokens = context.config.jdbcUrl.split('/')
        def url=tokens[0] + "//" + host + ":" + port
        logger.info("Master url is " + url)
        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url) {
            sql """use ${db}"""
            result = sql """show frontends;"""
            logger.info("show frontends result master: " + result)
            for (int i = 0; i < 120; i++) {
                Thread.sleep(5000)
                result = sql """SHOW DATA FROM ${table};"""
                logger.info("result " + result)
                if (result[row][column] == expected) {
                    return;
                }
            }
            throw new Exception("Row count report timeout.")
        }

    }

    def verify_full_analyze = {
        def result = sql """show column stats mv1"""
        assertEquals(5, result.size())

        result = sql """show column stats mv1(l_shipdate)"""
        assertEquals(1, result.size())
        assertEquals("l_shipdate", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("3.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("12.0", result[0][5])
        assertEquals("4.0", result[0][6])
        assertEquals("'2023-10-17'", result[0][7])
        assertEquals("'2023-10-19'", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])

        result = sql """show column cached stats mv1(l_shipdate)"""
        assertEquals(1, result.size())
        assertEquals("l_shipdate", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("3.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("12.0", result[0][5])
        assertEquals("4.0", result[0][6])
        assertEquals("'2023-10-17'", result[0][7])
        assertEquals("'2023-10-19'", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])

        result = sql """show column stats mv1(o_orderdate)"""
        assertEquals(1, result.size())
        assertEquals("o_orderdate", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("3.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("12.0", result[0][5])
        assertEquals("4.0", result[0][6])
        assertEquals("'2023-10-17'", result[0][7])
        assertEquals("'2023-10-19'", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])

        result = sql """show column cached stats mv1(o_orderdate)"""
        assertEquals(1, result.size())
        assertEquals("o_orderdate", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("3.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("12.0", result[0][5])
        assertEquals("4.0", result[0][6])
        assertEquals("'2023-10-17'", result[0][7])
        assertEquals("'2023-10-19'", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])

        result = sql """show column stats mv1(l_partkey)"""
        assertEquals(1, result.size())
        assertEquals("l_partkey", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("1.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("12.0", result[0][5])
        assertEquals("4.0", result[0][6])
        assertEquals("2", result[0][7])
        assertEquals("2", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])

        result = sql """show column cached stats mv1(l_partkey)"""
        assertEquals(1, result.size())
        assertEquals("l_partkey", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("1.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("12.0", result[0][5])
        assertEquals("4.0", result[0][6])
        assertEquals("2", result[0][7])
        assertEquals("2", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])

        result = sql """show column stats mv1(l_suppkey)"""
        assertEquals(1, result.size())
        assertEquals("l_suppkey", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("1.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("12.0", result[0][5])
        assertEquals("4.0", result[0][6])
        assertEquals("3", result[0][7])
        assertEquals("3", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])

        result = sql """show column cached stats mv1(l_suppkey)"""
        assertEquals(1, result.size())
        assertEquals("l_suppkey", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("1.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("12.0", result[0][5])
        assertEquals("4.0", result[0][6])
        assertEquals("3", result[0][7])
        assertEquals("3", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])

        result = sql """show column stats mv1(sum_total)"""
        assertEquals(1, result.size())
        assertEquals("sum_total", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("2.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("48.0", result[0][5])
        assertEquals("16.0", result[0][6])
        assertEquals("99.50", result[0][7])
        assertEquals("109.20", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])

        result = sql """show column cached stats mv1(sum_total)"""
        assertEquals(1, result.size())
        assertEquals("sum_total", result[0][0])
        assertEquals("mv1", result[0][1])
        assertEquals("3.0", result[0][2])
        assertEquals("2.0", result[0][3])
        assertEquals("0.0", result[0][4])
        assertEquals("48.0", result[0][5])
        assertEquals("16.0", result[0][6])
        assertEquals("99.50", result[0][7])
        assertEquals("109.20", result[0][8])
        assertEquals("FULL", result[0][9])
        assertEquals("MANUAL", result[0][11])
    }

    sql """drop database if exists test_analyze_mtmv"""
    sql """create database test_analyze_mtmv"""
    sql """use test_analyze_mtmv"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""
    sql """CREATE TABLE IF NOT EXISTS orders  (
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
		    FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES ("replication_num" = "1");
    """

    sql """insert into orders values
	    (1, 1, 'ok', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
	    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
	    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy');
    """

    sql """CREATE TABLE IF NOT EXISTS lineitem (
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
	    PROPERTIES ("replication_num" = "1");
    """

    sql """insert into lineitem values
	    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
	    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
	    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """

    sql """
	    CREATE MATERIALIZED VIEW mv1
	    BUILD DEFERRED REFRESH AUTO ON MANUAL
	    partition by(l_shipdate)
	    DISTRIBUTED BY RANDOM BUCKETS 2
	    PROPERTIES ('replication_num' = '1')
	    AS
	    select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
	    from lineitem
	    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
	    group by
	    l_shipdate,
	    o_orderdate,
	    l_partkey,
	    l_suppkey;
    """
    sql """REFRESH MATERIALIZED VIEW mv1 AUTO"""
    while(true) {
        Thread.sleep(1000)
        def result = sql """select * from mv_infos("database"="test_analyze_mtmv") where Name="mv1";"""
        if (result[0][5] == "SUCCESS") {
            break;
        }
    }

    def dup_sql1 = """select * from mv1 order by l_shipdate;"""
    qt_sql1 dup_sql1

    sql """analyze table mv1 with sync"""
    verify_full_analyze()


    sql """drop stats mv1"""
    def result_sample = sql """show column stats mv1(sum_total)"""
    assertEquals(0, result_sample.size())
    result_sample = sql """show column cached stats mv1(sum_total)"""
    assertEquals(0, result_sample.size())

    try {
        wait_row_count_reported("test_analyze_mtmv", "mv1", 0, 4, "3")
    } catch (Exception e) {
        logger.info(e.getMessage());
        return;
    }
    sql """analyze table mv1 with sync with sample rows 4000000"""
    result_sample = sql """show column stats mv1(l_shipdate)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("l_shipdate", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("3.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("12.0", result_sample[0][5])
    assertEquals("4.0", result_sample[0][6])
    assertEquals("'2023-10-17'", result_sample[0][7])
    assertEquals("'2023-10-19'", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column cached stats mv1(l_shipdate)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("l_shipdate", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("3.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("12.0", result_sample[0][5])
    assertEquals("4.0", result_sample[0][6])
    assertEquals("'2023-10-17'", result_sample[0][7])
    assertEquals("'2023-10-19'", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column stats mv1(o_orderdate)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("o_orderdate", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("3.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("12.0", result_sample[0][5])
    assertEquals("4.0", result_sample[0][6])
    assertEquals("'2023-10-17'", result_sample[0][7])
    assertEquals("'2023-10-19'", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column cached stats mv1(o_orderdate)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("o_orderdate", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("3.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("12.0", result_sample[0][5])
    assertEquals("4.0", result_sample[0][6])
    assertEquals("'2023-10-17'", result_sample[0][7])
    assertEquals("'2023-10-19'", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column stats mv1(l_partkey)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("l_partkey", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("1.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("12.0", result_sample[0][5])
    assertEquals("4.0", result_sample[0][6])
    assertEquals("2", result_sample[0][7])
    assertEquals("2", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column cached stats mv1(l_partkey)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("l_partkey", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("1.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("12.0", result_sample[0][5])
    assertEquals("4.0", result_sample[0][6])
    assertEquals("2", result_sample[0][7])
    assertEquals("2", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column stats mv1(l_suppkey)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("l_suppkey", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("1.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("12.0", result_sample[0][5])
    assertEquals("4.0", result_sample[0][6])
    assertEquals("3", result_sample[0][7])
    assertEquals("3", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column cached stats mv1(l_suppkey)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("l_suppkey", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("1.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("12.0", result_sample[0][5])
    assertEquals("4.0", result_sample[0][6])
    assertEquals("3", result_sample[0][7])
    assertEquals("3", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column stats mv1(sum_total)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("sum_total", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("2.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("48.0", result_sample[0][5])
    assertEquals("16.0", result_sample[0][6])
    assertEquals("99.50", result_sample[0][7])
    assertEquals("109.20", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column cached stats mv1(sum_total)"""
    logger.info("result " + result_sample)
    assertEquals(1, result_sample.size())
    assertEquals("sum_total", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("3.0", result_sample[0][2])
    assertEquals("2.0", result_sample[0][3])
    assertEquals("0.0", result_sample[0][4])
    assertEquals("48.0", result_sample[0][5])
    assertEquals("16.0", result_sample[0][6])
    assertEquals("99.50", result_sample[0][7])
    assertEquals("109.20", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    def result = sql """show variables like "%enable_partition_analyze%" """
    if (result[0][1].equalsIgnoreCase("false")) {
        logger.info("partition analyze disabled. " + result)
        sql """drop database if exists test_analyze_mtmv"""
        return;
    }

    sql """drop stats mv1"""
    sql """analyze table mv1 with sync"""
    verify_full_analyze()
    result = sql """show column stats mv1 partition(*)"""
    assertEquals(15, result.size())

    sql """drop database if exists test_analyze_mtmv"""
}

