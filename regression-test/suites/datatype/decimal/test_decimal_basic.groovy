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

suite("test_decimal_basic", "datatype") {
    sql "DROP TABLE IF EXISTS test_decimal_basic"
    sql """
        CREATE TABLE test_decimal_basic (`c_bigint` bigint, `c_short_decimal` decimal(5,2),
        `c_mediam_decimal` decimal(17,8), `c_long_decimal` decimal(27,9))
        ENGINE=OLAP DISTRIBUTED BY HASH(`c_bigint`) BUCKETS 1 properties("replication_num" = "1");
        """
    streamLoad {
        // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table 'test_decimal_basic'

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '|'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'decimal.csv'

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

    qt_sql1 "select * from test_decimal_basic"
    qt_sql2 "desc test_decimal_basic"

    qt_sql3 "select c_short_decimal + c_mediam_decimal from test_decimal_basic"
    qt_sql4 "select c_long_decimal - c_mediam_decimal from test_decimal_basic"
    qt_sql5 "select c_short_decimal * c_long_decimal from test_decimal_basic"
    qt_sql6 "select c_long_decimal / c_mediam_decimal from test_decimal_basic"
    qt_sql7 "select c_long_decimal % c_mediam_decimal from test_decimal_basic"
    qt_sql8 "select c_long_decimal & c_mediam_decimal from test_decimal_basic"
    qt_sql9 "select c_long_decimal | c_mediam_decimal from test_decimal_basic"
    qt_sql10 "select c_long_decimal ^ c_mediam_decimal from test_decimal_basic"

    qt_sql11 "select min(c_short_decimal) from test_decimal_basic"
    qt_sql12 "select min(c_mediam_decimal) from test_decimal_basic"
    qt_sql13 "select min(c_long_decimal) from test_decimal_basic"

    qt_sql14 "select max(c_short_decimal) from test_decimal_basic"
    qt_sql15 "select max(c_mediam_decimal) from test_decimal_basic"
    qt_sql16 "select max(c_long_decimal) from test_decimal_basic"

    qt_sql17 "select sum(c_short_decimal) from test_decimal_basic"
    qt_sql18 "select sum(c_mediam_decimal) from test_decimal_basic"
    qt_sql19 "select sum(c_long_decimal) from test_decimal_basic"

    qt_sql20 "select avg(c_short_decimal) from test_decimal_basic"
    qt_sql21 "select avg(c_mediam_decimal) from test_decimal_basic"
    qt_sql22 "select avg(c_long_decimal) from test_decimal_basic"

    qt_sql23 "select variance(c_short_decimal) from test_decimal_basic"
    qt_sql24 "select variance(c_mediam_decimal) from test_decimal_basic"
    qt_sql25 "select variance(c_long_decimal) from test_decimal_basic"

    qt_sql26 "select var_samp(c_short_decimal) from test_decimal_basic"
    qt_sql27 "select var_samp(c_mediam_decimal) from test_decimal_basic"
    qt_sql28 "select var_samp(c_long_decimal) from test_decimal_basic"

    qt_sql29 "select var_pop(c_short_decimal) from test_decimal_basic"
    qt_sql30 "select var_pop(c_mediam_decimal) from test_decimal_basic"
    qt_sql31 "select var_pop(c_long_decimal) from test_decimal_basic"

    qt_sql32 "select stddev(c_short_decimal) from test_decimal_basic"
    qt_sql33 "select stddev(c_mediam_decimal) from test_decimal_basic"
    qt_sql34 "select stddev(c_long_decimal) from test_decimal_basic"

    qt_sql35 "select stddev_samp(c_short_decimal) from test_decimal_basic"
    qt_sql36 "select stddev_samp(c_mediam_decimal) from test_decimal_basic"
    qt_sql37 "select stddev_samp(c_long_decimal) from test_decimal_basic"

    qt_sql38 "select stddev_pop(c_short_decimal) from test_decimal_basic"
    qt_sql39 "select stddev_pop(c_mediam_decimal) from test_decimal_basic"
    qt_sql40 "select stddev_pop(c_long_decimal) from test_decimal_basic"

    qt_sql41 "select ndv(c_short_decimal) from test_decimal_basic"
    qt_sql42 "select ndv(c_mediam_decimal) from test_decimal_basic"
    qt_sql43 "select ndv(c_long_decimal) from test_decimal_basic"

    qt_sql44 "select percentile(c_short_decimal, 0.99) from test_decimal_basic"
    qt_sql45 "select percentile(c_mediam_decimal, 0.99) from test_decimal_basic"
    qt_sql46 "select percentile(c_long_decimal, 0.99) from test_decimal_basic"

    qt_sql47 "select percentile_approx(c_short_decimal, 0.99) from test_decimal_basic"
    qt_sql48 "select percentile_approx(c_mediam_decimal, 0.99) from test_decimal_basic"
    qt_sql49 "select percentile_approx(c_long_decimal, 0.99) from test_decimal_basic"

    qt_sql50 "select topn(c_short_decimal, 2) from test_decimal_basic"
    qt_sql51 "select topn(c_mediam_decimal, 2) from test_decimal_basic"
    qt_sql52 "select topn(c_long_decimal, 2) from test_decimal_basic"

    qt_sql53 "select count(c_short_decimal) from test_decimal_basic"
    qt_sql54 "select count(c_mediam_decimal) from test_decimal_basic"
    qt_sql55 "select count(c_long_decimal) from test_decimal_basic"

    qt_sql56 "select approx_count_distinct(c_short_decimal) from test_decimal_basic"
    qt_sql57 "select approx_count_distinct(c_mediam_decimal) from test_decimal_basic"
    qt_sql58 "select approx_count_distinct(c_long_decimal) from test_decimal_basic"

    order_qt_sql59 "select distinct(c_short_decimal) from test_decimal_basic"
    order_qt_sql60 "select distinct(c_mediam_decimal) from test_decimal_basic"
    order_qt_sql61 "select distinct(c_long_decimal) from test_decimal_basic"

    sql "DROP TABLE test_decimal_basic"
}


