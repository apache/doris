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

suite("test_nullif") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    qt_ifnull_const1 """select CONCAT('a', ifnull(split_part('A.B','.',1), 'x'));"""
    qt_ifnull_const2 """select CONCAT('a', ifnull(split_part('A.B','.',1), null));"""

    def tableName = "datetype"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                c_bigint bigint,
                c_double double,
                c_string string,
                c_date date,
                c_timestamp datetime,
                c_date_1 datev2,
                c_timestamp_1 datetimev2,
                c_timestamp_2 datetimev2(3),
                c_timestamp_3 datetimev2(6),
                c_boolean boolean,
                c_short_decimal decimal(5,2),
                c_long_decimal decimal(27,9)
            )
            DUPLICATE KEY(c_bigint)
            DISTRIBUTED BY HASH(c_bigint) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """

    streamLoad {
        // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table tableName

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '|'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'datetype.csv'

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

    qt_select "select nullif(k6, \"false\") k from test_query_db.test order by k1"
    qt_select "select if(c_date is null,c_timestamp,c_date) from ${tableName} where c_date is null and c_timestamp is not null"
    qt_select "select if(c_bigint > 10,c_timestamp,c_date) from ${tableName}"
    qt_select "select if(c_date_1 is null,c_timestamp_1,c_date_1) from ${tableName} where c_date_1 is null and c_timestamp_1 is not null"
    qt_select "select if(c_date_1 is null,c_timestamp_2,c_date_1) from ${tableName} where c_date_1 is null and c_timestamp_2 is not null"
    qt_select "select if(c_date_1 is null,c_timestamp_3,c_date_1) from ${tableName} where c_date_1 is null and c_timestamp_3 is not null"

    sql "use test_query_db"
    def tableName1 = "test"
    qt_if_nullif1 """select if(null, -1, 10) a, if(null, "hello", "worlk") b"""
    qt_if_nullif2 """select if(k1 > 5, true, false) a from baseall order by k1"""
    qt_if_nullif3 """select if(k1, 10, -1) a from baseall order by k1"""
    qt_if_nullif4 """select if(length(k6) >= 5, true, false) a from baseall order by k1"""
    qt_if_nullif5 """select if(k6 like "fa%", -1, 10) a from baseall order by k6"""
    qt_if_nullif6 """select if(k6 like "%e", "hello", "world") a from baseall order by k6"""
    qt_if_nullif7 """select if(k6, -1, 0) a from baseall order by k6"""
    qt_if_nullif8 """select ifnull(b.k1, -1) k1 from baseall a left join bigtable b on a.k1 = b.k1 + 5 
            order by a.k1"""
    qt_if_nullif10 """select ifnull(b.k6, "hll") k1 from baseall a left join bigtable b on a.k1 = b.k1 + 5 
            order by k1"""
    qt_if_nullif11 """select ifnull(b.k10, "2017-06-06") k1 from baseall a left join bigtable b on 
            a.k1 = b.k1 + 5 order by k1"""
    qt_if_nullif12 """select ifnull(b.k10, cast("2017-06-06" as date)) k1 from baseall a left join bigtable 
            b on a.k1 = b.k1 + 5 order by k1"""
    qt_if_nullif13 """select ifnull(b.k1, "-1") k1 from baseall a left join bigtable b on a.k1 = b.k1 + 5 
            order by a.k1"""
    qt_if_nullif14 """select ifnull(b.k6, 1001) k1 from baseall a left join bigtable b on a.k1 = b.k1 + 5 
            order by k1"""
    qt_if_nullif15 """select nullif(k1, 100) k1 from baseall order by k1"""
    qt_if_nullif16 """select nullif(k6, "false") k from baseall order by k1"""
    qt_if_nullif17 """select cast(nullif(k10, cast("2012-03-14" as date)) as date) from baseall order by k1"""
    qt_if_nullif18 """select cast(nullif(k11, cast("2000-01-01 00:00:00" as datetime)) as datetime) from baseall order by k1"""
    qt_if_nullif19 """select nullif(b.k1, null) k1 from baseall a left join bigtable b on a.k1 = b.k1 
            order by k1"""

    test{
        sql"""select ifnull(null,2,3)"""
        check {result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    test{
        sql """select ifnull(1234567890123456789012345678901234567890,2)"""
        check {result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    qt_if_nullif20 """select ifnull(123456789.5678901234567890,2),
        ifnull("1234567890123456789012345678901234567890",2)"""
    qt_if_nullif21 """select IFNULL("hello", "doris"), IFNULL(NULL,0)"""
    qt_if_nullif22 """select ifnull("null",2), ifnull("NULL",2), ifnull("null","2019-09-09 00:00:00"),
        ifnull(NULL, concat("NUL", "LL"))"""
    
    for( index in range(1, 12)) {
        logger.info(index.toString())
        qt_if_nullif23 """select ifnull(k${index}, NULL) from ${tableName1} order by k${index}"""
        qt_if_nullif24 """select ifnull(NULL, k${index}) from ${tableName1} order by k${index}"""
    }
    qt_if_nullif25 """select ifnull("null",2+3*5), ifnull(NULL,concat(1,2)), ifnull(NULL, ifnull(1,3)),
           ifnull(NULL,NULL) <=> NULL"""
    qt_if_nullif26 """select ifnull(length("null"), 2), ifnull(concat(NULL, 0), 2), ifnull("1.0" + "3.3","2019-09-09 00:00:00"),
        ifnull(ltrim("  NULL"), concat("NUL", "LL"))"""
    qt_if_nullif27 """select ifnull(2+3, 2), ifnull((3*1 > 1 || 1>0), 2), ifnull((3*1 > 1 or 1>0), 2),
        ifnull(upper("null"), concat("NUL", "LL"))"""
    qt_if_nullif28 """select ifnull(date(substring("2020-02-09", 1, 1024)), null)"""
}
