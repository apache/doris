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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("schema_change_modify_mv_column_type") {
    //test legacy planner

    def dataFile = """${getS3Url()}/regression/datatypes/test_scalar_types_10w.csv"""

    // define dup key table1
    def testTable = "tbl_scalar_types_dup"
    sql "DROP TABLE IF EXISTS ${testTable} FORCE"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `k1` bigint(11) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_bigint` bigint(20) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_date` date NULL,
            `c_datetime` datetime NULL,
            `c_datev2` datev2 NULL,
            `c_datetimev2` datetimev2(0) NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

    // load data
    streamLoad {
        table testTable
        file dataFile
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(100000, json.NumberTotalRows)
            assertEquals(100000, json.NumberLoadedRows)
        }
    }
    createMV ("""CREATE MATERIALIZED VIEW mv_${testTable}_1 AS SELECT c_tinyint as a1, c_bool as a2, k1 as a3, c_smallint as a4, c_int as a5, c_bigint as a6, c_largeint as b1, c_float as b2, c_double as b3,  c_decimal as b4, c_decimalv3 as b5, c_date as b6, c_datetime as c1, c_datev2 as c2, c_datetimev2 as c3, c_char as c4, c_varchar as c5, c_string as c6 FROM ${testTable} ORDER BY c_tinyint, c_bool, k1""")
    qt_sql """ desc ${testTable} all """
    sql "set topn_opt_limit_threshold = 100"
    qt_sql "SELECT * from ${testTable} order by 1, 2, 3 limit 10"
    qt_sql "SELECT * from ${testTable} where c_tinyint = 10 order by 1, 2, 3 limit 10 "
    test {
        sql "ALTER table ${testTable} MODIFY COLUMN c_int BIGINT;"
        exception "Can not modify column contained by mv"
    }
}
