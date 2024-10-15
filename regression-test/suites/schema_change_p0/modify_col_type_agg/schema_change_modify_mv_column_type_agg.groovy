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

suite("schema_change_modify_mv_column_type_agg") {
    //test legacy planner

    def dataFile = """${getS3Url()}/regression/datatypes/test_scalar_types_10w.csv"""

    // define dup key table1
    def testTable = "tbl_scalar_types_agg"
    sql "DROP TABLE IF EXISTS ${testTable} FORCE"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `k1` bigint(11) NULL,
            `k2` bigint(11) NULL,
            `c_bool` boolean replace NULL,
            `c_tinyint` tinyint(4) min NULL,
            `c_smallint` smallint(6) max NULL,
            `c_int` int(11) max NULL,
            `c_bigint` bigint(20) sum NULL,
            `c_largeint` largeint(40) min NULL,
            `c_float` float min NULL,
            `c_double` double max NULL,
            `c_decimal` decimal(20, 3) sum NULL,
            `c_decimalv3` decimalv3(20, 3) sum NULL,
            `c_date` date replace NULL,
            `c_datetime` datetime replace NULL,
            `c_datev2` datev2 replace NULL,
            `c_datetimev2` datetimev2(0)replace NULL,
            `c_char` char(15) replace NULL,
            `c_varchar` varchar(100) replace NULL,
            `c_string` text replace NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`)
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
        }
    }
    createMV ("""CREATE MATERIALIZED VIEW mv_${testTable}_1 AS SELECT k2, k1, max(c_int) FROM ${testTable} GROUP BY k2, k1""")
    qt_sql """ desc ${testTable} all """
    sql "set topn_opt_limit_threshold = 100"
    qt_sql "SELECT * from ${testTable} order by 1, 2, 3 limit 10"
    qt_sql "SELECT * from ${testTable} where c_tinyint = 10 order by 1, 2, 3 limit 10 "

    sql """
          ALTER table ${testTable} MODIFY COLUMN c_int BIGINT max;
          """
    def getJobState = { tableName ->
          def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
          return jobStateResult[0][9]
     }
    int max_try_time = 100
    while (max_try_time--){
        String result = getJobState(testTable)
        if (result == "FINISHED") {
            break
        } else {
            sleep(2000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    qt_sql """ desc ${testTable} all """
    sql "INSERT INTO ${testTable} SELECT * from ${testTable}"
}
