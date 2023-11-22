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

suite("test_point_query_load", "p0") {

    // nereids do not support point query now
    sql """set enable_nereids_planner=false"""

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
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "store_row_column" = "true");
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
    createMV ("""CREATE MATERIALIZED VIEW mv_${testTable} AS SELECT c_tinyint, c_bool, k1, c_smallint, c_int, c_bigint, c_largeint, c_float, c_double,  c_decimal, c_decimalv3, c_date, c_datetime, c_datev2, c_datetimev2, c_char, c_varchar, c_string FROM ${testTable} ORDER BY c_tinyint, c_bool, k1""")

    sql "set topn_opt_limit_threshold = 100"
    explain {
        sql("SELECT * from ${testTable} where c_tinyint = 10 order by 1, 2, 3 limit 10")
        contains "(mv_${testTable})"
    }
    qt_sql "SELECT * from ${testTable} order by 1, 2, 3 limit 10"
    qt_sql "SELECT * from ${testTable} where c_tinyint = 10 order by 1, 2, 3 limit 10 "

    sql """
          ALTER table ${testTable} MODIFY COLUMN c_int BIGINT;
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
    sql "INSERT INTO ${testTable} SELECT * from ${testTable}"
}
