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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.
suite("load"){
    def tableName="opensky"
    def sourceFiles=["reorder_flightlist_20190101_20190131.csv.gz", "reorder_flightlist_20190201_20190228.csv.gz", "reorder_flightlist_20190301_20190331.csv.gz", "reorder_flightlist_20190401_20190430.csv.gz", "reorder_flightlist_20190501_20190531.csv.gz", "reorder_flightlist_20190601_20190630.csv.gz", "reorder_flightlist_20190701_20190731.csv.gz", "reorder_flightlist_20190801_20190831.csv.gz", "reorder_flightlist_20190901_20190930.csv.gz", "reorder_flightlist_20191001_20191031.csv.gz", "reorder_flightlist_20191101_20191130.csv.gz", "reorder_flightlist_20191201_20191231.csv.gz", "reorder_flightlist_20200101_20200131.csv.gz", "reorder_flightlist_20200201_20200229.csv.gz", "reorder_flightlist_20200301_20200331.csv.gz", "reorder_flightlist_20200401_20200430.csv.gz", "reorder_flightlist_20200501_20200531.csv.gz", "reorder_flightlist_20200601_20200630.csv.gz", "reorder_flightlist_20200701_20200731.csv.gz", "reorder_flightlist_20200801_20200831.csv.gz", "reorder_flightlist_20200901_20200930.csv.gz", "reorder_flightlist_20201001_20201031.csv.gz", "reorder_flightlist_20201101_20201130.csv.gz", "reorder_flightlist_20201201_20201231.csv.gz", "reorder_flightlist_20210101_20210131.csv.gz", "reorder_flightlist_20210201_20210228.csv.gz", "reorder_flightlist_20210301_20210331.csv.gz", "reorder_flightlist_20210401_20210430.csv.gz", "reorder_flightlist_20210501_20210530.csv.gz", "reorder_flightlist_20210601_20210630.csv.gz"]

    sql """ DROP TABLE IF EXISTS $tableName """

    def scriptDir = new File(getClass().protectionDomain.codeSource.location.path).parent

    sql new File("""${scriptDir}/ddl/${tableName}.sql""").text

    for (String sourceFile in sourceFiles) {
        streamLoad {
            // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', ','
            set 'compress_type', 'GZ'
            set 'max_filter_ratio', '0.1'
            set 'timeout', '3600'
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url() + '/regression/clickhouse/opensky/' + sourceFile}"""

            time 0 

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
                    // assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                    assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }
}
