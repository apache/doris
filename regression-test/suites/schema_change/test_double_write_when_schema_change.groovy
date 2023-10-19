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
suite("double_write_schema_change") {

    // ssb_sf1_p1 is writted to test unique key table merge correctly.
    // It creates unique key table and sets bucket num to 1 in order to make sure that
    // many rowsets will be created during loading and then the merge process will be triggered.

    def tableName = "lineorder"
    def columns = """lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority, 
                    lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount, 
                    lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy"""

    sql new File("""${context.file.parent}/ddl/${tableName}_delete.sql""").text
    sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

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
        set 'columns', columns


        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file """${getS3Url()}/regression/ssb/sf1/${tableName}.tbl.gz"""

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

    def getJobState = { indexName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${indexName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    def insert_sql = """ insert into ${tableName} values(100000000, 1, 1, 1, 1, 1, "1", 1, 1, 1, 1, 1, 1, 1, 1, 1, "1") """

    sql """ ALTER TABLE ${tableName} modify COLUMN lo_custkey double"""
    int max_try_time = 3000
    while (max_try_time--){
        String result = getJobState(tableName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            if (result == "RUNNING") {
                sql insert_sql
            }
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
}
