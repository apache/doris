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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.http.NoHttpResponseException

suite('test_abort_txn_by_be_cloud1', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += [ "enable_java_support=false" ]
    options.feConfigs += [ "enable_abort_txn_by_checking_coordinator_be=true" ]
    options.feConfigs += [ "enable_abort_txn_by_checking_conflict_txn=false" ]
    options.beNum = 1

    docker(options) {
        def getJobState = { tableName ->
            def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            return jobStateResult[0][9]
        }
        
        def s3BucketName = getS3BucketName()
        def s3WithProperties = """WITH S3 (
            |"AWS_ACCESS_KEY" = "${getS3AK()}",
            |"AWS_SECRET_KEY" = "${getS3SK()}",
            |"AWS_ENDPOINT" = "${getS3Endpoint()}",
            |"AWS_REGION" = "${getS3Region()}",
            |"provider" = "${getS3Provider()}")
            |PROPERTIES(
            |"exec_mem_limit" = "8589934592",
            |"load_parallelism" = "3")""".stripMargin()

        // set fe configuration
        sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"

        def tableName = "lineorder"
        // create table if not exists
        sql new File("""${context.file.parent}/ddl/lineorder_delete.sql""").text
        sql new File("""${context.file.parent}/ddl/lineorder_create.sql""").text

        def coordinatorBe = cluster.getAllBackends().get(0)
        def coordinatorBeHost = coordinatorBe.host
        
        def column =  """lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority, 
                lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount, 
                lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy"""

        thread {
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
                set 'columns', column


                // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
                // also, you can stream load a http stream, e.g. http://xxx/some.csv
                file """${getS3Url()}/regression/ssb/sf100/lineorder.tbl.1.gz"""

                time 600 * 1000

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
        }

        sleep(10000)

        sql """ alter table ${tableName} modify column lo_suppkey bigint NULL """

        String result = ""
        int max_try_time = 3000
        while (max_try_time--){
            result = getJobState(tableName)
            if (result == "PENDING") {
                sleep(3000)
            } else {
                break;
            }
        }
        if (max_try_time < 1){
            assertEquals(1,2)
        }
        assertEquals(result, "WAITING_TXN");

        cluster.stopBackends(coordinatorBe.index)
        def isDead = false
        for (def i = 0; i < 10; i++) {
            def be = sql_return_maparray('show backends').find { it.Host == coordinatorBeHost }
            if (!be.Alive.toBoolean()) {
                isDead = true
                break
            }
            sleep 1000
        }
        assertTrue(isDead)
        sleep 10000

        result = getJobState(tableName)
        assertEquals(result, "WAITING_TXN");

        // coordinatorBe restart, abort txn on it
        cluster.startBackends(coordinatorBe.index)
        def isAlive = false
        for (def i = 0; i < 20; i++) {
            def be = sql_return_maparray('show backends').find { it.Host == coordinatorBeHost }
            if (be.Alive.toBoolean()) {
                isAlive = true
                break
            }
            sleep 1000
        }
        assertTrue(isAlive)
        sleep 20000

        max_try_time = 3000
        while (max_try_time--){
            result = getJobState(tableName)
            if (result == "FINISHED") {
                sleep(3000)
                break
            } else {
                sleep(100)
                if (max_try_time < 1){
                    assertEquals(1,2)
                }
            }
        }
    }
}
