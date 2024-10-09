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

suite('test_abort_txn_by_fe_local3') {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.skipRunWhenPipelineDiff = false
    options.enableDebugPoints()
    options.beConfigs += [ "enable_java_support=false" ]
    options.feConfigs += [ "enable_abort_txn_by_checking_coordinator_be=false" ]
    options.feConfigs += [ "enable_abort_txn_by_checking_conflict_txn=true" ]
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

        def table= "lineorder"
        // create table if not exists
        sql new File("""${context.file.parent}/ddl/lineorder_delete.sql""").text
        sql new File("""${context.file.parent}/ddl/lineorder_create.sql""").text
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + '_' + uniqueID

        // load data from cos
        def loadSql = new File("""${context.file.parent}/ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        def coordinatorFe = cluster.getAllFrontends().get(0)
        def coordinatorFeHost = coordinatorFe.host

        sleep(5000)

        sql """ alter table ${table} modify column lo_suppkey bigint NULL """
        
        String result = ""
        int max_try_time = 3000
        while (max_try_time--){
            result = getJobState(table)
            if (result == "PENDING") {
                sleep(3000)
            } else {
                break;
            }
        }
        if (max_try_time < 1){
            assertEquals(1,2)
        }
        sleep 10000
        assertEquals(result, "WAITING_TXN");

        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        max_try_time = 3000
        while (max_try_time--){
            result = getJobState(table)
            System.out.println(result)
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
