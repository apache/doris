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

suite('test_abort_txn_by_fe', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = null
    options.enableDebugPoints()
    options.beConfigs += [ "enable_java_support=false" ]
    options.feConfigs += [
        "load_checker_interval_second=2",
        "enable_abort_txn_by_checking_coordinator_be=false",
        "enable_abort_txn_by_checking_conflict_txn=true",
    ]
    options.feNum = 3
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

        if (isCloudMode()) {
            sleep 6000
        } else {
            def dbId = getDbId()
            dockerAwaitUntil(20, {
                def txns = sql_return_maparray("show proc '/transactions/${dbId}/running'")
                txns.any { it.Label == loadLabel }
            })
        }

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

        def oldMasterFe = cluster.getMasterFe()
        cluster.restartFrontends(oldMasterFe.index)
        boolean hasRestart = false
        for (int i = 0; i < 30; i++) {
            if (cluster.getFeByIndex(oldMasterFe.index).alive) {
                hasRestart = true
                break
            }
            sleep 1000
        }
        assertTrue(hasRestart)
        context.reconnectFe()
        if (!isCloudMode()) {
            def newMasterFe = cluster.getMasterFe()
            assertTrue(oldMasterFe.index != newMasterFe.index)
        }

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
