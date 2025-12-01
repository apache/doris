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
import groovy.json.JsonOutput
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_recycler_cleanup_snapshot") {
    def enableMultiVersionStatus = context.config.enableMultiVersionStatus
    def enableClusterSnapshot = context.config.enableClusterSnapshot

    if(!enableMultiVersionStatus || !enableClusterSnapshot) {
        logger.info("enableMultiVersionStatus or enableClusterSnapshot is not true, skip cleanup snapshot")
        return
    }

    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId;
    def dbName = context.config.getDbNameByFile(context.file);
    def caseStartTime = System.currentTimeMillis()
    def recyclerLastSuccessTime = -1
    def recyclerLastFinishTime = -1

    // Make sure to complete at least one round of recycling
    def getRecycleJobInfo = {
        def recycleJobInfoApi = { checkFunc ->
            httpTest {
                endpoint context.config.recycleServiceHttpAddress
                uri "/RecyclerService/http/recycle_job_info?token=$token&instance_id=$instanceId"
                op "get"
                check checkFunc
            }
        }
        recycleJobInfoApi.call() {
            respCode, body ->
                logger.info("http cli result: ${body} ${respCode}")
                def recycleJobInfoResult = body
                logger.info("recycleJobInfoResult:${recycleJobInfoResult}")
                assertEquals(respCode, 200)
                def info = parseJson(recycleJobInfoResult.trim())
                if (info.last_finish_time_ms != null) {
                    recyclerLastFinishTime = Long.parseLong(info.last_finish_time_ms)
                }
                if (info.last_success_time_ms != null) {
                    recyclerLastSuccessTime = Long.parseLong(info.last_success_time_ms)
                }
        }
    }

    def getCreatedSnapshot = {
        def snapshotInfo = sql """ SELECT * FROM information_schema.cluster_snapshots """
        logger.info("snapshotInfo:${snapshotInfo}")
        return snapshotInfo
    }

    def deleteCreatedSnapshot = {
        def snapshotList = getCreatedSnapshot()
        for (snapshot : snapshotList) {
            sql """ ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${snapshot.snapshot_id}'; """
        }
    }

    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(10000)
        getRecycleJobInfo()
        logger.info("caseStartTime=${caseStartTime}, recyclerLastSuccessTime=${recyclerLastSuccessTime}")
        if (recyclerLastFinishTime > caseStartTime) {
            break
        }
    } while (true)
    assertEquals(recyclerLastFinishTime, recyclerLastSuccessTime)

    // Make sure to complete at least one round of checking
    def checkerLastSuccessTime = -1
    def checkerLastFinishTime = -1

    def triggerChecker = {
        def triggerCheckerApi = { checkFunc ->
            httpTest {
                endpoint context.config.recycleServiceHttpAddress
                uri "/RecyclerService/http/check_instance?token=$token&instance_id=$instanceId"
                op "get"
                check checkFunc
            }
        }
        triggerCheckerApi.call() {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                def triggerCheckerResult = body
                logger.info("triggerCheckerResult:${triggerCheckerResult}".toString())
                assertTrue(triggerCheckerResult.trim().equalsIgnoreCase("OK"))
        }
    }
    def getCheckJobInfo = {
        def checkJobInfoApi = { checkFunc ->
            httpTest {
                endpoint context.config.recycleServiceHttpAddress
                uri "/RecyclerService/http/check_job_info?token=$token&instance_id=$instanceId"
                op "get"
                check checkFunc
            }
        }
        checkJobInfoApi.call() {
            respCode, body ->
                logger.info("http cli result: ${body} ${respCode}")
                def checkJobInfoResult = body
                logger.info("checkJobInfoResult:${checkJobInfoResult}")
                assertEquals(respCode, 200)
                def info = parseJson(checkJobInfoResult.trim())
                if (info.last_finish_time_ms != null) { // Check done
                    checkerLastFinishTime = Long.parseLong(info.last_finish_time_ms)
                }
                if(info.last_success_time_ms != null) {
                    checkerLastSuccessTime = Long.parseLong(info.last_success_time_ms)
                }
        }
    }
    
    // collect all tables of tablet ids and tablet names and drop table
    def allTabletIds = []
    def allTableNames = []
    def allTables = sql " SHOW TABLES FROM ${dbName}"

    for (tableInfo : allTables) {
        def table_name = tableInfo[0]

        def tabletIds = sql " SHOW TABLETS FROM ${dbName}.${table_name}"
        for (tabletInfo : tabletIds) {
            allTabletIds.add(tabletInfo[0])
        }
        allTableNames.add(table_name)

        logger.info("table name: ${table_name}, tablet ids: ${allTabletIds}")
        sql " DROP TABLE IF EXISTS ${dbName}.${table_name} FORCE"
    }

    deleteCreatedSnapshot()

    def retry = 15
    def success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000) // 20s
        if (checkRecycleTable(token, instanceId, cloudUniqueId, "", allTabletIds, false)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)
}
