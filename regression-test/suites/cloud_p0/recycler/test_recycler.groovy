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

suite("test_recycler") {
    // create table
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;

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
                    assertTrue(info.last_success_time_ms != null)
                    recyclerLastSuccessTime = Long.parseLong(info.last_success_time_ms)
                }
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

    def recycleJobType = [
        "recycle_indexes",
        "recycle_partitions",
        "recycle_tmp_rowsets",
        "recycle_rowsets",
        "recycle_tablet",
        "recycle_segment",
        "recycle_copy_jobs",
        "recycle_stage",
        "recycle_expired_stage_objects",
        "recycle_versions",
        "abort_timeout_txn"
    ]

    for (String jobType : recycleJobType) {
        logger.info("Checking metrics for job type: ${jobType}")
        checkRecycleMetrics(context.config.recycleServiceHttpAddress, jobType)
    }

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
                triggerCheckerResult = body
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
                checkJobInfoResult = body
                logger.info("checkJobInfoResult:${checkJobInfoResult}")
                assertEquals(respCode, 200)
                def info = parseJson(checkJobInfoResult.trim())
                if (info.last_finish_time_ms != null) { // Check done
                    checkerLastFinishTime = Long.parseLong(info.last_finish_time_ms)
                    assertTrue(info.last_success_time_ms != null)
                    checkerLastSuccessTime = Long.parseLong(info.last_success_time_ms)
                }
        }
    }

    do {
        triggerChecker()
        Thread.sleep(10000) // 10s
        getCheckJobInfo()
        logger.info("checkerLastFinishTime=${checkerLastFinishTime}, checkerLastSuccessTime=${checkerLastSuccessTime}")
        if (checkerLastFinishTime > recyclerLastSuccessTime) {
            break
        }
    } while (true)
    assertEquals(checkerLastFinishTime, checkerLastSuccessTime)
}
