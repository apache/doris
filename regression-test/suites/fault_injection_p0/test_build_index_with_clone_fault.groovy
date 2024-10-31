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

suite("test_build_index_with_clone_fault_injection", "nonConcurrent"){
    if (isCloudMode()) {
        return 
    }
    def backends = sql_return_maparray('show backends')
    // if backens is less than 2, skip this case
    if (backends.size() < 2) {
        return
    }
    def timeout = 300000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_last_build_index_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() == 0) {
                logger.info(table_name + " last index job finished")
                return "SKIPPED"
            }
            if (alter_res.size() > 0) {
                def last_job_state = alter_res[alter_res.size()-1][7];
                if (last_job_state == "FINISHED" || last_job_state == "CANCELLED") {
                    sleep(10000) // wait change table state to normal
                    logger.info(table_name + " last index job finished, state: " + last_job_state + ", detail: " + alter_res)
                    return last_job_state;
                }
            }
            useTime = t
            sleep(delta_time)
        }
        logger.info("wait_for_last_build_index_on_table_finish debug: " + alter_res)
        assertTrue(useTime <= OpTimeout, "wait_for_last_build_index_on_table_finish timeout")
        return "wait_timeout"
    }

    def assertShowBuildIndexWithRetry = { tbl, expectedState, expectedMsg, maxRetries, waitSeconds ->
        int attempt = 0
        while (attempt < maxRetries) {
            def show_build_index = sql_return_maparray("show build index where TableName = \"${tbl}\" ORDER BY JobId DESC LIMIT 1")
            if (show_build_index && show_build_index.size() > 0) {
                def currentState = show_build_index[0].State
                def currentMsg = show_build_index[0].Msg
                if ((currentState == expectedState && currentMsg == expectedMsg) || currentState == "FINISHED") {
                    logger.info(currentState+" "+currentMsg)
                    return
                } else {
                    logger.warn("Attempt ${attempt + 1}: Expected State='${expectedState}' and Msg='${expectedMsg}', but got State='${currentState}' and Msg='${currentMsg}'. Retrying after ${waitSeconds} second(s)...")
                }
            } else {
                logger.warn("Attempt ${attempt + 1}: show_build_index is empty or null. Retrying after ${waitSeconds} second(s)...")
            }
            attempt++
            if (attempt < maxRetries) {
                sleep(waitSeconds * 1000)
            }
        }
        def finalBuildIndex = sql_return_maparray("show build index where TableName = \"${tbl}\" ORDER BY JobId DESC LIMIT 1")
        assertTrue(finalBuildIndex && finalBuildIndex.size() > 0, "show_build_index is empty or null after ${maxRetries} attempts")
        assertEquals(expectedState, finalBuildIndex[0].State, "State does not match after ${maxRetries} attempts")
        assertEquals(expectedMsg, finalBuildIndex[0].Msg, "Msg does not match after ${maxRetries} attempts")
    }

    def tbl = 'test_build_index_with_clone'
    try {
        GetDebugPoint().enableDebugPointForAllBEs("EngineCloneTask.wait_clone")
        logger.info("add debug point EngineCloneTask.wait_clone")
        sql """ DROP TABLE IF EXISTS ${tbl} """
        sql """
            CREATE TABLE ${tbl} (
            `k1` int(11) NULL,
            `k2` int(11) NULL
            )
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            """
        for (def i = 1; i <= 5; i++) {
            sql "INSERT INTO ${tbl} VALUES (${i}, ${10 * i})"
        }

        sql """ sync """

        // get tablets and set replica status to DROP
        def tablet = sql_return_maparray("show tablets from ${tbl}")[0]
        sql """
            ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "${tablet.TabletId}", "backend_id" = "${tablet.BackendId}", "status" = "drop");
            """
        // create index on table 
        sql """ create index idx_k2 on ${tbl}(k2) using inverted """
        sql """ build index idx_k2 on ${tbl} """

        assertShowBuildIndexWithRetry(tbl, 'WAITING_TXN', 'table is unstable', 3, 10)

        def state = wait_for_last_build_index_on_table_finish(tbl, timeout)
        assertEquals(state, "FINISHED")
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("EngineCloneTask.wait_clone")
    }
}
