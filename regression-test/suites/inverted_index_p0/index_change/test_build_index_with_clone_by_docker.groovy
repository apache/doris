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
import org.apache.doris.regression.util.NodeType
import org.apache.doris.regression.suite.SuiteCluster

suite("test_build_index_with_clone_by_docker"){
    if (isCloudMode()) {
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

    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.setFeNum(1)
    options.setBeNum(3)
    options.cloudMode = false
    def tbl = 'test_build_index_with_clone_by_docker'
    docker(options) {
        cluster.injectDebugPoints(NodeType.BE, ['EngineCloneTask.wait_clone' : null])
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
        // sleep 5s to wait for the build index job report table is unstable
        sleep(5000)
        def show_build_index = sql_return_maparray("show build index where TableName = \"${tbl}\" ORDER BY JobId DESC LIMIT 1")
        assertEquals('WAITING_TXN', show_build_index[0].State)
        assertEquals('table is unstable', show_build_index[0].Msg)

        def state = wait_for_last_build_index_on_table_finish(tbl, timeout)
        assertEquals(state, "FINISHED")
    }
}
