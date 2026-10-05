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

suite("test_retry_e-230_async_mtmv_job", 'p0, docker') {
    if (!isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    options.enableDebugPoints()
    // one master, one observer
    options.setFeNum(2)
    options.feConfigs.add('max_query_retry_time=100')
    options.feConfigs.add('sys_log_verbose_modules=org')
    options.setBeNum(1)
    options.cloudMode = true
    // 1. connect to master
    options.connectToFollower = false

    def getMvTaskId = { idx ->
        def ret = sql_return_maparray """
            select * from tasks("type"="mv") order by CreateTime
        """
        ret[idx].TaskId
    }

    def getMvTask = { taskId ->
        def ret = sql_return_maparray """
            select * from tasks("type"="mv") where TaskId=$taskId
        """
        ret
    }

    for (def j = 0; j < 2; j++) {
        docker(options) {
            def tbl = 'async_mtmv_job_tbl'
            def tbl_view = 'async_mtmv_job_tbl_view'

            try {
                sql """
                    CREATE TABLE ${tbl} 
                        ( k2 TINYINT, k3 INT not null ) COMMENT "base table" 
                        PARTITION BY LIST(`k3`) (
                            PARTITION `p1` VALUES IN ('1'),
                            PARTITION `p2` VALUES IN ('2'), 
                        PARTITION `p3` VALUES IN ('3') ) 
                        DISTRIBUTED BY HASH(k2) BUCKETS 2 PROPERTIES ( "replication_num" = "1" );
                """
                sql """
                    INSERT INTO ${tbl} VALUES (1, 1), (2, 2), (3, 3);
                """

                def result = sql """select * from ${tbl} order by k2;"""
                log.info("insert result : {}", result)
                assertEquals([[1, 1], [2, 2], [3, 3]], result)

                sql """
                    CREATE MATERIALIZED VIEW ${tbl_view} 
                    BUILD DEFERRED REFRESH AUTO ON MANUAL
                    partition by(`k3`) DISTRIBUTED BY RANDOM BUCKETS 2 
                    PROPERTIES ( 'replication_num' = '1', 'refresh_partition_num' = '2' ) AS 
                    SELECT * from ${tbl}; 
                """

                // inject -230 in be
                cluster.injectDebugPoints(NodeType.BE, ['CloudTablet.capture_rs_readers.return.e-230' : null])
                // first refresh
                sql """
                    REFRESH MATERIALIZED VIEW ${tbl_view} AUTO
                """
                def firstTaskId = getMvTaskId(0)
                def firstTask
                awaitUntil(100) {
                    firstTask = getMvTask(firstTaskId)
                    logger.info("firstTask = {}, Status = {}, bool = {}", firstTask, firstTask.Status, firstTask.Status[0] == "FAILED") 
                    firstTask.Status[0] as String == "FAILED" as String
                }

                // due to inject -230, so after retry, task still failed
                assertTrue(firstTask.ErrorMsg[0].contains("Max retry attempts reached"))


                cluster.injectDebugPoints(NodeType.FE, ['MTMVTask.retry.longtime' : null])
                // second refresh
                sql """
                    REFRESH MATERIALIZED VIEW ${tbl_view} AUTO
                """
                // after 10s, debug point should be cleared, second should retry succ, but cost > 10s
                def futrue1 = thread {
                    Thread.sleep(50 * 1000)
                    cluster.clearBackendDebugPoints()
                }

                def begin = System.currentTimeMillis();
                def futrue2 = thread {
                    def secondTaskId = getMvTaskId(1)
                    def secondTask
                    awaitUntil(100, 5) {
                        secondTask = getMvTask(secondTaskId)
                        logger.info("secondTask = {}", secondTask) 
                        secondTask.Status[0] == "SUCCESS"
                    }
                }

                futrue2.get()
                def cost = System.currentTimeMillis() - begin;
                log.info("time cost: {}", cost)
                futrue1.get()
                assertTrue(cost > 50 * 1000)

                // check view succ
                def ret = sql """select * from $tbl_view order by k2;"""
                assertEquals([[1, 1], [2, 2], [3, 3]], ret)
            } finally {
                cluster.clearFrontendDebugPoints()
                cluster.clearBackendDebugPoints()   
            }
        }
        // 2. connect to follower
        options.connectToFollower = true
    }
}

