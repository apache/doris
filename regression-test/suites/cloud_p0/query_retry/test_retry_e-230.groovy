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

suite("test_retry_e-230") {
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
    for (def j = 0; j < 2; j++) {
        docker(options) {
            def tbl = 'test_retry_e_230_tbl'
            def tbl1 = 'table_1'
            def tbl2 = 'table_2'
            sql """ DROP TABLE IF EXISTS ${tbl} """
            sql """ DROP TABLE IF EXISTS ${tbl1} """
            sql """ DROP TABLE IF EXISTS ${tbl2} """
            try {
                sql """set global experimental_enable_pipeline_x_engine=false"""
                cluster.injectDebugPoints(NodeType.BE, ['CloudTablet.capture_rs_readers.return.e-230' : null])

                sql """
                    CREATE TABLE ${tbl} (
                    `k1` int(11) NULL,
                    `k2` int(11) NULL
                    )
                    DUPLICATE KEY(`k1`, `k2`)
                    COMMENT 'OLAP'
                    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                    PROPERTIES (
                    "replication_num"="1"
                    );
                    """
                for (def i = 1; i <= 5; i++) {
                    sql "INSERT INTO ${tbl} VALUES (${i}, ${10 * i})"
                }

                cluster.injectDebugPoints(NodeType.FE, ['StmtExecutor.retry.longtime' : null])
                def futrue1 = thread {
                    Thread.sleep(3000)
                    cluster.clearBackendDebugPoints()
                }

                def begin = System.currentTimeMillis();
                def futrue2 = thread {
                    def result = try_sql """select * from ${tbl}"""
                }

                futrue2.get()
                def cost = System.currentTimeMillis() - begin;
                log.info("time cost: {}", cost)
                futrue1.get()
                // fe StmtExecutor retry time, at most 25 * 1.5s + 25 * 2.5s
                assertTrue(cost > 3000 && cost < 100000)

                sql """
                    CREATE TABLE IF NOT EXISTS ${tbl1} (
                    `siteid` int(11) NOT NULL COMMENT "",
                    `citycode` int(11) NOT NULL COMMENT "",
                    `userid` int(11) NOT NULL COMMENT "",
                    `pv` int(11) NOT NULL COMMENT ""
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`siteid`)
                    COMMENT "OLAP"
                    DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
                    PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "in_memory" = "false",
                        "storage_format" = "V2"
                    )
                """

                sql """
                    CREATE TABLE IF NOT EXISTS ${tbl2} (
                    `siteid` int(11) NOT NULL COMMENT "",
                    `citycode` int(11) NOT NULL COMMENT "",
                    `userid` int(11) NOT NULL COMMENT "",
                    `pv` int(11) NOT NULL COMMENT ""
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`siteid`)
                    COMMENT "OLAP"
                    DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
                    PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "in_memory" = "false",
                        "storage_format" = "V2"
                    )
                """

                sql """
                    insert into ${tbl1} values (9,10,11,12), (1,2,3,4)
                """

                // dp again
                cluster.injectDebugPoints(NodeType.BE, ['CloudTablet.capture_rs_readers.return.e-230' : null])

                def futrue3 = thread {
                    Thread.sleep(4000)
                    cluster.clearBackendDebugPoints()
                }

                begin = System.currentTimeMillis();
                def futrue4 = thread {
                    def result = try_sql """insert into ${tbl2} select * from ${tbl1}"""
                }

                futrue4.get()
                cost = System.currentTimeMillis() - begin;
                log.info("time cost insert into select : {}", cost)
                futrue3.get()
                // fe StmtExecutor retry time, at most 25 * 1.5s + 25 * 2.5s
                assertTrue(cost > 4000 && cost < 100000)

            } finally {
                cluster.clearFrontendDebugPoints()
                cluster.clearBackendDebugPoints()   
                sql """ DROP TABLE IF EXISTS ${tbl} """
                sql """ DROP TABLE IF EXISTS ${tbl1} """
                sql """ DROP TABLE IF EXISTS ${tbl2} """
            }
        }
        // 2. connect to follower
        options.connectToFollower = true
    }
}
