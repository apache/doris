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
import com.mysql.cj.jdbc.StatementImpl
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType
import org.apache.doris.regression.suite.SuiteCluster

suite("test_retry_e-230", 'docker') {
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

    def insert_sql = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'VISIBLE'"))
    }
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
                    insert_sql """INSERT INTO ${tbl} VALUES (${i}, ${10 * i})""", 1
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

                insert_sql """INSERT INTO ${tbl1} VALUES (9,10,11,12), (1,2,3,4)""", 2

                // dp again
                cluster.injectDebugPoints(NodeType.BE, ['CloudTablet.capture_rs_readers.return.e-230' : null])

                cluster.clearFrontendDebugPoints()
                try {
                    sql """insert into ${tbl2} select * from ${tbl1}"""
                    assertFalse(true)
                } catch (Exception e) {
                    logger.info("Received expected exception when insert into select: {}", e.getMessage())
                    assert e.getMessage().contains("[E-230]injected error"), "Unexpected exception message when insert into select"
                }
                
                cluster.injectDebugPoints(NodeType.FE, ['StmtExecutor.retry.longtime' : null]) 

                def futrue3 = thread {
                    Thread.sleep(4000)
                    cluster.clearBackendDebugPoints()
                }

                begin = System.currentTimeMillis();
                def futrue4 = thread {
                    insert_sql """insert into ${tbl2} select * from ${tbl1}""", 2
                }

                futrue4.get()
                cost = System.currentTimeMillis() - begin;
                log.info("time cost insert into select : {}", cost)
                futrue3.get()
                def tbl1Ret = sql_return_maparray """select * from ${tbl1}"""
                log.info("tbl1 ret {}", tbl1Ret)
                def tbl2Ret = sql_return_maparray """select * from ${tbl2}"""
                log.info("tbl2 ret {}", tbl2Ret)
                // Compare the results from both tables
                assertEquals(tbl1Ret, tbl2Ret, "Data in ${tbl1} and ${tbl2} should be identical")
                // fe StmtExecutor retry time, at most 25 * 1.5s + 25 * 2.5s
                assertTrue(cost > 4000 && cost < 100000)

            } finally {
                cluster.clearFrontendDebugPoints()
                cluster.clearBackendDebugPoints()   
            }
        }
        // 2. connect to follower
        options.connectToFollower = true
    }
}
