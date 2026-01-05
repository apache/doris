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

suite("test_retry_be_restart", "p0, docker") {
    if (!isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.setFeNum(1)
    options.feConfigs.add('max_query_retry_time=100')
    options.feConfigs.add('sys_log_verbose_modules=org')
    options.setBeNum(1)
    options.cloudMode = true
    // 1. connect to master
    options.connectToFollower = false

    def queryTask = {
        for (int i = 0; i < 100; i++) {
            try {
                log.info("query count: {}", i)
                sql """select * from test_be_restart_table"""
                Thread.sleep(100)
            } catch (Exception e) {
                logger.warn("select failed: ${e.message}")
                assertFalse(true);
            }
        }
    }

    def pointSelectQueryTask = {
        for (int i = 0; i < 100; i++) {
            try {
                log.info("query count: {}", i)
                sql """select * from test_be_restart_table where account=1 and site_code=1"""
                Thread.sleep(100)
            } catch (Exception e) {
                logger.warn("select failed: ${e.message}")
                assertFalse(true);
            }
        }
    }

    docker(options) {
        def be1 = cluster.getBeByIndex(1)
        def beId = be1.backendId;

        sql """
            CREATE TABLE IF NOT EXISTS `test_be_restart_table`
            (
                `account` bigint NULL COMMENT '用户ID',
                `site_code` int NULL COMMENT '站点代码',
                `site_code_str` varchar(64) NOT NULL DEFAULT "" COMMENT 'string类型站点编号,查询返回数据使用',
                `register_time` datetime(3) NULL COMMENT '注册时间，tidb中为int,需要转换',
                `increment_no` bigint NOT NULL AUTO_INCREMENT(1),
                `currency` varchar(65533) NULL COMMENT '币种'
            )
            ENGINE=OLAP
            UNIQUE KEY(`account`, `site_code`)
            PARTITION BY LIST(`site_code`)
            (
                PARTITION p_1 VALUES IN (1),
                PARTITION p_2 VALUES IN (2)
            )
            DISTRIBUTED BY HASH(`account`) BUCKETS 8
            PROPERTIES (
                "binlog.enable" = "true",
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """
        sql """
            INSERT INTO test_be_restart_table VALUES (1, 1, '1', '2026-01-01 00:00:00', 1, 'USD');
        """
        sql """
            INSERT INTO test_be_restart_table VALUES (2, 2, '2', '2026-01-01 00:00:00', 2, 'EUR');
        """
        sql """
            INSERT INTO test_be_restart_table VALUES (3, 1, '3', '2026-01-01 00:00:00', 3, 'GBP');
        """

        def result = sql """select account, site_code from test_be_restart_table order by account, site_code;"""
        log.info("insert result : {}", result)
        assertEquals([[1L, 1], [2L, 2], [3L, 1]], result)
        cluster.injectDebugPoints(NodeType.FE, ['StmtExecutor.retry.longtime' : null])
        // this should be run at least 10 seconds
        def queryThread = Thread.start(queryTask)
        def pointSelectQueryThread = Thread.start(pointSelectQueryTask)
        sleep(5 * 1000)
        cluster.restartBackends()
        // query should have no failure
        // wait query thread finish
        queryThread.join(15000)
        pointSelectQueryThread.join(15000)
    }
}

