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

    docker(options) {
        def be1 = cluster.getBeByIndex(1)
        def beId = be1.backendId;

        sql """
            CREATE TABLE test_be_restart_table
                ( k1 TINYINT, k2 INT not null )
                DISTRIBUTED BY HASH(k2) BUCKETS 2 PROPERTIES ( "replication_num" = "1" );
        """
        sql """
            INSERT INTO test_be_restart_table VALUES (1, 1), (2, 2), (3, 3);
        """

        def result = sql """select * from test_be_restart_table order by k2;"""
        log.info("insert result : {}", result)
        assertEquals([[1, 1], [2, 2], [3, 3]], result)

        // this should be run at least 10 seconds
        def queryThread = Thread.start(queryTask)
        sleep(5 * 1000)
        cluster.restartBackends();
        // query should have no failure
        // wait query thread finish
        queryThread.join(15000);
    }
}

