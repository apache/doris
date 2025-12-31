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
import org.apache.doris.regression.suite.SuiteCluster

suite("test_external_ms_cluster", "docker") {

    // This test demonstrates how to use a shared MS/FDB to create multiple isolated Doris clusters.
    // Scenario:
    //  Create two clusters where the second one shares the MS/FDB cluster (without FE/BE)
    // with the first one.

    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_external_ms_cluster because not in cloud mode")
        return
    }

    def opt1 = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1)

    def opt2 = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 0, externalMsCluster: "cluster_2")

    // cluster_1 depends on cluster_2's MS/FDB
    dockers([
        "cluster_2": opt1,
        "cluster_1": opt2
    ]) { clusters ->
        connectWithDockerCluster(clusters.cluster_2) {
            // Create database and table on cluster_2
            sql "CREATE DATABASE IF NOT EXISTS test_db"
            sql "USE test_db"
            sql """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT,
                    name VARCHAR(100)
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_table VALUES (1, 'cluster1_data')"

            def result1 = sql "SELECT * FROM test_table"
            logger.info("Cluster1 data: ${result1}")
            assert result1.size() == 1
        }

        connectWithDockerCluster(clusters.cluster_1) {
            // Create different database and table on cluster_1
            sql "CREATE DATABASE IF NOT EXISTS test_db"
            sql "USE test_db"
            sql """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT,
                    value VARCHAR(100)
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_table VALUES (2, 'cluster2_data')"

            def result2 = sql "SELECT * FROM test_table"
            logger.info("Cluster2 data: ${result2}")
            assert result2.size() == 1
        }
    }
}
