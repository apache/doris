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

suite('test_partition_serialized_sp', 'docker') {
    def resourceName = "test_partition_serialized_sp_resource"
    def policyName= "test_partition_serialized_sp_policy"
    def tableName = "test_partition_serialized_sp_table"

    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 1
    
    docker(options) {
        logger.info("Starting test_partition_serialized_sp test...")
        def check_storage_policy_exist = { name->
            def polices = sql"""
            show storage policy;
            """
            for (p in polices) {
                if (name == p[0]) {
                    return true;
                }
            }
            return false;
        }

        if (check_storage_policy_exist(policyName)) {
            sql """
                DROP STORAGE POLICY ${policyName}
            """
        }

        def has_resource = sql """
            SHOW RESOURCES WHERE NAME = "${resourceName}";
        """

        if (has_resource.size() > 0) {
            sql """
                DROP RESOURCE ${resourceName}
            """
        }

        sql """
            CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                "type"="s3",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "AWS_ROOT_PATH" = "regression/cooldown",
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_MAX_CONNECTIONS" = "50",
                "AWS_REQUEST_TIMEOUT_MS" = "3000",
                "AWS_CONNECTION_TIMEOUT_MS" = "1000",
                "AWS_BUCKET" = "${getS3BucketName()}",
                "s3_validity_check" = "false"
            );
        """

        sql """
        CREATE STORAGE POLICY ${policyName}
        PROPERTIES(
            "storage_resource" = "${resourceName}",
            "cooldown_ttl" = "1d"
        );
        """
        
        logger.info("Created storage policy: ${policyName}")

        sql "DROP TABLE IF EXISTS ${tableName}"
        
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            ts DATETIME,
            name VARCHAR(50)
        )
        PARTITION BY RANGE(ts) (
            PARTITION p1 VALUES LESS THAN ("2023-01-01 00:00:00") ("storage_policy" = "${policyName}"),
            PARTITION p2 VALUES LESS THAN ("2023-02-01 00:00:00") ("storage_policy" = "${policyName}"),
            PARTITION p3 VALUES LESS THAN ("2023-03-01 00:00:00") ("storage_policy" = "${policyName}")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """
        
        logger.info("Created partitioned table with storage policy")
        
        sql """
        INSERT INTO ${tableName} VALUES 
        (1, "2022-12-15 12:00:00", "test1"),
        (2, "2023-01-15 12:00:00", "test2"),
        (3, "2023-02-15 12:00:00", "test3");
        """
        
        def res = sql """ SHOW CREATE TABLE ${tableName} """

        assertTrue(res[0][1].contains("\"${policyName}\""))
        
        logger.info("All partitions have correct storage policy before restart")
        
        logger.info("Restarting Doris cluster...")
        cluster.restartFrontends()
        sleep(20000)
        context.reconnectFe()
        logger.info("Successfully reconnected to the frontend")
        
        res = sql """ SHOW CREATE TABLE ${tableName} """

        assertTrue(res[0][1].contains("\"${policyName}\""))
        
        def result = sql """
        SELECT * FROM ${tableName} ORDER BY id;
        """
        
        assertEquals(result.size(), 3)
    }
}