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

suite("test_be_workload_policy_cancel_query_metrics") {
    String dbName = context.config.getDbNameByFile(context.file)
    String workloadGroupName = "test_be_cancel_query_metrics_wg"
    String forComputeGroupStr = ""
    String currentCgName = ""
    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        String validCluster = clusters[0][0]
        currentCgName = "${validCluster}."
        forComputeGroupStr = " for ${validCluster} "
    }

    // Wait for the default FE->BE publish cycle instead of mutating a shared FE config.
    def waitForBePolicyPublish = {
        Thread.sleep(40000)
    }

    // Verify that the query was cancelled by the expected BE policy and that
    // the cancellation message exposes the metric that triggered it.
    def assertCancelledByPolicy = { querySql, policyName, metricName ->
        Throwable queryException = null
        try {
            sql querySql
        } catch (Throwable t) {
            queryException = t
        }
        assertTrue(queryException != null, "query should be cancelled by workload policy ${policyName}")
        String msg = queryException.getMessage()
        logger.info("BE workload policy cancel message for ${policyName}: ${msg}")
        assertTrue(msg != null && msg.contains("cancelled by workload policy: ${policyName}"),
                "unexpected cancel policy: " + msg)
        assertTrue(msg.contains(metricName), "missing trigger metric ${metricName}: " + msg)
    }

    // Build isolated workload groups so all policies can be published once and verified independently.
    def policyCases = [
            [
                    workloadGroupName: "${workloadGroupName}_time",
                    policyName      : "test_be_cancel_query_time_policy",
                    conditionSql    : "query_time > 1",
                    metricName      : "query_time"
            ],
            [
                    workloadGroupName: "${workloadGroupName}_scan_rows",
                    policyName      : "test_be_cancel_query_scan_rows_policy",
                    conditionSql    : "be_scan_rows > 1",
                    metricName      : "scan_rows"
            ],
            [
                    workloadGroupName: "${workloadGroupName}_scan_bytes",
                    policyName      : "test_be_cancel_query_scan_bytes_policy",
                    conditionSql    : "be_scan_bytes > 1",
                    metricName      : "scan_bytes"
            ],
            [
                    workloadGroupName: "${workloadGroupName}_memory",
                    policyName      : "test_be_cancel_query_memory_policy",
                    conditionSql    : "query_be_memory_bytes > 1",
                    metricName      : "query_memory"
            ]
    ]

    try {
        sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
        sql """DROP TABLE IF EXISTS ${dbName}.test_be_workload_policy_cancel_query_metrics_tbl"""
        sql """
            CREATE TABLE ${dbName}.test_be_workload_policy_cancel_query_metrics_tbl (
                id INT,
                payload STRING
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql """
            INSERT INTO ${dbName}.test_be_workload_policy_cancel_query_metrics_tbl
            SELECT number, REPEAT('x', 4096)
            FROM numbers("number" = "200")
        """

        // Use a scan-heavy query for time/rows/bytes and a sort-heavy query for
        // memory so the BE runtime counters can trip the dedicated policy.
        String scanQuerySql = """
            SELECT SUM(SLEEP(1) + id)
            FROM (
                SELECT id
                FROM ${dbName}.test_be_workload_policy_cancel_query_metrics_tbl
                ORDER BY id
                LIMIT 10
            ) s
        """
        String memoryQuerySql = """
            SELECT SUM(SLEEP(1) + payload_len)
            FROM (
                SELECT LENGTH(payload) AS payload_len
                FROM ${dbName}.test_be_workload_policy_cancel_query_metrics_tbl
                ORDER BY payload
                LIMIT 10
            ) s
        """

        policyCases.each { policyCase ->
            sql """DROP WORKLOAD POLICY IF EXISTS ${policyCase.policyName}"""
            sql """DROP WORKLOAD GROUP IF EXISTS ${policyCase.workloadGroupName} ${forComputeGroupStr}"""
            sql """
                CREATE WORKLOAD GROUP ${policyCase.workloadGroupName} ${forComputeGroupStr}
                PROPERTIES ('max_cpu_percent' = '100')
            """
        }

        // Publish all BE-side policies together and reuse the same propagation wait.
        policyCases.each { policyCase ->
            sql """
                CREATE WORKLOAD POLICY ${policyCase.policyName}
                CONDITIONS(${policyCase.conditionSql})
                ACTIONS(cancel_query)
                PROPERTIES(
                    'priority' = '100',
                    'workload_group' = '${currentCgName}${policyCase.workloadGroupName}'
                )
            """
        }

        waitForBePolicyPublish()

        // Switch workload groups between assertions so each query can only match one policy.
        policyCases.each { policyCase ->
            sql """SET workload_group = '${policyCase.workloadGroupName}'"""
            sql """SET enable_sql_cache = false"""
            String querySql = policyCase.metricName == "query_memory" ? memoryQuerySql : scanQuerySql
            assertCancelledByPolicy(querySql, policyCase.policyName, policyCase.metricName)
        }
    } finally {
        try {
            sql """SET workload_group = ''"""
        } catch (Throwable t) {
            logger.info("ignore reset workload_group failure: " + t.getMessage())
        }
        policyCases.each { policyCase ->
            sql """DROP WORKLOAD POLICY IF EXISTS ${policyCase.policyName}"""
            sql """DROP WORKLOAD GROUP IF EXISTS ${policyCase.workloadGroupName} ${forComputeGroupStr}"""
        }
    }
}
