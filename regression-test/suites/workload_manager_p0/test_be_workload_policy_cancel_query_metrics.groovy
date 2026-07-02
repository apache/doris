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

    // Read and later restore the shared FE publish interval so the suite can
    // shrink BE policy propagation latency without leaving global state behind.
    def getFrontendConfigValue = { configKey ->
        def configRows = sql """ADMIN SHOW FRONTEND CONFIG LIKE '${configKey}'"""
        assertEquals(1, configRows.size())
        return configRows[0][1].toString()
    }

    // Wait long enough for the BE-side policy to become effective on the target cluster.
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

    // Create one BE-side cancel_query policy at a time so the metric under test
    // is the only possible cancellation reason within the dedicated workload group.
    def createPolicyAndAssertCancellation = { policyName, conditionSql, querySql, metricName ->
        sql """DROP WORKLOAD POLICY IF EXISTS ${policyName}"""
        try {
            sql """
                CREATE WORKLOAD POLICY ${policyName}
                CONDITIONS(${conditionSql})
                ACTIONS(cancel_query)
                PROPERTIES(
                    'priority' = '100',
                    'workload_group' = '${currentCgName}${workloadGroupName}'
                )
            """
            waitForBePolicyPublish()
            sql """SET workload_group = '${workloadGroupName}'"""
            sql """SET enable_sql_cache = false"""
            assertCancelledByPolicy(querySql, policyName, metricName)
        } finally {
            sql """DROP WORKLOAD POLICY IF EXISTS ${policyName}"""
        }
    }

    String originalPublishTopicInfoIntervalMs = getFrontendConfigValue("publish_topic_info_interval_ms")
    try {
        sql """ADMIN SET FRONTEND CONFIG("publish_topic_info_interval_ms" = "1000")"""
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

            sql """DROP WORKLOAD GROUP IF EXISTS ${workloadGroupName} ${forComputeGroupStr}"""
            sql """
                CREATE WORKLOAD GROUP ${workloadGroupName} ${forComputeGroupStr}
                PROPERTIES ('max_cpu_percent' = '100')
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

            createPolicyAndAssertCancellation(
                    "test_be_cancel_query_time_policy",
                    "query_time > 1",
                    scanQuerySql,
                    "query_time")
            createPolicyAndAssertCancellation(
                    "test_be_cancel_query_scan_rows_policy",
                    "be_scan_rows > 1",
                    scanQuerySql,
                    "scan_rows")
            createPolicyAndAssertCancellation(
                    "test_be_cancel_query_scan_bytes_policy",
                    "be_scan_bytes > 1",
                    scanQuerySql,
                    "scan_bytes")
            createPolicyAndAssertCancellation(
                    "test_be_cancel_query_memory_policy",
                    "query_be_memory_bytes > 1",
                    memoryQuerySql,
                    "query_memory")
        } finally {
            try {
                sql """SET workload_group = ''"""
            } catch (Throwable t) {
                logger.info("ignore reset workload_group failure: " + t.getMessage())
            }
            sql """DROP WORKLOAD GROUP IF EXISTS ${workloadGroupName} ${forComputeGroupStr}"""
        }
    } finally {
        sql """ADMIN SET FRONTEND CONFIG("publish_topic_info_interval_ms" = "${originalPublishTopicInfoIntervalMs}")"""
    }
}
