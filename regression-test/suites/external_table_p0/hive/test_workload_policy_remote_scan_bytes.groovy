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

suite("test_workload_policy_remote_scan_bytes", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    String hivePrefix = "hive2"
    setHivePrefix(hivePrefix)

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
    String catalogName = "test_workload_policy_remote_scan_bytes"
    String workloadGroupName = "test_remote_scan_bytes_wg"
    String policyName = "test_remote_scan_bytes_policy"
    String invalidPolicyName = "test_remote_scan_bytes_invalid"
    // Lower the FE->BE publish interval during this suite so the BE-side policy
    // becomes visible without waiting for the default 30s publish window.
    def getFrontendConfigValue = { configKey ->
        def configRows = sql """ADMIN SHOW FRONTEND CONFIG LIKE '${configKey}'"""
        assertEquals(1, configRows.size())
        return configRows[0][1].toString()
    }
    String originalPublishTopicInfoIntervalMs = getFrontendConfigValue("publish_topic_info_interval_ms")

    String forComputeGroupStr = ""
    String currentCgName = ""
    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        String validCluster = clusters[0][0]
        currentCgName = "${validCluster}."
        forComputeGroupStr = " for ${validCluster} "
    }

    try {
        // Use a shorter publish interval in the shared regression environment and
        // restore it in finally to avoid leaving global FE state behind.
        sql """ADMIN SET FRONTEND CONFIG("publish_topic_info_interval_ms" = "1000")"""
        try {
            sql """DROP WORKLOAD POLICY IF EXISTS ${policyName}"""
            sql """DROP WORKLOAD POLICY IF EXISTS ${invalidPolicyName}"""
            sql """DROP WORKLOAD GROUP IF EXISTS ${workloadGroupName} ${forComputeGroupStr}"""
            sql """DROP CATALOG IF EXISTS ${catalogName}"""

            sql """
                CREATE CATALOG ${catalogName} PROPERTIES (
                    'type' = 'hms',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                    'hadoop.username' = 'hive',
                    'ipc.client.fallback-to-simple-auth-allowed' = 'true'
                )
            """

            String lineitemDb = "tpch1_parquet"
            try {
                def tables = sql """SHOW TABLES FROM ${catalogName}.${lineitemDb} LIKE 'lineitem'"""
                if (tables.isEmpty()) {
                    throw new IllegalStateException("${lineitemDb}.lineitem does not exist")
                }
            } catch (Throwable ignored) {
                lineitemDb = "tpch1"
                def tables = sql """SHOW TABLES FROM ${catalogName}.${lineitemDb} LIKE 'lineitem'"""
                assertFalse(tables.isEmpty(), "${catalogName} does not contain tpch1_parquet.lineitem or tpch1.lineitem")
            }

            sql """
                CREATE WORKLOAD GROUP ${workloadGroupName} ${forComputeGroupStr}
                PROPERTIES ('max_cpu_percent' = '100')
            """

            test {
                sql """
                    CREATE WORKLOAD POLICY ${invalidPolicyName}
                    CONDITIONS(be_scan_bytes_from_remote_storage > -1)
                    ACTIONS(cancel_query)
                    PROPERTIES('enabled' = 'false')
                """
                exception "invalid remote scan bytes value"
            }

            sql """
                CREATE WORKLOAD POLICY ${policyName}
                CONDITIONS(be_scan_bytes_from_remote_storage > 1)
                ACTIONS(cancel_query)
                PROPERTIES(
                    'priority' = '100',
                    'workload_group' = '${currentCgName}${workloadGroupName}'
                )
            """

            def policy = sql """
                SELECT name, condition, action, priority, enabled, workload_group
                FROM information_schema.workload_policy
                WHERE name = '${policyName}'
            """
            assertEquals(1, policy.size())
            assertEquals(policyName, policy[0][0])
            assertTrue(policy[0][1].toString().contains("be_scan_bytes_from_remote_storage > 1"))

            // Wait long enough for the BE-side policy to become effective on the target cluster.
            Thread.sleep(40000)

            Throwable queryException = null
            sql """SET workload_group = '${workloadGroupName}'"""
            sql """SET enable_file_cache = false"""
            sql """SET enable_sql_cache = false"""
            try {
                sql """
                    SELECT SUM(SLEEP(1) + l_quantity)
                    FROM (
                        SELECT l_quantity
                        FROM ${catalogName}.${lineitemDb}.lineitem
                        LIMIT 10
                    ) s
                """
            } catch (Throwable t) {
                queryException = t
            }
            assertTrue(queryException != null, "query should be cancelled by remote scan bytes workload policy")
            String msg = queryException.getMessage()
            logger.info("Remote scan bytes workload policy cancel message: " + msg)
            assertTrue(msg != null && msg.contains("cancelled by workload policy: ${policyName}"),
                    "unexpected cancel policy: " + msg)
            assertTrue(msg.contains("scan_bytes_from_remote_storage"),
                    "remote scan bytes counter is missing from cancel message: " + msg)
        } finally {
            try {
                sql """SET workload_group = ''"""
            } catch (Throwable t) {
                logger.info("ignore reset workload_group failure: " + t.getMessage())
            }
            sql """DROP WORKLOAD POLICY IF EXISTS ${policyName}"""
            sql """DROP WORKLOAD POLICY IF EXISTS ${invalidPolicyName}"""
            sql """DROP WORKLOAD GROUP IF EXISTS ${workloadGroupName} ${forComputeGroupStr}"""
            sql """DROP CATALOG IF EXISTS ${catalogName}"""
        }
    } finally {
        sql """ADMIN SET FRONTEND CONFIG("publish_topic_info_interval_ms" = "${originalPublishTopicInfoIntervalMs}")"""
    }
}
