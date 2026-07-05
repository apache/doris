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

    // Flush and poll audit rows through a separate connection so diagnostics do not
    // inherit the tested workload group or change the primary assertion result.
    def fetchAuditRowsForMarker = { queryMarker ->
        return connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql """SET workload_group = ''"""
            sql """SET enable_sql_cache = false"""
            sql """CALL flush_audit_log()"""
            int retry = 20
            def auditRows = []
            String auditSql = """
                SELECT time, state, error_message, scan_bytes, scan_rows,
                       scan_bytes_from_remote_storage, scan_bytes_from_local_storage,
                       workload_group, stmt
                FROM __internal_schema.audit_log
                WHERE stmt LIKE '%${queryMarker}%'
                ORDER BY time DESC
                LIMIT 5
            """
            while (retry-- >= 0) {
                auditRows = sql auditSql
                if (!auditRows.isEmpty()) {
                    return auditRows
                }
                sleep(3000)
                sql """CALL flush_audit_log()"""
            }
            return auditRows
        }
    }

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

        waitForBePolicyPublish()

        Throwable queryException = null
        // Tag the target query so diagnostics can fetch the matching audit rows on failure.
        String queryMarker = "remote_scan_probe_" + System.currentTimeMillis()
        sql """SET workload_group = '${workloadGroupName}'"""
        sql """SET enable_file_cache = false"""
        sql """SET enable_sql_cache = false"""
        try {
            sql """
                SELECT /* ${queryMarker} */ SUM(SLEEP(1) + l_quantity)
                FROM (
                    SELECT l_quantity
                    FROM ${catalogName}.${lineitemDb}.lineitem
                    LIMIT 10
                ) s
            """
        } catch (Throwable t) {
            queryException = t
        }

        String msg = queryException == null ? null : queryException.getMessage()
        boolean cancelledByExpectedPolicy = msg != null
                && msg.contains("cancelled by workload policy: ${policyName}")
                && msg.contains("scan_bytes_from_remote_storage")
        if (!cancelledByExpectedPolicy) {
            // Leave the tested workload group and remove the policy before running diagnostics.
            sql """SET workload_group = ''"""
            sql """DROP WORKLOAD POLICY IF EXISTS ${policyName}"""
            def auditRows = fetchAuditRowsForMarker(queryMarker)
            logger.info("Remote scan bytes workload policy audit rows for ${queryMarker}: " + auditRows)
        }

        assertTrue(queryException != null, "query should be cancelled by remote scan bytes workload policy")
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
}
