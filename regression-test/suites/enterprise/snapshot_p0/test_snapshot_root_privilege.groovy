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

suite("test_snapshot_root_privilege", "snapshot,docker,auth") {
    if (!isCloudMode()) {
        logger.info("Skip test_snapshot_root_privilege because not in cloud mode")
        return
    }

    def adminUser = 'test_snapshot_admin_user'
    def adminPwd = 'C123_567p'
    def normalUser = 'test_snapshot_normal_user'
    def normalPwd = 'C123_567p'

    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        feConfigs: [
            "cloud_auto_snapshot_min_interval_seconds=5",
        ],
        beConfigs: [
            "delete_bitmap_store_write_version=3",
            "delete_bitmap_store_read_version=3",
        ],
        msConfigs: [
            "enable_split_rowset_meta=true",
            "enable_split_tablet_schema_pb=true",
            "enable_multi_version_status=true",
            "multi_version_status_check_interval_seconds=1",
            "snapshot_min_interval_seconds=5"
        ],
        recycleConfigs: [
            "recycle_interval_seconds=1",
            "recycler_sleep_before_scheduling_seconds=1",
            "enable_snapshot_data_migrator=true",
        ])

    docker(opt) { ->
        // Get docker FE jdbc url
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")
        def feUrl = "jdbc:mysql://${fes[0].Host}:${fes[0].QueryPort}/"
        logger.info("Docker FE JDBC URL: " + feUrl)

        // Create a user with ADMIN privilege (not root)
        try_sql("DROP USER ${adminUser}")
        sql """CREATE USER '${adminUser}' IDENTIFIED BY '${adminPwd}'"""
        sql """GRANT ADMIN_PRIV ON *.*.* TO '${adminUser}'"""

        // Create a normal user without any special privilege
        try_sql("DROP USER ${normalUser}")
        sql """CREATE USER '${normalUser}' IDENTIFIED BY '${normalPwd}'"""
        sql """GRANT SELECT_PRIV ON *.*.* TO '${normalUser}'"""

        // Grant cluster usage for cloud mode
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${adminUser}'"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${normalUser}'"""

        // ========================
        // Part 1: Test root mode (default)
        // ========================
        logger.info("=== Part 1: Testing root mode (default) ===")

        // Ensure config is set to root mode
        sql """ADMIN SET FRONTEND CONFIG ("cluster_snapshot_min_privilege" = "root")"""

        // Test 1: Admin user cannot execute ADMIN SET CLUSTER SNAPSHOT FEATURE command
        logger.info("Test 1: Admin user cannot execute ADMIN SET CLUSTER SNAPSHOT FEATURE command (root mode)")
        connect(adminUser, adminPwd, feUrl) {
            try {
                sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
                fail("Expected exception for admin user executing ADMIN SET CLUSTER SNAPSHOT FEATURE in root mode")
            } catch (Exception e) {
                logger.info("Expected error: " + e.getMessage())
                assertTrue(e.getMessage().contains("root privilege"))
            }
        }

        // Test 2: Admin user cannot execute ADMIN CREATE CLUSTER SNAPSHOT command
        logger.info("Test 2: Admin user cannot execute ADMIN CREATE CLUSTER SNAPSHOT command (root mode)")
        connect(adminUser, adminPwd, feUrl) {
            try {
                sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'test_snapshot')"
                fail("Expected exception for admin user executing ADMIN CREATE CLUSTER SNAPSHOT in root mode")
            } catch (Exception e) {
                logger.info("Expected error: " + e.getMessage())
                assertTrue(e.getMessage().contains("root privilege"))
            }
        }

        // Test 3: Admin user cannot execute ADMIN SET AUTO CLUSTER SNAPSHOT command
        logger.info("Test 3: Admin user cannot execute ADMIN SET AUTO CLUSTER SNAPSHOT command (root mode)")
        connect(adminUser, adminPwd, feUrl) {
            try {
                sql "ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES('max_reserved_snapshots'='2')"
                fail("Expected exception for admin user executing ADMIN SET AUTO CLUSTER SNAPSHOT in root mode")
            } catch (Exception e) {
                logger.info("Expected error: " + e.getMessage())
                assertTrue(e.getMessage().contains("root privilege"))
            }
        }

        // Test 4: Admin user cannot execute ADMIN DROP CLUSTER SNAPSHOT command
        logger.info("Test 4: Admin user cannot execute ADMIN DROP CLUSTER SNAPSHOT command (root mode)")
        connect(adminUser, adminPwd, feUrl) {
            try {
                sql "ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = 'dummy_id'"
                fail("Expected exception for admin user executing ADMIN DROP CLUSTER SNAPSHOT in root mode")
            } catch (Exception e) {
                logger.info("Expected error: " + e.getMessage())
                assertTrue(e.getMessage().contains("root privilege"))
            }
        }

        // Test 5: Admin user cannot query information_schema.cluster_snapshots
        logger.info("Test 5: Admin user cannot query information_schema.cluster_snapshots (root mode)")
        connect(adminUser, adminPwd, feUrl) {
            try {
                sql "SELECT * FROM information_schema.cluster_snapshots"
                fail("Expected exception for admin user querying cluster_snapshots in root mode")
            } catch (Exception e) {
                logger.info("Expected error: " + e.getMessage())
                assertTrue(e.getMessage().contains("root privilege"))
            }
        }

        // Test 6: Admin user cannot query information_schema.cluster_snapshot_properties
        logger.info("Test 6: Admin user cannot query information_schema.cluster_snapshot_properties (root mode)")
        connect(adminUser, adminPwd, feUrl) {
            try {
                sql "SELECT * FROM information_schema.cluster_snapshot_properties"
                fail("Expected exception for admin user querying cluster_snapshot_properties in root mode")
            } catch (Exception e) {
                logger.info("Expected error: " + e.getMessage())
                assertTrue(e.getMessage().contains("root privilege"))
            }
        }

        // Test 7: Normal user cannot execute cluster snapshot commands (root mode)
        logger.info("Test 7: Normal user cannot execute ADMIN SET CLUSTER SNAPSHOT FEATURE command (root mode)")
        connect(normalUser, normalPwd, feUrl) {
            try {
                sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
                fail("Expected exception for normal user executing ADMIN SET CLUSTER SNAPSHOT FEATURE in root mode")
            } catch (Exception e) {
                logger.info("Expected error: " + e.getMessage())
                assertTrue(e.getMessage().contains("root privilege"))
            }
        }

        // ========================
        // Part 2: Test admin mode
        // ========================
        logger.info("=== Part 2: Testing admin mode ===")

        // Set config to admin mode
        sql """ADMIN SET FRONTEND CONFIG ("cluster_snapshot_min_privilege" = "admin")"""

        // Test 8: Admin user CAN execute ADMIN SET CLUSTER SNAPSHOT FEATURE command (admin mode)
        logger.info("Test 8: Admin user CAN execute ADMIN SET CLUSTER SNAPSHOT FEATURE command (admin mode)")
        connect(adminUser, adminPwd, feUrl) {
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            logger.info("Admin user successfully executed ADMIN SET CLUSTER SNAPSHOT FEATURE ON")
        }

        // Test 9: Admin user CAN query information_schema.cluster_snapshot_properties (admin mode)
        logger.info("Test 9: Admin user CAN query information_schema.cluster_snapshot_properties (admin mode)")
        connect(adminUser, adminPwd, feUrl) {
            def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshot_properties"
            logger.info("Admin user query result: " + res.toString())
            assertEquals(res.size(), 1)
            assertEquals(res[0]['SNAPSHOT_ENABLED'], 'YES')
        }

        // Test 10: Admin user CAN query information_schema.cluster_snapshots (admin mode)
        logger.info("Test 10: Admin user CAN query information_schema.cluster_snapshots (admin mode)")
        connect(adminUser, adminPwd, feUrl) {
            def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            logger.info("Admin user query result: " + res.toString())
            // Just verify the query works
        }

        // Test 11: Normal user still CANNOT execute cluster snapshot commands (admin mode)
        logger.info("Test 11: Normal user cannot execute ADMIN SET CLUSTER SNAPSHOT FEATURE command (admin mode)")
        connect(normalUser, normalPwd, feUrl) {
            try {
                sql "ADMIN SET CLUSTER SNAPSHOT FEATURE OFF"
                fail("Expected exception for normal user executing ADMIN SET CLUSTER SNAPSHOT FEATURE in admin mode")
            } catch (Exception e) {
                logger.info("Expected error: " + e.getMessage())
                assertTrue(e.getMessage().toLowerCase().contains("admin"))
            }
        }

        // Test 12: Normal user cannot query information_schema.cluster_snapshots (admin mode)
        logger.info("Test 12: Normal user cannot query information_schema.cluster_snapshots (admin mode)")
        connect(normalUser, normalPwd, feUrl) {
            try {
                sql "SELECT * FROM information_schema.cluster_snapshots"
                fail("Expected exception for normal user querying cluster_snapshots in admin mode")
            } catch (Exception e) {
                logger.info("Expected error: " + e.getMessage())
                assertTrue(e.getMessage().toLowerCase().contains("admin"))
            }
        }

        // ========================
        // Part 3: Root user always works
        // ========================
        logger.info("=== Part 3: Root user always works ===")

        // Test 13: Root user CAN execute cluster snapshot commands (in any mode)
        logger.info("Test 13: Root user CAN execute cluster snapshot commands")
        // Disable snapshot feature first
        sql "ADMIN SET CLUSTER SNAPSHOT FEATURE OFF"

        // Reset to root mode
        sql """ADMIN SET FRONTEND CONFIG ("cluster_snapshot_min_privilege" = "root")"""

        // Enable snapshot feature
        sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"

        // Query cluster snapshot properties
        def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshot_properties"
        logger.info("Cluster snapshot properties: " + res.toString())
        assertEquals(res.size(), 1)
        assertEquals(res[0]['SNAPSHOT_ENABLED'], 'YES')

        // Query cluster snapshots (should be empty initially)
        res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
        logger.info("Cluster snapshots: " + res.toString())
        // Just verify the query works, no assertion on content

        // Disable snapshot feature
        sql "ADMIN SET CLUSTER SNAPSHOT FEATURE OFF"

        // Cleanup
        try_sql("DROP USER ${adminUser}")
        try_sql("DROP USER ${normalUser}")

        logger.info("All cluster snapshot privilege tests passed!")
    }
}
