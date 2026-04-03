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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for UserAuthentication privilege checks.
 */
public class UserAuthenticationTest {
    @Mocked
    private Env env;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private SessionVariable sessionVariable;
    @Mocked
    private TableIf table;
    @Mocked
    private DatabaseIf db;
    @Mocked
    private CatalogIf catalog;

    private String originalMinPrivilege;

    @BeforeEach
    public void setUp() {
        originalMinPrivilege = Config.cluster_snapshot_min_privilege;
    }

    @AfterEach
    public void tearDown() {
        Config.cluster_snapshot_min_privilege = originalMinPrivilege;
    }

    /**
     * Test that a table named "cluster_snapshots" in a normal database (not information_schema)
     * does NOT trigger the special privilege check for cluster snapshot tables.
     * This verifies that only information_schema.cluster_snapshots requires special privileges.
     */
    @Test
    public void testSameNameTableInNormalDbNotTriggerSpecialCheck() throws Exception {
        // Set to root mode - if special check is triggered, non-root user would be denied
        Config.cluster_snapshot_min_privilege = "root";

        UserIdentity normalUser = new UserIdentity("normal_user", "%");
        normalUser.setIsAnalyzed();

        new Expectations() {
            {
                // Table setup - same name as cluster snapshot table but in a normal database
                table.getName();
                minTimes = 0;
                result = "cluster_snapshots";  // Same name as the special table

                table.getDatabase();
                minTimes = 0;
                result = db;

                // Database is NOT information_schema, it's a normal user database
                db.getFullName();
                minTimes = 0;
                result = "mydb";  // Normal database, not information_schema

                db.getCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getName();
                minTimes = 0;
                result = "internal";

                // ConnectContext setup
                connectContext.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.isPlayNereidsDump();
                minTimes = 0;
                result = false;

                connectContext.getEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                // Normal SELECT privilege check should be called (not special root check)
                // and should return true to allow access
                accessControllerManager.checkTblPriv(connectContext, "internal", "mydb",
                        "cluster_snapshots", PrivPredicate.SELECT);
                minTimes = 1;  // This MUST be called, proving we're using normal privilege check
                result = true;

                connectContext.getCurrentUserIdentity();
                minTimes = 0;
                result = normalUser;
            }
        };

        // Should NOT throw exception - normal user can access mydb.cluster_snapshots
        // because it's not the special information_schema table
        Assertions.assertDoesNotThrow(() ->
                UserAuthentication.checkPermission(table, connectContext, null));
    }

    /**
     * Test that a table named "cluster_snapshot_properties" in a normal database
     * does NOT trigger the special privilege check.
     */
    @Test
    public void testSameNamePropertiesTableInNormalDbNotTriggerSpecialCheck() throws Exception {
        Config.cluster_snapshot_min_privilege = "root";

        UserIdentity normalUser = new UserIdentity("normal_user", "%");
        normalUser.setIsAnalyzed();

        new Expectations() {
            {
                table.getName();
                minTimes = 0;
                result = "cluster_snapshot_properties";

                table.getDatabase();
                minTimes = 0;
                result = db;

                db.getFullName();
                minTimes = 0;
                result = "user_database";

                db.getCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getName();
                minTimes = 0;
                result = "internal";

                connectContext.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.isPlayNereidsDump();
                minTimes = 0;
                result = false;

                connectContext.getEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkTblPriv(connectContext, "internal", "user_database",
                        "cluster_snapshot_properties", PrivPredicate.SELECT);
                minTimes = 1;
                result = true;

                connectContext.getCurrentUserIdentity();
                minTimes = 0;
                result = normalUser;
            }
        };

        Assertions.assertDoesNotThrow(() ->
                UserAuthentication.checkPermission(table, connectContext, null));
    }

    /**
     * Test that information_schema.cluster_snapshots DOES trigger special privilege check
     * and denies non-root user in root mode.
     */
    @Test
    public void testInfoSchemaClusterSnapshotsRequiresRootPrivilege() {
        Config.cluster_snapshot_min_privilege = "root";

        UserIdentity normalUser = new UserIdentity("normal_user", "%");
        normalUser.setIsAnalyzed();

        new Expectations() {
            {
                table.getName();
                minTimes = 0;
                result = "cluster_snapshots";

                table.getDatabase();
                minTimes = 0;
                result = db;

                // This IS information_schema
                db.getFullName();
                minTimes = 0;
                result = InfoSchemaDb.DATABASE_NAME;

                connectContext.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.isPlayNereidsDump();
                minTimes = 0;
                result = false;

                connectContext.getCurrentUserIdentity();
                minTimes = 0;
                result = normalUser;
            }
        };

        // Should throw AnalysisException because non-root user cannot access
        // information_schema.cluster_snapshots in root mode
        Assertions.assertThrows(AnalysisException.class, () ->
                UserAuthentication.checkPermission(table, connectContext, null));
    }

    /**
     * Test that information_schema.cluster_snapshots allows root user.
     */
    @Test
    public void testInfoSchemaClusterSnapshotsAllowsRootUser() {
        Config.cluster_snapshot_min_privilege = "root";

        new Expectations() {
            {
                table.getName();
                minTimes = 0;
                result = "cluster_snapshots";

                table.getDatabase();
                minTimes = 0;
                result = db;

                db.getFullName();
                minTimes = 0;
                result = InfoSchemaDb.DATABASE_NAME;

                connectContext.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.isPlayNereidsDump();
                minTimes = 0;
                result = false;

                connectContext.getCurrentUserIdentity();
                minTimes = 0;
                result = UserIdentity.ROOT;
            }
        };

        // Root user should be able to access
        Assertions.assertDoesNotThrow(() ->
                UserAuthentication.checkPermission(table, connectContext, null));
    }

    /**
     * Test that cluster:information_schema.cluster_snapshots (with cluster prefix)
     * DOES trigger special privilege check.
     * This verifies ClusterNamespace.getNameFromFullName correctly strips the cluster prefix.
     */
    @Test
    public void testInfoSchemaWithClusterPrefixTriggersSpecialCheck() {
        Config.cluster_snapshot_min_privilege = "root";

        UserIdentity normalUser = new UserIdentity("normal_user", "%");
        normalUser.setIsAnalyzed();

        new Expectations() {
            {
                table.getName();
                minTimes = 0;
                result = "cluster_snapshots";

                table.getDatabase();
                minTimes = 0;
                result = db;

                // Database name with cluster prefix - ClusterNamespace.getNameFromFullName
                // should strip "default_cluster:" and return "information_schema"
                db.getFullName();
                minTimes = 0;
                result = "default_cluster:information_schema";

                connectContext.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.isPlayNereidsDump();
                minTimes = 0;
                result = false;

                connectContext.getCurrentUserIdentity();
                minTimes = 0;
                result = normalUser;
            }
        };

        // Should throw AnalysisException because after stripping cluster prefix,
        // the db name is "information_schema", which triggers special privilege check
        Assertions.assertThrows(AnalysisException.class, () ->
                UserAuthentication.checkPermission(table, connectContext, null));
    }

    /**
     * Test that information_schema.cluster_snapshots allows admin user in admin mode.
     */
    @Test
    public void testInfoSchemaClusterSnapshotsAllowsAdminInAdminMode() {
        Config.cluster_snapshot_min_privilege = "admin";

        UserIdentity adminUser = new UserIdentity("admin", "%");
        adminUser.setIsAnalyzed();

        new Expectations() {
            {
                table.getName();
                minTimes = 0;
                result = "cluster_snapshots";

                table.getDatabase();
                minTimes = 0;
                result = db;

                db.getFullName();
                minTimes = 0;
                result = InfoSchemaDb.DATABASE_NAME;

                connectContext.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.isPlayNereidsDump();
                minTimes = 0;
                result = false;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;

                connectContext.getCurrentUserIdentity();
                minTimes = 0;
                result = adminUser;
            }
        };

        // Admin user should be able to access in admin mode
        Assertions.assertDoesNotThrow(() ->
                UserAuthentication.checkPermission(table, connectContext, null));
    }
}
