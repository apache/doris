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
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergSysExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Test for UserAuthentication privilege checks.
 */
public class UserAuthenticationTest {
    private Env env = Mockito.mock(Env.class);
    private ConnectContext connectContext = Mockito.mock(ConnectContext.class);
    private AccessControllerManager accessControllerManager = Mockito.mock(AccessControllerManager.class);
    private SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
    private TableIf table = Mockito.mock(TableIf.class);
    private DatabaseIf db = Mockito.mock(DatabaseIf.class);
    private CatalogIf catalog = Mockito.mock(CatalogIf.class);
    private IcebergSysExternalTable icebergSysTable = Mockito.mock(IcebergSysExternalTable.class);
    private IcebergExternalTable icebergSourceTable = Mockito.mock(IcebergExternalTable.class);

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

        Mockito.when(table.getName()).thenReturn("cluster_snapshots");
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(db.getFullName()).thenReturn("mydb");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(sessionVariable.isPlayNereidsDump()).thenReturn(false);
        Mockito.when(connectContext.getEnv()).thenReturn(env);
        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(accessControllerManager.checkTblPriv(connectContext, "internal", "mydb",
                "cluster_snapshots", PrivPredicate.SELECT)).thenReturn(true);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(normalUser);

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

        Mockito.when(table.getName()).thenReturn("cluster_snapshot_properties");
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(db.getFullName()).thenReturn("user_database");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(sessionVariable.isPlayNereidsDump()).thenReturn(false);
        Mockito.when(connectContext.getEnv()).thenReturn(env);
        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(accessControllerManager.checkTblPriv(connectContext, "internal", "user_database",
                "cluster_snapshot_properties", PrivPredicate.SELECT)).thenReturn(true);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(normalUser);

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

        Mockito.when(table.getName()).thenReturn("cluster_snapshots");
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(db.getFullName()).thenReturn(InfoSchemaDb.DATABASE_NAME);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(sessionVariable.isPlayNereidsDump()).thenReturn(false);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(normalUser);

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

        Mockito.when(table.getName()).thenReturn("cluster_snapshots");
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(db.getFullName()).thenReturn(InfoSchemaDb.DATABASE_NAME);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(sessionVariable.isPlayNereidsDump()).thenReturn(false);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);

        // Root user should be able to access
        Assertions.assertDoesNotThrow(() ->
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

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Mockito.when(table.getName()).thenReturn("cluster_snapshots");
            Mockito.when(table.getDatabase()).thenReturn(db);
            Mockito.when(db.getFullName()).thenReturn(InfoSchemaDb.DATABASE_NAME);
            Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
            Mockito.when(sessionVariable.isPlayNereidsDump()).thenReturn(false);
            Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
            Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN)).thenReturn(true);
            Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(adminUser);

            // Admin user should be able to access in admin mode
            Assertions.assertDoesNotThrow(() ->
                    UserAuthentication.checkPermission(table, connectContext, null));
        }
    }

    @Test
    public void testIcebergSysTableUsesSourceTablePrivilege() throws Exception {
        Mockito.when(icebergSysTable.getSourceTable()).thenReturn(icebergSourceTable);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(sessionVariable.isPlayNereidsDump()).thenReturn(false);
        Mockito.when(icebergSourceTable.getName()).thenReturn("source_tbl");
        Mockito.when(icebergSourceTable.getDatabase()).thenReturn(db);
        Mockito.when(db.getFullName()).thenReturn("test_db");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("test_ctl");
        Mockito.when(connectContext.getEnv()).thenReturn(env);
        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(accessControllerManager.checkTblPriv(connectContext, "test_ctl", "test_db",
                "source_tbl", PrivPredicate.SELECT)).thenReturn(true);

        Assertions.assertDoesNotThrow(() ->
                UserAuthentication.checkPermission(icebergSysTable, connectContext, null));
    }
}
