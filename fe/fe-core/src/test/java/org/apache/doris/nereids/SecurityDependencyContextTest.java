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

package org.apache.doris.nereids;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.InternalAuthorizationPlugin;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.policy.PolicyMgr;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class SecurityDependencyContextTest {
    private static final UserIdentity USER = UserIdentity.createAnalyzedUserIdentWithIp("reader", "%");
    private static final UserIdentity OTHER_USER = UserIdentity.createAnalyzedUserIdentWithIp("other", "%");
    private static final String CATALOG = "internal";
    private static final String DATABASE = "db";
    private static final String TABLE = "tbl";
    private static final String COLUMN = "value";

    @Test
    public void testCurrentPrivilegeAndNoPolicyAllowReuse() throws Exception {
        BuiltInFixture fixture = new BuiltInFixture();
        SecurityDependencyContext snapshot = fixture.completeDependencies().snapshotForShortCircuit();
        Mockito.clearInvocations(fixture.accessManager, fixture.policyMgr);

        Assertions.assertTrue(snapshot.isValid(fixture.connectContext));
        Mockito.verify(fixture.policyMgr).hasRowPolicy(CATALOG, DATABASE, TABLE);
        Mockito.verify(fixture.accessManager).checkColumnsPriv(
                fixture.connectContext, CATALOG, DATABASE, TABLE,
                ImmutableSet.of(COLUMN), PrivPredicate.SELECT);
    }

    @Test
    public void testPolicyAddedAfterPlanningInvalidatesReuseBeforePrivilegeCheck() throws Exception {
        BuiltInFixture fixture = new BuiltInFixture();
        SecurityDependencyContext snapshot = fixture.completeDependencies().snapshotForShortCircuit();
        Mockito.when(fixture.policyMgr.hasRowPolicy(CATALOG, DATABASE, TABLE)).thenReturn(true);
        Mockito.clearInvocations(fixture.accessManager);

        Assertions.assertFalse(snapshot.isValid(fixture.connectContext));
        Mockito.verify(fixture.accessManager, Mockito.never()).checkColumnsPriv(
                Mockito.any(ConnectContext.class), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.anySet(), Mockito.any());
    }

    @Test
    public void testSelectRevocationInvalidatesReuse() throws Exception {
        BuiltInFixture fixture = new BuiltInFixture();
        SecurityDependencyContext snapshot = fixture.completeDependencies().snapshotForShortCircuit();
        Mockito.doThrow(new AnalysisException("denied")).when(fixture.accessManager).checkColumnsPriv(
                fixture.connectContext, CATALOG, DATABASE, TABLE,
                ImmutableSet.of(COLUMN), PrivPredicate.SELECT);

        Assertions.assertFalse(snapshot.isValid(fixture.connectContext));
    }

    @Test
    public void testDifferentPlanningUserInvalidatesReuse() {
        BuiltInFixture fixture = new BuiltInFixture();
        SecurityDependencyContext snapshot = fixture.completeDependencies().snapshotForShortCircuit();

        Mockito.when(fixture.connectContext.getCurrentUserIdentity()).thenReturn(OTHER_USER);
        Assertions.assertFalse(snapshot.isValid(fixture.connectContext));
    }

    @Test
    public void testDifferentAuthenticatedRolesInvalidateReuse() {
        BuiltInFixture fixture = new BuiltInFixture();
        SecurityDependencyContext snapshot = fixture.completeDependencies().snapshotForShortCircuit();

        Mockito.when(fixture.connectContext.getAuthenticatedRoles()).thenReturn(ImmutableSet.of("auditor"));
        Assertions.assertFalse(snapshot.isValid(fixture.connectContext));
    }

    @Test
    public void testEffectiveExternalRowPolicyFailsClosed() {
        BuiltInFixture fixture = new BuiltInFixture();
        SecurityDependencyContext dependencies = fixture.completeDependencies();
        dependencies.addRowPolicies(
                ImmutableList.of(RowFilterSpec.restrictive("row:1", "tenant_id = 1")));

        Assertions.assertTrue(dependencies.hasEffectiveRowPolicy());
        Assertions.assertFalse(dependencies.snapshotForShortCircuit().isValid(fixture.connectContext));
    }

    @Test
    public void testNamespaceChangeBeforeSnapshotFailsClosed() {
        BuiltInFixture fixture = new BuiltInFixture();
        SecurityDependencyContext dependencies = fixture.completeDependencies();
        Mockito.when(fixture.database.getFullName()).thenReturn("renamed_db");

        Assertions.assertFalse(dependencies.snapshotForShortCircuit().isValid(fixture.connectContext));
        Mockito.verify(fixture.policyMgr, Mockito.never()).hasRowPolicy(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testDataMaskDisablesDirectReuse() {
        BuiltInFixture fixture = new BuiltInFixture();
        SecurityDependencyContext dependencies = fixture.completeDependencies();
        dependencies.addDataMask(CATALOG, DATABASE, TABLE, COLUMN,
                Optional.of(new DataMaskSpec("mask:1", "null")));

        Assertions.assertTrue(dependencies.hasDataMask());
        Assertions.assertFalse(dependencies.snapshotForShortCircuit().isValid(fixture.connectContext));
    }

    @Test
    public void testMissingPrivilegeRecordingFailsClosed() {
        BuiltInFixture fixture = new BuiltInFixture();

        Assertions.assertFalse(new SecurityDependencyContext(fixture.connectContext)
                .snapshotForShortCircuit().isValid(fixture.connectContext));
    }

    private static class BuiltInFixture {
        private final CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        private final DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        private final OlapTable table = Mockito.mock(OlapTable.class);
        private final Auth auth = Mockito.mock(Auth.class);
        private final PolicyMgr policyMgr = Mockito.mock(PolicyMgr.class);
        private final AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        private final Env env = Mockito.mock(Env.class);
        private final ConnectContext connectContext = Mockito.mock(ConnectContext.class);

        @SuppressWarnings({"rawtypes", "unchecked"})
        private BuiltInFixture() {
            Mockito.when(catalog.getName()).thenReturn(CATALOG);
            Mockito.when(database.getCatalog()).thenReturn((CatalogIf) catalog);
            Mockito.when(database.getFullName()).thenReturn(DATABASE);
            Mockito.when(table.getDatabase()).thenReturn((DatabaseIf) database);
            Mockito.when(table.getName()).thenReturn(TABLE);
            Mockito.when(policyMgr.hasRowPolicy(CATALOG, DATABASE, TABLE)).thenReturn(false);
            Mockito.when(accessManager.getAccessControllerOrDefault(CATALOG))
                    .thenReturn(new InternalAuthorizationPlugin(auth));
            Mockito.when(env.getPolicyMgr()).thenReturn(policyMgr);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(USER);
            Mockito.when(connectContext.getAuthenticatedRoles()).thenReturn(ImmutableSet.of("reader_role"));
            Mockito.when(connectContext.getEnv()).thenReturn(env);
            Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        }

        private SecurityDependencyContext completeDependencies() {
            SecurityDependencyContext dependencies = new SecurityDependencyContext(connectContext);
            dependencies.addCheckedPrivilege(table, ImmutableSet.of(COLUMN));
            dependencies.addDataMask(CATALOG, DATABASE, TABLE, COLUMN, Optional.empty());
            return dependencies;
        }
    }
}
