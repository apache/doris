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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Optional;

public class SecurityDependencyContextTest {
    private static final UserIdentity USER = UserIdentity.createAnalyzedUserIdentWithIp("reader", "%");
    private static final String CATALOG = "internal";
    private static final String DATABASE = "db";
    private static final String TABLE = "tbl";
    private static final String COLUMN = "value";

    @Test
    public void testUnchangedPoliciesAreValid() {
        RowFilterSpec rowFilter = RowFilterSpec.restrictive("row:1", "tenant_id = 1");
        DataMaskSpec dataMask = new DataMaskSpec("mask:1", "null");
        SecurityDependencyContext dependencies = new SecurityDependencyContext(USER);
        dependencies.setRowPolicies(CATALOG, DATABASE, TABLE, ImmutableList.of(rowFilter));
        dependencies.addDataMask(CATALOG, DATABASE, TABLE, COLUMN, Optional.of(dataMask));

        ConnectContext connectContext = contextWithPolicies(
                ImmutableList.of(RowFilterSpec.restrictive("row:1", "tenant_id = 1")),
                ImmutableMap.of(COLUMN, new DataMaskSpec("mask:1", "null")));

        Assertions.assertTrue(dependencies.snapshot().isValid(connectContext));
    }

    @Test
    public void testAddedRowPolicyInvalidatesNegativeSnapshot() {
        SecurityDependencyContext dependencies = new SecurityDependencyContext(USER);
        dependencies.setRowPolicies(CATALOG, DATABASE, TABLE, ImmutableList.of());
        ConnectContext connectContext = contextWithPolicies(
                ImmutableList.of(RowFilterSpec.restrictive("row:1", "tenant_id = 1")), ImmutableMap.of());

        Assertions.assertFalse(dependencies.snapshot().isValid(connectContext));
    }

    @Test
    public void testChangedRowPolicyInvalidatesSnapshot() {
        SecurityDependencyContext dependencies = new SecurityDependencyContext(USER);
        dependencies.setRowPolicies(CATALOG, DATABASE, TABLE,
                ImmutableList.of(RowFilterSpec.restrictive("row:1", "tenant_id = 1")));
        ConnectContext connectContext = contextWithPolicies(
                ImmutableList.of(RowFilterSpec.restrictive("row:2", "tenant_id = 2")), ImmutableMap.of());

        Assertions.assertFalse(dependencies.snapshot().isValid(connectContext));
    }

    @Test
    public void testAddedDataMaskInvalidatesNegativeSnapshot() {
        SecurityDependencyContext dependencies = new SecurityDependencyContext(USER);
        dependencies.addDataMask(CATALOG, DATABASE, TABLE, COLUMN, Optional.empty());
        ConnectContext connectContext = contextWithPolicies(ImmutableList.of(),
                ImmutableMap.of(COLUMN, new DataMaskSpec("mask:1", "null")));

        Assertions.assertFalse(dependencies.snapshot().isValid(connectContext));
    }

    @Test
    public void testDifferentExecutingIdentityInvalidatesSnapshot() {
        SecurityDependencyContext dependencies = new SecurityDependencyContext(USER);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(
                UserIdentity.createAnalyzedUserIdentWithIp("other", "%"));

        Assertions.assertFalse(dependencies.snapshot().isValid(connectContext));
    }

    @Test
    public void testMissingPlanningIdentityFailsClosed() {
        SecurityDependencyContext dependencies = new SecurityDependencyContext(null);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);

        Assertions.assertFalse(dependencies.snapshot().isValid(connectContext));
    }

    @Test
    public void testMissingPrivilegeRecordingDisablesShortCircuitReuse() {
        SecurityDependencyContext dependencies = new SecurityDependencyContext(USER);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(USER);

        Assertions.assertFalse(dependencies.snapshotForShortCircuit().isValid(connectContext));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testPrivilegeRevocationOrAuthorizationFailureInvalidatesSnapshot() throws Exception {
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        TableIf table = Mockito.mock(TableIf.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Env env = Mockito.mock(Env.class);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);

        Mockito.when(catalog.getName()).thenReturn(CATALOG);
        Mockito.when(catalog.getDb(DATABASE)).thenReturn(Optional.of(database));
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn(DATABASE);
        Mockito.when(database.getTable(TABLE)).thenReturn(Optional.of(table));
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getName()).thenReturn(TABLE);
        Mockito.when(catalogMgr.getCatalog(CATALOG)).thenReturn(catalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(USER);
        Mockito.when(connectContext.getEnv()).thenReturn(env);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());

        SecurityDependencyContext dependencies = new SecurityDependencyContext(USER);
        dependencies.addCheckedPrivilege(table, ImmutableSet.of(COLUMN));
        SecurityDependencyContext snapshot = dependencies.snapshot();
        Assertions.assertTrue(snapshot.isValid(connectContext));

        Mockito.doThrow(new UserException("SELECT was revoked or authorization is unavailable"))
                .when(accessManager).checkColumnsPriv(
                        connectContext, CATALOG, DATABASE, TABLE, ImmutableSet.of(COLUMN), PrivPredicate.SELECT);
        Assertions.assertFalse(snapshot.isValid(connectContext));
    }

    private ConnectContext contextWithPolicies(
            ImmutableList<RowFilterSpec> rowFilters, ImmutableMap<String, DataMaskSpec> dataMasks) {
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(accessManager.evalRowFilterPolicies(USER, CATALOG, DATABASE, TABLE)).thenReturn(rowFilters);
        Mockito.when(accessManager.evalDataMaskPolicies(
                ArgumentMatchers.eq(USER), ArgumentMatchers.eq(CATALOG), ArgumentMatchers.eq(DATABASE),
                ArgumentMatchers.eq(TABLE), ArgumentMatchers.anySet())).thenReturn(dataMasks);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(USER);
        Mockito.when(connectContext.getEnv()).thenReturn(env);
        return connectContext;
    }
}
