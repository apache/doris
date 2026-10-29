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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authentication.Principal;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ConnectorCopyOnWriteUtilsTest {
    @Test
    void checksMasksForAllTargetColumnsInOneCall() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        UserIdentity user = Mockito.mock(UserIdentity.class);
        Env env = Mockito.mock(Env.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(user);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        Mockito.when(database.getFullName()).thenReturn("database");
        Mockito.when(table.getName()).thenReturn("target");
        Mockito.when(table.getBaseSchema(true)).thenReturn(List.of(
                new Column("ID", ScalarType.INT), new Column("value", ScalarType.INT)));
        Set<String> columns = Set.of("ID", "value");
        Mockito.when(accessManager.evalRowFilterPolicies(user, "catalog", "database", "target"))
                .thenReturn(List.of());
        Mockito.when(accessManager.evalDataMaskPolicies(user, "catalog", "database", "target", columns))
                .thenReturn(Map.of());
        Assertions.assertDoesNotThrow(() -> ConnectorCopyOnWriteUtils.requireUnrestrictedSource(context, table));

        Mockito.when(accessManager.evalDataMaskPolicies(user, "catalog", "database", "target", columns))
                .thenReturn(Map.of("value", new DataMaskSpec("masked", "mask(value)")));
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorCopyOnWriteUtils.requireUnrestrictedSource(context, table));
        Mockito.verify(accessManager, Mockito.times(2))
                .evalDataMaskPolicies(user, "catalog", "database", "target", columns);
    }

    @Test
    void countContextPreservesCallerSessionAndRestoresThreadLocal() throws Exception {
        boolean previousUnitTest = FeConstants.runningUnitTest;
        ConnectContext previous = ConnectContext.get();
        FeConstants.runningUnitTest = true;
        try {
            ConnectContext source = Mockito.mock(ConnectContext.class);
            Env env = Mockito.mock(Env.class);
            InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
            Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
            Mockito.when(internalCatalog.getName()).thenReturn("internal");
            UserIdentity user = Mockito.mock(UserIdentity.class);
            Principal principal = Mockito.mock(Principal.class);
            SessionVariable variables = new SessionVariable();
            variables.setTimeZone("Asia/Shanghai");
            Mockito.when(source.getEnv()).thenReturn(env);
            Mockito.when(source.getNameSpaceContext())
                    .thenReturn(new NameSpaceContext("catalog", "database", 42L));
            Mockito.when(source.getSessionId()).thenReturn("original-session");
            Mockito.when(source.getCurrentUserIdentity()).thenReturn(user);
            Mockito.when(source.getAuthenticatedPrincipal()).thenReturn(principal);
            Mockito.when(source.getAuthenticatedRoles()).thenReturn(Set.of("reader"));
            Mockito.when(source.getRemoteIP()).thenReturn("192.0.2.1");
            Mockito.when(source.getConnectAttributes()).thenReturn(Map.of("client", "test"));
            Mockito.when(source.getSessionVariable()).thenReturn(variables);
            Mockito.when(source.getUserVars()).thenReturn(new HashMap<>(Map.of("x", new IntLiteral(7))));
            Mockito.when(source.getEffectiveCloudCluster()).thenReturn("compute-group");
            Mockito.doCallRealMethod().when(source).setThreadLocalInfo();
            source.setThreadLocalInfo();

            ConnectContext count = ConnectorCopyOnWriteUtils.createCountContext(source);
            Assertions.assertEquals("original-session", count.getSessionId());
            Assertions.assertSame(user, count.getCurrentUserIdentity());
            Assertions.assertSame(principal, count.getAuthenticatedPrincipal());
            Assertions.assertEquals(Set.of("reader"), count.getAuthenticatedRoles());
            Assertions.assertEquals("catalog", count.getDefaultCatalog());
            Assertions.assertEquals("database", count.getDatabase());
            Assertions.assertEquals(42L, count.getNameSpaceContext().getCurrentDbId());
            Mockito.verify(env).getInternalCatalog();
            Mockito.verifyNoMoreInteractions(env);
            Assertions.assertEquals("192.0.2.1", count.getRemoteIP());
            Assertions.assertEquals("compute-group", count.getEffectiveCloudCluster());
            Assertions.assertEquals(Map.of("client", "test"), count.getConnectAttributes());
            Assertions.assertNotSame(variables, count.getSessionVariable());
            Assertions.assertEquals("Asia/Shanghai", count.getSessionVariable().getTimeZone());
            count.getSessionVariable().setTimeZone("UTC");
            count.getUserVars().clear();
            Assertions.assertEquals("Asia/Shanghai", variables.getTimeZone());
            Assertions.assertEquals(1, source.getUserVars().size());
            try (AutoCloseConnectContext scope = new AutoCloseConnectContext(count)) {
                Assertions.assertSame(scope.connectContext, ConnectContext.get());
            }
            Assertions.assertSame(source, ConnectContext.get());
        } finally {
            FeConstants.runningUnitTest = previousUnitTest;
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }
}
