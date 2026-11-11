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

import org.apache.doris.analysis.TablePattern;
import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.ColPrivilegeKey;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

public class ShowDatabasesCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    private Env env;
    private AccessControllerManager accessManager;
    private ConnectContext ctx;
    private InternalCatalog catalog;
    private CatalogMgr catalogMgr;
    private MockedStatic<Env> envMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        ctx = Mockito.mock(ConnectContext.class);
        catalog = Mockito.mock(InternalCatalog.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);

        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
    }

    @AfterEach
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    @Test
    void testValidate() {
        EqualTo equalTo = new EqualTo(new UnboundSlot("schema_name"),
                new StringLiteral(CatalogMocker.TEST_DB_NAME));

        // normal
        ShowDatabasesCommand command = new ShowDatabasesCommand(internalCtl, null, equalTo);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));

        // catalog is null
        ShowDatabasesCommand command2 = new ShowDatabasesCommand(null, null, equalTo);
        Assertions.assertDoesNotThrow(() -> command2.validate(ctx));
    }

    @Test
    void testDeniedExternalCatalogNeverLoadsMetadata() throws Exception {
        ExternalCatalog external = Mockito.mock(ExternalCatalog.class);
        Mockito.when(ctx.getCatalog("lake")).thenReturn(external);
        Mockito.when(ctx.getDefaultCatalog()).thenReturn("lake");
        Mockito.when(external.getDbNames()).thenThrow(new IllegalStateException("remote connection details"));
        EqualTo where = new EqualTo(new UnboundSlot("Database"), new StringLiteral("visible"));
        for (ShowDatabasesCommand command : Arrays.asList(
                new ShowDatabasesCommand("lake", null, null),
                new ShowDatabasesCommand(null, null, null),
                new ShowDatabasesCommand("lake", "vis%", null),
                new ShowDatabasesCommand("lake", null, where))) {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> command.doRun(ctx, null));
            Assertions.assertFalse(error.getMessage().contains("remote connection details"));
        }
        Mockito.verify(external, Mockito.never()).getDbNames();
        Mockito.verify(accessManager, Mockito.times(4)).checkCtlPriv(ctx, "lake", PrivPredicate.SHOW);
    }

    @Test
    void testVisibleExternalCatalogStillFiltersDatabases() throws Exception {
        ExternalCatalog external = Mockito.mock(ExternalCatalog.class);
        Mockito.when(ctx.getCatalog("lake")).thenReturn(external);
        Mockito.when(accessManager.checkCtlPriv(ctx, "lake", PrivPredicate.SHOW)).thenReturn(true);
        Mockito.when(external.getDbNames()).thenReturn(Arrays.asList("visible", "hidden", "other"));
        Mockito.when(accessManager.checkDbPriv(Mockito.nullable(ConnectContext.class),
                Mockito.eq("lake"), Mockito.eq("visible"), Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
        ShowResultSet result = new ShowDatabasesCommand("lake", null, null).doRun(ctx, null);
        Assertions.assertEquals(Collections.singletonList(Collections.singletonList("visible")),
                result.getResultRows());
        InOrder order = Mockito.inOrder(accessManager, external);
        order.verify(accessManager).checkCtlPriv(ctx, "lake", PrivPredicate.SHOW);
        order.verify(external).getDbNames();
        Assertions.assertEquals(result.getResultRows(),
                new ShowDatabasesCommand("lake", "vis%", null).doRun(ctx, null).getResultRows());
    }

    @Test
    void testInternalCatalogDoesNotRequireCatalogPrivilege() throws Exception {
        Mockito.when(catalog.isInternalCatalog()).thenReturn(true);
        Mockito.when(ctx.getCatalog(internalCtl)).thenReturn(catalog);
        Mockito.when(catalog.getDbNames()).thenReturn(Collections.emptyList());
        Assertions.assertTrue(new ShowDatabasesCommand(internalCtl, null, null)
                .doRun(ctx, null).getResultRows().isEmpty());
        Mockito.verify(accessManager, Mockito.never()).checkCtlPriv(Mockito.any(ConnectContext.class),
                Mockito.anyString(), Mockito.any(PrivPredicate.class));
        Mockito.verify(catalog).getDbNames();
    }

    @Test
    void testMissingCatalogKeepsExplicitError() {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowDatabasesCommand("missing", null, null).doRun(ctx, null));
        Assertions.assertTrue(error.getMessage().contains("No catalog found"));
    }

    @Test
    void testDatabaseTableAndColumnGrantsKeepCatalogVisible() throws Exception {
        ExternalCatalog external = Mockito.mock(ExternalCatalog.class);
        Mockito.when(ctx.getCatalog("lake")).thenReturn(external);
        Mockito.when(external.getDbNames()).thenReturn(Arrays.asList("visible", "hidden"));
        for (String level : Arrays.asList("database", "table", "column")) {
            TablePattern pattern = new TablePattern("lake", "visible", level.equals("database") ? "*" : "t");
            pattern.analyze();
            Role role = level.equals("column")
                    ? new Role("reader", pattern, PrivBitSet.of(), Collections.singletonMap(
                            new ColPrivilegeKey(Privilege.SELECT_PRIV, "lake", "visible", "t"),
                            Collections.singleton("c")))
                    : new Role("reader", pattern, PrivBitSet.of(Privilege.SELECT_PRIV));
            Mockito.when(accessManager.checkCtlPriv(ctx, "lake", PrivPredicate.SHOW))
                    .thenAnswer(invocation -> role.checkCtlPriv("lake", PrivPredicate.SHOW, PrivBitSet.of()));
            Mockito.when(accessManager.checkDbPriv(ctx, "lake", "visible", PrivPredicate.SHOW))
                    .thenAnswer(invocation -> role.checkDbPriv("lake", "visible", PrivPredicate.SHOW, PrivBitSet.of()));
            Assertions.assertEquals(Collections.singletonList(Collections.singletonList("visible")),
                    new ShowDatabasesCommand("lake", null, null).doRun(ctx, null).getResultRows(), level);
        }
    }
}

