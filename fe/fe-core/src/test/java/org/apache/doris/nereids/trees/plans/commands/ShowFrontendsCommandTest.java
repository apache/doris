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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.HAProtocol;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.tablefunction.FrontendsDisksTableValuedFunction;
import org.apache.doris.tablefunction.FrontendsTableValuedFunction;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class ShowFrontendsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String infoDB = InfoSchemaDb.DATABASE_NAME;

    private Env env;
    private ConnectContext ctx;
    private AccessControllerManager accessControllerManager;
    private InternalCatalog catalog;
    private CatalogMgr catalogMgr;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        ctx = Mockito.mock(ConnectContext.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);
        catalog = Mockito.mock(InternalCatalog.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);

        Mockito.when(ctx.getState()).thenReturn(new QueryState());

        HAProtocol haProtocol = Mockito.mock(HAProtocol.class);
        Mockito.when(env.getHaProtocol()).thenReturn(haProtocol);
        Mockito.when(env.getSelfNode())
                .thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9030));
    }

    @AfterEach
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    @Test
    public void testNormal() throws Exception {
        Mockito.when(accessControllerManager.checkDbPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(infoDB), Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);

        ShowFrontendsCommand command = new ShowFrontendsCommand(null);
        ShowResultSet showResultSet = command.doRun(ctx, null);
        List<Column> columnList = showResultSet.getMetaData().getColumns();
        ImmutableList<String> frontendsTitleNames = FrontendsTableValuedFunction.getFrontendsTitleNames();
        Assertions.assertTrue(!columnList.isEmpty() && columnList.size() == frontendsTitleNames.size());
        for (int i = 0; i < frontendsTitleNames.size(); i++) {
            Assertions.assertTrue(columnList.get(i).getName().equalsIgnoreCase(frontendsTitleNames.get(i)));
        }
    }

    @Test
    public void testNoPrivilege() throws Exception {
        Mockito.when(accessControllerManager.checkDbPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(infoDB), Mockito.eq(PrivPredicate.SELECT))).thenReturn(false);

        ShowFrontendsCommand command = new ShowFrontendsCommand(null);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }

    @Test
    public void testNormalShowFrontendsDisks() throws Exception {
        Mockito.when(accessControllerManager.checkDbPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(infoDB), Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);

        ShowFrontendsCommand command = new ShowFrontendsCommand("disks");
        ShowResultSet showResultSet = command.doRun(ctx, null);
        List<Column> columnList = showResultSet.getMetaData().getColumns();
        ImmutableList<String> frontendsDisksTitleNames = FrontendsDisksTableValuedFunction
                .getFrontendsDisksTitleNames();
        Assertions.assertTrue(!columnList.isEmpty() && columnList.size() == frontendsDisksTitleNames.size());
        for (int i = 0; i < frontendsDisksTitleNames.size(); i++) {
            Assertions.assertTrue(columnList.get(i).getName().equalsIgnoreCase(frontendsDisksTitleNames.get(i)));
        }
    }

    @Test
    public void testNoPrivilegeShowFrontendsDisks() throws Exception {
        Mockito.when(accessControllerManager.checkDbPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(infoDB), Mockito.eq(PrivPredicate.SELECT))).thenReturn(false);

        ShowFrontendsCommand command = new ShowFrontendsCommand("disks");
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }
}
