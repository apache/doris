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

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

public class ShowTableCommandTest extends TestWithFeService {
    private static final String CATALOG_NAME = "hive_catalog";
    private static final String DB_NAME = "hive_db";

    private ConnectContext ctx;

    private void runBefore() throws IOException {
        ctx = createDefaultCtx();
    }

    @Test
    public void testValidate() throws Exception {
        runBefore();
        ctx.setDatabase(CatalogMocker.TEST_DB_NAME);
        ctx.changeDefaultCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);

        ShowTableCommand command = new ShowTableCommand(CatalogMocker.TEST_DB_NAME,
                InternalCatalog.INTERNAL_CATALOG_NAME, false, PlanType.SHOW_TABLES);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));
    }

    @Test
    void testInvalidate() throws Exception {
        runBefore();
        ctx.setDatabase("");
        ctx.changeDefaultCatalog("");

        // db is empty
        ShowTableCommand command = new ShowTableCommand("",
                InternalCatalog.INTERNAL_CATALOG_NAME, false, PlanType.SHOW_TABLES);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx));

        // catalog is empty
        ShowTableCommand command2 = new ShowTableCommand(CatalogMocker.TEST_DB_NAME,
                "", false, PlanType.SHOW_TABLES);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(ctx));
    }

    /**
     * Bundle of mocks needed to drive {@link ShowTableCommand#doRun}: a mocked {@link ConnectContext}
     * whose {@code getEnv().getCatalogMgr().getCatalogOrAnalysisException(...).getDbOrAnalysisException(...)}
     * chain resolves to {@code dbIf}, wired to the given catalog (which decides whether
     * {@code dbIf.getCatalog() instanceof InternalCatalog} holds).
     */
    private static final class ShowTableCommandMocks {
        private final ConnectContext ctx = Mockito.mock(ConnectContext.class);
        private final StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        private final Env env = Mockito.mock(Env.class);
        private final AccessControllerManager accessControllerManager = Mockito.mock(AccessControllerManager.class);
        @SuppressWarnings("unchecked")
        private final DatabaseIf<TableIf> dbIf = Mockito.mock(DatabaseIf.class);

        @SuppressWarnings("unchecked")
        ShowTableCommandMocks(CatalogIf<?> catalogIf) throws AnalysisException {
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            Mockito.when(ctx.getEnv()).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
            Mockito.when(catalogMgr.getCatalogOrAnalysisException(Mockito.anyString())).thenReturn(catalogIf);
            Mockito.when(catalogIf.getDbOrAnalysisException(Mockito.anyString())).thenReturn(dbIf);
            Mockito.when(dbIf.getCatalog()).thenReturn(catalogIf);
            Mockito.when(dbIf.getFullName()).thenReturn(DB_NAME);
        }
    }

    private static ShowResultSet runDoRun(ShowTableCommandMocks mocks, ShowTableCommand command) throws Exception {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> mockedConnectContext = Mockito.mockStatic(ConnectContext.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(mocks.env);
            mockedConnectContext.when(ConnectContext::get).thenReturn(mocks.ctx);
            return command.doRun(mocks.ctx, mocks.executor);
        }
    }

    @Test
    public void testExternalCatalogNonVerboseShowTablesUsesFastPath() throws Exception {
        CatalogIf<?> catalogIf = Mockito.mock(CatalogIf.class);
        ShowTableCommandMocks mocks = new ShowTableCommandMocks(catalogIf);
        Mockito.when(mocks.dbIf.getTableNamesWithLock())
                .thenReturn(ImmutableSet.of("t2", "t1", "t_filtered_out"));
        // Every table is visible except "t_filtered_out", which SHOW privilege denies.
        Mockito.when(mocks.accessControllerManager.checkTblPriv(
                Mockito.eq(mocks.ctx), Mockito.eq(CATALOG_NAME), Mockito.eq(DB_NAME),
                Mockito.anyString(), Mockito.eq(PrivPredicate.SHOW)))
                .thenAnswer(invocation -> !"t_filtered_out".equals(invocation.getArgument(3)));

        ShowTableCommand command = new ShowTableCommand(DB_NAME, CATALOG_NAME, false, PlanType.SHOW_TABLES);
        ShowResultSet result = runDoRun(mocks, command);

        // Behavior: names come back sorted, and the privilege-denied table is excluded.
        List<List<String>> rows = result.getResultRows();
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(Lists.newArrayList("t1"), rows.get(0));
        Assertions.assertEquals(Lists.newArrayList("t2"), rows.get(1));

        // Call counts: the fast path must list names, and must never load every table.
        // It uses getTableNamesWithLock() (not the exception-swallowing ...OrEmpty... variant)
        // so a name-conflict exception raised while listing is still propagated.
        Mockito.verify(mocks.dbIf, Mockito.times(1)).getTableNamesWithLock();
        Mockito.verify(mocks.dbIf, Mockito.times(0)).getTables();
    }

    @Test
    public void testExternalCatalogVerboseShowTablesUsesSlowPath() throws Exception {
        CatalogIf<?> catalogIf = Mockito.mock(CatalogIf.class);
        ShowTableCommandMocks mocks = new ShowTableCommandMocks(catalogIf);
        Mockito.when(mocks.dbIf.getTables()).thenReturn(Lists.newArrayList());

        // Verbose SHOW TABLES needs per-table metadata (storage format, etc.), so even on an
        // external catalog it must fall back to the slow path that loads every table.
        ShowTableCommand command = new ShowTableCommand(DB_NAME, CATALOG_NAME, true, PlanType.SHOW_TABLES);
        ShowResultSet result = runDoRun(mocks, command);

        Assertions.assertTrue(result.getResultRows().isEmpty());
        Mockito.verify(mocks.dbIf, Mockito.times(1)).getTables();
        Mockito.verify(mocks.dbIf, Mockito.times(0)).getTableNamesWithLock();
    }

    @Test
    public void testExternalCatalogShowViewsUsesSlowPath() throws Exception {
        CatalogIf<?> catalogIf = Mockito.mock(CatalogIf.class);
        ShowTableCommandMocks mocks = new ShowTableCommandMocks(catalogIf);
        Mockito.when(mocks.dbIf.getTables()).thenReturn(Lists.newArrayList());

        // SHOW VIEWS needs the engine type of every table to filter views, so the name-only
        // fast path (guarded by PlanType.SHOW_TABLES) must not be taken here.
        ShowTableCommand command = new ShowTableCommand(DB_NAME, CATALOG_NAME, false, PlanType.SHOW_VIEWS);
        ShowResultSet result = runDoRun(mocks, command);

        Assertions.assertTrue(result.getResultRows().isEmpty());
        Mockito.verify(mocks.dbIf, Mockito.times(1)).getTables();
        Mockito.verify(mocks.dbIf, Mockito.times(0)).getTableNamesWithLock();
    }

    @Test
    public void testInternalCatalogNonVerboseShowTablesUsesSlowPath() throws Exception {
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        ShowTableCommandMocks mocks = new ShowTableCommandMocks(internalCatalog);
        Mockito.when(mocks.dbIf.getTables()).thenReturn(Lists.newArrayList());

        // The fast path is only for external catalogs; internal catalogs always take the
        // slow path regardless of verbosity.
        ShowTableCommand command = new ShowTableCommand(DB_NAME, InternalCatalog.INTERNAL_CATALOG_NAME, false,
                PlanType.SHOW_TABLES);
        ShowResultSet result = runDoRun(mocks, command);

        Assertions.assertTrue(result.getResultRows().isEmpty());
        Mockito.verify(mocks.dbIf, Mockito.times(1)).getTables();
        Mockito.verify(mocks.dbIf, Mockito.times(0)).getTableNamesWithLock();
    }
}
