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

package org.apache.doris.qe;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalCatalogNamespaceFailureTest extends TestWithFeService {
    private static final String DATABASE = "db_old";
    private static final String INTERNAL_DB = "namespace_internal";
    private static final String FAILURE = "Remote catalog unavailable";
    private FailingCatalog previousCatalog;
    private FailingCatalog viewCatalog;
    private ExternalTable viewTable;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(INTERNAL_DB);
        connectContext.setDatabase(INTERNAL_DB);
        createTable("CREATE TABLE t (k INT NOT NULL, v INT NOT NULL) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        createMv("CREATE MATERIALIZED VIEW mv AS SELECT k AS mv_k, SUM(v) AS mv_v FROM t GROUP BY k");
    }

    @BeforeEach
    public void prepareCatalogs() {
        previousCatalog = new FailingCatalog("namespace_previous");
        viewCatalog = new FailingCatalog("namespace_view");
        for (FailingCatalog catalog : List.of(previousCatalog, viewCatalog)) {
            Deencapsulation.invoke(Env.getCurrentEnv().getCatalogMgr(), "addCatalog", catalog);
        }
        connectContext.changeDefaultCatalog(previousCatalog.getName());
        connectContext.setDatabase(DATABASE);
        connectContext.setThreadLocalInfo();
        viewTable = viewCatalog.getDbNullable(DATABASE).getTableNullable("t");
    }

    @AfterEach
    public void removeCatalogs() {
        connectContext.setThreadLocalInfo();
        connectContext.changeDefaultCatalog("internal");
        connectContext.setDatabase(INTERNAL_DB);
        for (FailingCatalog catalog : List.of(previousCatalog, viewCatalog)) {
            Deencapsulation.invoke(Env.getCurrentEnv().getCatalogMgr(), "removeCatalog", catalog.getId());
            catalog.onClose();
            Env.getCurrentEnv().getExtMetaCacheMgr().removeCatalog(catalog.getId());
        }
    }

    @Test
    public void testFailedUseKeepsDatabaseNameAndIdAfterConcurrentRefresh() {
        NameSpaceContext saved = connectContext.getNameSpaceContext();
        // Invalidate the cache exactly after Env.changeDb validates db_new, before its setter's lookup.
        previousCatalog.failAfterLookup = true;
        RuntimeException error = Assertions.assertThrows(RuntimeException.class,
                () -> Env.getCurrentEnv().changeDb(connectContext, "db_new"));
        Assertions.assertTrue(error.getMessage().contains(FAILURE), error.getMessage());
        assertNamespace(saved);
        previousCatalog.unavailable = false;
        Assertions.assertDoesNotThrow(() -> Env.getCurrentEnv().changeDb(connectContext, "db_new"));
        Assertions.assertEquals("db_new", connectContext.getDatabase());
        Assertions.assertEquals(previousCatalog.getDbNullable("db_new").getId(), connectContext.getCurrentDbId());
    }

    @Test
    public void testHealthyExternalViewRestoresUnavailablePreviousCatalog() {
        NameSpaceContext saved = connectContext.getNameSpaceContext();
        previousCatalog.makeUnavailable();
        Assertions.assertDoesNotThrow(() -> analyzeExternalView("SELECT id FROM t"));
        assertNamespace(saved);
    }

    @Test
    public void testExternalViewEntryFailureRestoresPreviousNamespace() {
        NameSpaceContext saved = connectContext.getNameSpaceContext();
        viewCatalog.makeUnavailable();
        RuntimeException error = Assertions.assertThrows(RuntimeException.class,
                () -> analyzeExternalView("SELECT id FROM t"));
        Assertions.assertTrue(error.getMessage().contains(FAILURE), error.getMessage());
        assertNamespace(saved);
    }

    @Test
    public void testExternalViewAnalysisFailureIsNotMaskedByRestoration() {
        NameSpaceContext saved = connectContext.getNameSpaceContext();
        previousCatalog.makeUnavailable();
        RuntimeException error = Assertions.assertThrows(RuntimeException.class,
                () -> analyzeExternalView("SELECT * FROM missing_view_table"));
        Assertions.assertTrue(error.getMessage().contains("missing_view_table"), error.getMessage());
        assertNamespace(saved);
    }

    @Test
    public void testExternalViewPreservesEmptyNamespace() {
        connectContext.changeDefaultCatalog("");
        NameSpaceContext saved = connectContext.getNameSpaceContext();
        Assertions.assertDoesNotThrow(() -> analyzeExternalView("SELECT id FROM t"));
        assertNamespace(saved);
    }

    @Test
    public void testHealthyMvContextCopiesNamespaceAndBecomesCurrent() {
        NameSpaceContext saved = connectContext.getNameSpaceContext();
        ConnectContext mv = MTMVPlanUtil.createBasicMvContext(connectContext, List.of(), Map.of());
        Assertions.assertNotSame(connectContext, mv);
        Assertions.assertSame(mv, ConnectContext.get());
        Assertions.assertEquals(saved.getDefaultCatalog(), mv.getDefaultCatalog());
        Assertions.assertEquals(saved.getCurrentDb(), mv.getDatabase());
        Assertions.assertEquals(saved.getCurrentDbId(), mv.getCurrentDbId());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFailedMvContextConstructionPreservesThreadLocal(boolean noPreviousContext) {
        if (noPreviousContext) {
            ConnectContext.remove();
        }
        previousCatalog.makeUnavailable();
        Assertions.assertThrows(RuntimeException.class,
                () -> MTMVPlanUtil.createBasicMvContext(connectContext, List.of(), Map.of()));
        Assertions.assertSame(noPreviousContext ? null : connectContext, ConnectContext.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSyncMvSetupFailureDoesNotBreakInternalQuery(boolean failAfterContextCreation) {
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze("SELECT k, SUM(v) FROM internal." + INTERNAL_DB + ".t GROUP BY k");
        CascadesContext cascadesContext = checker.getCascadesContext();
        StatementContext statement = cascadesContext.getStatementContext();
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbNullable(INTERNAL_DB).getTableNullable("t");
        Assertions.assertTrue(table.getVisibleIndexIdToMeta().size() > 1, "Must exercise a visible sync MV");
        NameSpaceContext saved = connectContext.getNameSpaceContext();
        if (failAfterContextCreation) {
            previousCatalog.failAfterLookup = true;
        } else {
            previousCatalog.makeUnavailable();
        }
        Assertions.assertDoesNotThrow(() -> InitMaterializationContextHook.INSTANCE
                .initMaterializationContext(cascadesContext));
        Assertions.assertTrue(previousCatalog.unavailable, "MV setup must attempt the default catalog lookup");
        Assertions.assertSame(connectContext, ConnectContext.get());
        Assertions.assertSame(statement, ConnectContext.get().getStatementContext());
        assertNamespace(saved);
        Assertions.assertDoesNotThrow(() -> checker.rewrite().optimize());
    }

    private Plan analyzeExternalView(String sql) {
        // The real binder parses the view under the second catalog and restores the first catalog afterwards.
        StatementContext statement = new StatementContext(connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statement);
        CascadesContext cascadesContext = CascadesContext.initContext(statement,
                new NereidsParser().parseSingle("SELECT 1"), PhysicalProperties.ANY);
        return Deencapsulation.invoke(new BindRelation(), "parseAndAnalyzeExternalView", viewTable,
                viewCatalog.getName(), DATABASE, sql, cascadesContext);
    }

    private void assertNamespace(NameSpaceContext saved) {
        Assertions.assertEquals(saved.getDefaultCatalog(), connectContext.getDefaultCatalog());
        Assertions.assertEquals(saved.getCurrentDb(), connectContext.getDatabase());
        Assertions.assertEquals(saved.getCurrentDbId(), connectContext.getCurrentDbId());
    }

    private static class FailingCatalog extends TestExternalCatalog {
        private boolean unavailable;
        private boolean failAfterLookup;

        FailingCatalog(String name) {
            super(Env.getCurrentEnv().getNextId(), name, "",
                    new HashMap<>(Map.of("catalog_provider.class", Provider.class.getName())), "");
        }

        @Override
        protected void initLocalObjectsImpl() {
            if (unavailable) {
                throw new IllegalStateException(FAILURE);
            }
        }

        @Override
        public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
            ExternalDatabase<? extends ExternalTable> db = super.getDbNullable(dbName);
            if (failAfterLookup) {
                failAfterLookup = false;
                makeUnavailable();
            }
            return db;
        }

        void makeUnavailable() {
            resetToUninitialized(false);
            unavailable = true;
        }
    }

    public static class Provider implements TestExternalCatalog.TestCatalogProvider {
        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return Map.of(DATABASE, Map.of("t", List.of(new Column("id", Type.INT))),
                    "db_new", Map.of("t", List.of(new Column("id", Type.INT))));
        }
    }
}
