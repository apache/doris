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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests for the table-name listing path of SHOW TABLES on external catalogs.
 * A plain SHOW TABLES must not initialize table objects (one remote metadata
 * load per table); verbose SHOW FULL TABLES, SHOW VIEWS and the internal
 * catalog keep iterating table objects. Name-listing failures (e.g. the
 * case-insensitive name-conflict error) must propagate instead of being
 * swallowed into an empty result.
 */
public class ShowTableCommandExternalTest {
    private static final String CTL = "test_ctl";
    private static final String DB = "test_db";

    private Env env;
    private AccessControllerManager accessManager;
    private ConnectContext ctx;
    private CatalogMgr catalogMgr;
    private CatalogIf catalog;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() throws Exception {
        env = Mockito.mock(Env.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        ctx = Mockito.mock(ConnectContext.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);
        catalog = Mockito.mock(CatalogIf.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(ctx.getEnv()).thenReturn(env);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalogOrAnalysisException(CTL)).thenReturn(catalog);
        // Allow SHOW privilege on every table by default; individual tests can override.
        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class),
                Mockito.eq(CTL), Mockito.eq(DB), Mockito.anyString(),
                Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
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

    @SuppressWarnings("unchecked")
    private ExternalDatabase<ExternalTable> mockExternalDb(Set<String> tableNames) throws Exception {
        ExternalDatabase<ExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn(DB);
        // any catalog that is not the internal one routes to the names-only path
        Mockito.when(db.getCatalog()).thenReturn(Mockito.mock(CatalogIf.class));
        Mockito.when(db.getTableNamesWithLock()).thenReturn(tableNames);
        Mockito.when(catalog.getDbOrAnalysisException(DB)).thenReturn(db);
        return db;
    }

    private ExternalTable mockExternalTable(String name, String engine) {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.isTemporary()).thenReturn(false);
        Mockito.when(table.getMysqlType()).thenReturn("BASE TABLE");
        Mockito.when(table.getEngine()).thenReturn(engine);
        return table;
    }

    private TableIf mockTable(String name, boolean temporary) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.isTemporary()).thenReturn(temporary);
        return table;
    }

    private List<String> firstColumn(ShowResultSet resultSet) {
        return resultSet.getResultRows().stream().map(row -> row.get(0)).collect(Collectors.toList());
    }

    @Test
    void testShowTablesOnExternalCatalogListsNamesOnly() throws Exception {
        Set<String> names = Sets.newLinkedHashSet(Lists.newArrayList("tbl3", "tbl1", "tbl2"));
        ExternalDatabase<ExternalTable> db = mockExternalDb(names);

        ShowTableCommand command = new ShowTableCommand(DB, CTL, false, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = command.doRun(ctx, null);

        // names are returned sorted, and no table object is ever requested
        Assertions.assertEquals(Lists.newArrayList("tbl1", "tbl2", "tbl3"), firstColumn(resultSet));
        Mockito.verify(db, Mockito.never()).getTables();
    }

    @Test
    void testShowTablesLikePatternOnExternalCatalog() throws Exception {
        envMockedStatic.when(() -> Env.getLowerCaseTableNames(CTL)).thenReturn(0);
        ExternalDatabase<ExternalTable> db =
                mockExternalDb(Sets.newLinkedHashSet(Lists.newArrayList("tbl1", "tbl2", "other_tbl")));

        ShowTableCommand command = new ShowTableCommand(DB, CTL, false, "tbl_", null, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = command.doRun(ctx, null);

        Assertions.assertEquals(Lists.newArrayList("tbl1", "tbl2"), firstColumn(resultSet));
        Mockito.verify(db, Mockito.never()).getTables();
    }

    @Test
    void testShowTablesPrivilegeFilterOnExternalCatalog() throws Exception {
        ExternalDatabase<ExternalTable> db =
                mockExternalDb(Sets.newLinkedHashSet(Lists.newArrayList("tbl1", "tbl_secret")));
        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class),
                Mockito.eq(CTL), Mockito.eq(DB), Mockito.eq("tbl_secret"),
                Mockito.eq(PrivPredicate.SHOW))).thenReturn(false);

        ShowTableCommand command = new ShowTableCommand(DB, CTL, false, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = command.doRun(ctx, null);

        Assertions.assertEquals(Lists.newArrayList("tbl1"), firstColumn(resultSet));
        Mockito.verify(db, Mockito.never()).getTables();
    }

    @Test
    void testShowTablesPropagatesNameListingFailure() throws Exception {
        // getTableNamesWithLock() must not be replaced by getTableNamesOrEmptyWithLock():
        // a case-insensitive name conflict has to surface as an error, not an empty result.
        ExternalDatabase<ExternalTable> db = mockExternalDb(Sets.newLinkedHashSet());
        Mockito.when(db.getTableNamesWithLock()).thenThrow(
                new RuntimeException("Found conflicting table names under case-insensitive conditions"));

        ShowTableCommand command = new ShowTableCommand(DB, CTL, false, PlanType.SHOW_TABLES);
        Assertions.assertThrows(RuntimeException.class, () -> command.doRun(ctx, null));
        Mockito.verify(db, Mockito.never()).getTables();
    }

    @Test
    void testShowFullTablesOnExternalCatalogStillLoadsTables() throws Exception {
        ExternalDatabase<ExternalTable> db = mockExternalDb(Sets.newLinkedHashSet());
        ExternalTable t1 = mockExternalTable("tbl1", "ICEBERG");
        ExternalTable t2 = mockExternalTable("tbl2", "ICEBERG");
        Mockito.when(db.getTables()).thenReturn(Lists.newArrayList(t2, t1));

        ShowTableCommand command = new ShowTableCommand(DB, CTL, true, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = command.doRun(ctx, null);

        Assertions.assertEquals(Lists.newArrayList("tbl1", "tbl2"), firstColumn(resultSet));
        // verbose output carries type and storage format columns
        Assertions.assertEquals(4, resultSet.getResultRows().get(0).size());
        Assertions.assertEquals("BASE TABLE", resultSet.getResultRows().get(0).get(1));
        Mockito.verify(db, Mockito.times(1)).getTables();
    }

    @Test
    void testShowViewsOnExternalCatalogStillLoadsTables() throws Exception {
        ExternalDatabase<ExternalTable> db = mockExternalDb(Sets.newLinkedHashSet());
        ExternalTable view = mockExternalTable("v1", TableIf.TableType.VIEW.toEngineName());
        ExternalTable table = mockExternalTable("tbl1", "ICEBERG");
        Mockito.when(db.getTables()).thenReturn(Lists.newArrayList(view, table));

        ShowTableCommand command = new ShowTableCommand(DB, CTL, false, PlanType.SHOW_VIEWS);
        ShowResultSet resultSet = command.doRun(ctx, null);

        Assertions.assertEquals(Lists.newArrayList("v1"), firstColumn(resultSet));
        Mockito.verify(db, Mockito.times(1)).getTables();
    }

    @Test
    void testShowTablesOnInternalCatalogStillLoadsTables() throws Exception {
        @SuppressWarnings("unchecked")
        DatabaseIf<TableIf> db = Mockito.mock(DatabaseIf.class);
        Mockito.when(db.getFullName()).thenReturn(DB);
        Mockito.when(db.getCatalog()).thenReturn(Mockito.mock(InternalCatalog.class));
        TableIf normal = mockTable("tbl1", false);
        TableIf temporary = mockTable("tmp_tbl", true);
        Mockito.when(db.getTables()).thenReturn(Lists.newArrayList(normal, temporary));
        Mockito.when(catalog.getDbOrAnalysisException(DB)).thenReturn(db);

        ShowTableCommand command = new ShowTableCommand(DB, CTL, false, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = command.doRun(ctx, null);

        // the internal path is unchanged: table objects are used and temporary tables are hidden
        Assertions.assertEquals(Lists.newArrayList("tbl1"), firstColumn(resultSet));
        Mockito.verify(db, Mockito.times(1)).getTables();
        Mockito.verify(db, Mockito.never()).getTableNamesWithLock();
    }
}
