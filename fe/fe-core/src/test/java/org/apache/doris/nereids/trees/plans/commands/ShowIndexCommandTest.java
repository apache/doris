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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalDatabase;
import org.apache.doris.datasource.lance.LanceLogicalIndex;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowIndexCommandTest extends TestWithFeService {
    private static final String INTERNAL_TABLE = "show_index_internal";
    private static final String EXTERNAL_CATALOG = "show_index_test_external";
    private static final String UNREACHABLE_LANCE_CATALOG = "show_index_unreachable_lance";
    private static final String REST_LANCE_CATALOG = "show_index_rest_lance";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("CREATE TABLE test." + INTERNAL_TABLE + " (\n"
                + "  k1 INT,\n"
                + "  value STRING,\n"
                + "  INDEX idx_value(value) USING INVERTED COMMENT 'internal index'\n"
                + ") DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createCatalog("CREATE CATALOG " + EXTERNAL_CATALOG + " PROPERTIES (\n"
                + "  'type' = 'test',\n"
                + "  'catalog_provider.class' = '"
                + ShowIndexCommandTest.class.getName() + "$ExternalCatalogProvider'\n"
                + ")");
        createCatalog("CREATE CATALOG " + UNREACHABLE_LANCE_CATALOG + " PROPERTIES (\n"
                + "  'type' = 'lance',\n"
                + "  'lance.catalog.type' = 'rest',\n"
                + "  'lance.rest.uri' = 'http://127.0.0.1:1',\n"
                + "  'test_connection' = 'false'\n"
                + ")");
        createCatalog("CREATE CATALOG " + REST_LANCE_CATALOG + " PROPERTIES (\n"
                + "  'type' = 'lance',\n"
                + "  'lance.catalog.type' = 'rest',\n"
                + "  'lance.rest.uri' = 'http://127.0.0.1:1',\n"
                + "  'test_connection' = 'false'\n"
                + ")");
    }

    @Test
    void testAnalyze() throws Exception {
        TableNameInfo tableName = new TableNameInfo("test", "abc");
        ShowIndexCommand si = new ShowIndexCommand(tableName);
        si.analyze(connectContext);
        tableName = new TableNameInfo("hive_catalog", "test", "abc");
        si = new ShowIndexCommand(tableName);
        si.analyze(connectContext);
        tableName = new TableNameInfo("internal", "test", "abc");
        si = new ShowIndexCommand(tableName);
        si.analyze(connectContext);

        tableName = new TableNameInfo("", "");
        si = new ShowIndexCommand(tableName);
        ShowIndexCommand finalSi1 = si;
        Assertions.assertThrows(AnalysisException.class, () -> finalSi1.analyze(connectContext));

        connectContext.setDatabase(null);
        try {
            tableName = new TableNameInfo("", "test");
            si = new ShowIndexCommand(tableName);
            ShowIndexCommand finalSi2 = si;
            Assertions.assertThrows(AnalysisException.class, () -> finalSi2.analyze(connectContext));
        } finally {
            connectContext.setDatabase("test");
        }
    }

    @Test
    void testInternalIndexRowsRemainUnchanged() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException(INTERNAL_TABLE);
        List<Index> indexes = table.getIndexes();
        Assertions.assertEquals(1, indexes.size());
        Index index = indexes.get(0);

        ShowIndexCommand command = new ShowIndexCommand(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "test", INTERNAL_TABLE));
        ShowResultSet result = command.doRun(connectContext, null);

        Assertions.assertEquals(Collections.singletonList(Lists.newArrayList(
                INTERNAL_TABLE, "", index.getIndexName(), "", String.join(",", index.getColumns()),
                "", "", "", "", "", index.getIndexType().name(), index.getComment(),
                index.getPropertiesString())), result.getResultRows());
    }

    @Test
    void testBuildLanceRowsMapsAllThirteenColumns() {
        LanceLogicalIndex index = new LanceLogicalIndex(
                "VectorIndex", Collections.singletonList("embedding"), "IVF_PQ",
                "{\"metric_type\":\"cosine\"}");

        List<List<String>> rows = ShowIndexCommand.buildLanceRows(
                "documents", Collections.singletonList(index));

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(13, rows.get(0).size());
        Assertions.assertEquals(Arrays.asList(
                "documents", "", "VectorIndex", "1", "embedding", "", "", "", "", "",
                "IVF_PQ", "", "{\"metric_type\":\"cosine\"}"), rows.get(0));
    }

    @Test
    void testBuildLanceRowsExpandsCompositeIndexWithoutReorderingInput() {
        LanceLogicalIndex first = new LanceLogicalIndex(
                "z_index", Arrays.asList("first", "second", "third"), "BTREE", "{}");
        LanceLogicalIndex second = new LanceLogicalIndex(
                "a_index", Collections.singletonList("fourth"), "BITMAP", "{}");
        List<LanceLogicalIndex> indexes = Lists.newArrayList(first, second);

        List<List<String>> rows = ShowIndexCommand.buildLanceRows("events", indexes);

        Assertions.assertSame(first, indexes.get(0));
        Assertions.assertSame(second, indexes.get(1));
        Assertions.assertEquals(4, rows.size());
        Assertions.assertEquals(Arrays.asList("z_index", "z_index", "z_index", "a_index"),
                Arrays.asList(rows.get(0).get(2), rows.get(1).get(2), rows.get(2).get(2), rows.get(3).get(2)));
        Assertions.assertEquals(Arrays.asList("1", "2", "3", "1"),
                Arrays.asList(rows.get(0).get(3), rows.get(1).get(3), rows.get(2).get(3), rows.get(3).get(3)));
        Assertions.assertEquals(Arrays.asList("first", "second", "third", "fourth"),
                Arrays.asList(rows.get(0).get(4), rows.get(1).get(4), rows.get(2).get(4), rows.get(3).get(4)));
    }

    @Test
    void testOtherExternalCatalogStillReturnsEmptyWithoutResolvingTable() throws Exception {
        ShowIndexCommand command = new ShowIndexCommand(
                new TableNameInfo(EXTERNAL_CATALOG, "external_db", "missing_table"));

        ShowResultSet result = command.doRun(connectContext, null);

        Assertions.assertTrue(result.getResultRows().isEmpty());
    }

    @Test
    void testDeniedUserDoesNotInitializeUnreachableLanceCatalog() throws Exception {
        LanceExternalCatalog catalog = (LanceExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog(UNREACHABLE_LANCE_CATALOG);
        Assertions.assertFalse(catalog.isInitialized());
        ConnectContext deniedContext = createCtx(
                UserIdentity.createAnalyzedUserIdentWithIp("show_index_denied_user", "%"), "127.0.0.1");
        try {
            ShowIndexCommand command = new ShowIndexCommand(
                    new TableNameInfo(UNREACHABLE_LANCE_CATALOG, "unreachable_db", "unreachable_table"));

            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class, () -> command.doRun(deniedContext, null));

            Assertions.assertTrue(exception.getMessage().contains("denied"));
            Assertions.assertFalse(catalog.isInitialized());
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    void testAuthorizedLanceRestRejectedBeforeCatalogInitialization() throws Exception {
        LanceExternalCatalog catalog = (LanceExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog(REST_LANCE_CATALOG);
        Assertions.assertFalse(catalog.isInitialized());
        ShowIndexCommand command = new ShowIndexCommand(
                new TableNameInfo(REST_LANCE_CATALOG, "unreachable_db", "unreachable_table"));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> command.doRun(connectContext, null));

        Assertions.assertEquals(
                "SHOW INDEX is not supported for Lance REST catalogs", exception.getDetailMessage());
        Assertions.assertFalse(catalog.isInitialized());
    }

    @Test
    void testNonLanceTableInLanceCatalogRejected() throws Exception {
        Env mockedEnvironment = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        LanceExternalDatabase database = Mockito.mock(LanceExternalDatabase.class);
        TableIf notLanceTable = Mockito.mock(TableIf.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(mockedEnvironment);
            Mockito.when(mockedEnvironment.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(
                    Mockito.any(ConnectContext.class), Mockito.eq("lance_fs"), Mockito.eq("db"),
                    Mockito.eq("table"),
                    Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
            Mockito.when(mockedEnvironment.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalogOrAnalysisException("lance_fs")).thenReturn(catalog);
            Mockito.when(catalog.isRestCatalogConfigured()).thenReturn(false);
            Mockito.doReturn(database).when(catalog).getDbOrAnalysisException("db");
            Mockito.doReturn(notLanceTable).when(database).getTableOrAnalysisException("table");
            ShowIndexCommand command = new ShowIndexCommand(
                    new TableNameInfo("lance_fs", "db", "table"));

            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class, () -> command.doRun(connectContext, null));

            Assertions.assertEquals("Table table is not a Lance table", exception.getDetailMessage());
        }
    }

    public static class ExternalCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            Map<String, Map<String, List<Column>>> metadata = new HashMap<>();
            metadata.put("external_db", Collections.emptyMap());
            return metadata;
        }
    }
}
