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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalDatabase;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Routes-and-rejection coverage for Lance index DDL: top-level CREATE [OR REPLACE] INDEX and
 * DROP INDEX against Lance catalog tables run the static validation matrix and are then
 * rejected with typed messages (reject-all mode), ALTER TABLE ADD/DROP INDEX keeps the generic
 * external-table rejection, the ALTER privilege check precedes the typed rejection, and no
 * Env.getNextId() allocation happens on any rejected path.
 */
public class AlterTableCommandLanceIndexTest {
    private static final String CTL = "lance_ctl";
    private static final String DB = "db";
    private static final String TBL = "tbl";
    private static final String VALID_ANN_PROPERTIES =
            "PROPERTIES(\"index_type\"=\"IVF_PQ\", \"metric\"=\"l2\", "
                    + "\"num_partitions\"=\"256\", \"num_sub_vectors\"=\"16\")";

    private final NereidsParser parser = new NereidsParser();
    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    /**
     * Fully mocked catalog resolution chain ending at a LanceExternalTable with three NOT NULL
     * columns: v ARRAY&#60;FLOAT&#62;, c INT, s STRING.
     */
    private static class LanceFixture implements AutoCloseable {
        private final MockedStatic<Env> mockedEnv;
        private final Env env;
        private final CatalogMgr catalogMgr;
        private final LanceExternalCatalog catalog;
        private final LanceExternalDatabase database;
        private final LanceExternalTable table;

        LanceFixture(boolean restCatalog, boolean alterGranted) throws DdlException {
            mockedEnv = Mockito.mockStatic(Env.class);
            env = Mockito.mock(Env.class);
            catalogMgr = Mockito.mock(CatalogMgr.class);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            catalog = Mockito.mock(LanceExternalCatalog.class);
            database = Mockito.mock(LanceExternalDatabase.class);
            table = Mockito.mock(LanceExternalTable.class);

            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class),
                    Mockito.eq(CTL), Mockito.eq(DB), Mockito.eq(TBL),
                    Mockito.eq(PrivPredicate.ALTER))).thenReturn(alterGranted);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalogOrException(Mockito.eq(CTL), Mockito.any()))
                    .thenReturn(catalog);
            Mockito.doReturn(database).when(catalog).getDbOrDdlException(DB);
            Mockito.doReturn(table).when(database).getTableOrDdlException(TBL);
            Mockito.when(catalog.isRestCatalogConfigured()).thenReturn(restCatalog);
            Mockito.when(table.getColumn(Mockito.anyString())).thenAnswer(invocation -> {
                String name = invocation.getArgument(0);
                if ("v".equalsIgnoreCase(name)) {
                    return new Column("v", new ArrayType(Type.FLOAT), false, null, false, null, "");
                }
                if ("c".equalsIgnoreCase(name)) {
                    return new Column("c", Type.INT, false, null, false, null, "");
                }
                if ("s".equalsIgnoreCase(name)) {
                    return new Column("s", Type.STRING, false, null, false, null, "");
                }
                return null;
            });
            Mockito.when(table.getType()).thenReturn(TableIf.TableType.LANCE_EXTERNAL_TABLE);
            Mockito.when(table.getName()).thenReturn(TBL);
        }

        @Override
        public void close() {
            mockedEnv.close();
        }
    }

    /**
     * Mocked catalog resolution chain ending at an internal OlapTable.
     */
    private static class InternalFixture implements AutoCloseable {
        private final MockedStatic<Env> mockedEnv;
        private final Env env;

        InternalFixture() throws DdlException {
            mockedEnv = Mockito.mockStatic(Env.class);
            env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            CatalogIf internalCatalog = Mockito.mock(InternalCatalog.class);
            DatabaseIf database = Mockito.mock(DatabaseIf.class);
            OlapTable table = Mockito.mock(OlapTable.class);

            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class),
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                    Mockito.eq(PrivPredicate.ALTER))).thenReturn(true);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalogOrException(
                    Mockito.eq(InternalCatalog.INTERNAL_CATALOG_NAME), Mockito.any()))
                    .thenReturn(internalCatalog);
            Mockito.doReturn(database).when(internalCatalog).getDbOrDdlException(DB);
            Mockito.doReturn(table).when(database).getTableOrDdlException(TBL);
        }

        @Override
        public void close() {
            mockedEnv.close();
        }
    }

    private String runAndGetMessage(String sql) {
        AlterTableCommand command = (AlterTableCommand) parser.parseSingle(sql);
        try {
            command.run(connectContext, null);
            throw new AssertionError("expected an AnalysisException but the statement succeeded: " + sql);
        } catch (AnalysisException e) {
            // Catalog-resolution paths (privilege check, Lance validator, typed rejections) throw
            // the legacy AnalysisException; getDetailMessage() strips the "errCode = 2" prefix.
            return e.getDetailMessage();
        } catch (org.apache.doris.nereids.exceptions.AnalysisException e) {
            // IndexDefinition.validate() guards throw the nereids AnalysisException instead.
            return e.getMessage();
        } catch (Exception e) {
            throw new AssertionError("unexpected exception " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    private AnalysisException runAndGetCommonAnalysisException(String sql) {
        AlterTableCommand command = (AlterTableCommand) parser.parseSingle(sql);
        try {
            command.run(connectContext, null);
            throw new AssertionError("expected an AnalysisException but the statement succeeded: " + sql);
        } catch (AnalysisException e) {
            return e;
        } catch (Exception e) {
            throw new AssertionError("unexpected exception " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    @Test
    public void testCreateIndexOnLanceTableIsTypedRejected() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false, true)) {
            String message = runAndGetMessage(
                    "CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (v) USING ANN "
                            + VALID_ANN_PROPERTIES);
            Assertions.assertEquals("CREATE INDEX is not supported for Lance catalog tables", message);
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void testCreateOrReplaceIndexOnLanceTableIsTypedRejected() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false, true)) {
            String message = runAndGetMessage(
                    "CREATE OR REPLACE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (c) USING BTREE");
            Assertions.assertEquals("CREATE OR REPLACE INDEX is not supported for Lance catalog tables",
                    message);
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void testDropIndexOnLanceTableIsTypedRejected() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false, true)) {
            Assertions.assertEquals("DROP INDEX is not supported for Lance catalog tables",
                    runAndGetMessage("DROP INDEX idx ON " + CTL + "." + DB + "." + TBL));
            // Reject-all mode is uniform: IF EXISTS does not change the outcome.
            Assertions.assertEquals("DROP INDEX is not supported for Lance catalog tables",
                    runAndGetMessage("DROP INDEX IF EXISTS idx ON " + CTL + "." + DB + "." + TBL));
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void testFilesystemCatalogRejectionsExposeNotSupportedErrorCode() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false, true)) {
            for (String sql : new String[] {
                    "CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (c) USING BTREE",
                    "CREATE OR REPLACE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (c) USING BTREE",
                    "DROP INDEX idx ON " + CTL + "." + DB + "." + TBL}) {
                AnalysisException exception = runAndGetCommonAnalysisException(sql);
                Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_OPERATION_NOT_SUPPORTED,
                        exception.getMysqlErrorCode());
            }
        }
    }

    @Test
    public void testRestCatalogMessagesFireFirst() throws Exception {
        try (LanceFixture fixture = new LanceFixture(true, true)) {
            Assertions.assertEquals("CREATE INDEX is not supported for Lance REST catalogs",
                    runAndGetMessage("CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL
                            + " (v) USING ANN " + VALID_ANN_PROPERTIES));
            Assertions.assertEquals("CREATE OR REPLACE INDEX is not supported for Lance REST catalogs",
                    runAndGetMessage("CREATE OR REPLACE INDEX idx ON " + CTL + "." + DB + "." + TBL
                            + " (c) USING BTREE"));
            Assertions.assertEquals("DROP INDEX is not supported for Lance REST catalogs",
                    runAndGetMessage("DROP INDEX idx ON " + CTL + "." + DB + "." + TBL));
            // The REST fail-fast precedes even the index-name bounds.
            Assertions.assertEquals("CREATE INDEX is not supported for Lance REST catalogs",
                    runAndGetMessage("CREATE INDEX `` ON " + CTL + "." + DB + "." + TBL
                            + " (v) USING ANN " + VALID_ANN_PROPERTIES));
            Assertions.assertEquals("DROP INDEX is not supported for Lance REST catalogs",
                    runAndGetMessage("DROP INDEX `` ON " + CTL + "." + DB + "." + TBL));
            Mockito.verify(fixture.catalog, Mockito.never()).getDbOrDdlException(Mockito.anyString());
            Mockito.verify(fixture.database, Mockito.never()).getTableOrDdlException(Mockito.anyString());
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void testStaticValidationErrorsPrecedeTheTypedRejection() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false, true)) {
            Assertions.assertEquals("metric must be one of l2, cosine, dot",
                    runAndGetMessage("CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL
                            + " (v) USING ANN PROPERTIES(\"index_type\"=\"IVF_PQ\", \"metric\"=\"l1\", "
                            + "\"num_partitions\"=\"256\", \"num_sub_vectors\"=\"16\")"));
            Assertions.assertEquals("num_partitions must be a positive integer",
                    runAndGetMessage("CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL
                            + " (v) USING ANN PROPERTIES(\"index_type\"=\"IVF_PQ\", "
                            + "\"num_sub_vectors\"=\"16\")"));
            Assertions.assertEquals("BTREE indexes do not support properties",
                    runAndGetMessage("CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL
                            + " (c) USING BTREE PROPERTIES(\"k\"=\"v\")"));
            Assertions.assertEquals("Index column 'nope' does not exist",
                    runAndGetMessage("CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL
                            + " (nope) USING BTREE"));
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void testBlankQuotedIndexNameRejectedOnBothPaths() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false, true)) {
            // Nereids accepts an empty backquoted identifier, so `CREATE INDEX `` ` and
            // `DROP INDEX `` ` reach the command with an empty index name. Both paths must
            // surface the name error, not the typed unsupported-operation rejection.
            Assertions.assertEquals("index name cannot be empty",
                    runAndGetMessage("CREATE INDEX `` ON " + CTL + "." + DB + "." + TBL
                            + " (v) USING ANN " + VALID_ANN_PROPERTIES));
            Assertions.assertEquals("index name cannot be empty",
                    runAndGetMessage("CREATE OR REPLACE INDEX `` ON " + CTL + "." + DB + "." + TBL
                            + " (c) USING BTREE"));
            Assertions.assertEquals("index name cannot be empty",
                    runAndGetMessage("DROP INDEX `` ON " + CTL + "." + DB + "." + TBL));
            Assertions.assertEquals("index name cannot be empty",
                    runAndGetMessage("DROP INDEX IF EXISTS `` ON " + CTL + "." + DB + "." + TBL));
            AnalysisException exception = runAndGetCommonAnalysisException(
                    "DROP INDEX `` ON " + CTL + "." + DB + "." + TBL);
            Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_INVALID, exception.getMysqlErrorCode());
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void testNonLanceIndexTypesRejectedEndToEnd() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false, true)) {
            // Index types outside the Lance matrix surface the vocabulary error through the
            // command path, before the typed rejection and without any id allocation.
            Assertions.assertEquals("Lance catalog tables only support USING ANN, BTREE, or BITMAP",
                    runAndGetMessage("CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL
                            + " (c) USING NGRAM_BF"));
            Assertions.assertEquals("Lance catalog tables only support USING ANN, BTREE, or BITMAP",
                    runAndGetMessage("CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL
                            + " (c) USING INVERTED"));
            Assertions.assertEquals("Lance catalog tables only support USING ANN, BTREE, or BITMAP",
                    runAndGetMessage("CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (c)"));
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void testAlterTableAddAndDropIndexKeepTheGenericRejection() throws Exception {
        try (LanceFixture fixture = new LanceFixture(true, true)) {
            // alter = true ops skip both the REST fail-fast branch and the Lance branch, then
            // fall through to the existing generic external-table rejection.
            String addMessage = runAndGetMessage(
                    "ALTER TABLE " + CTL + "." + DB + "." + TBL + " ADD INDEX idx (c) USING INVERTED");
            Assertions.assertTrue(addMessage.contains("do not support SCHEMA_CHANGE clause now"),
                    addMessage);

            String dropMessage = runAndGetMessage(
                    "ALTER TABLE " + CTL + "." + DB + "." + TBL + " DROP INDEX idx");
            Assertions.assertTrue(dropMessage.contains("do not support SCHEMA_CHANGE clause now"),
                    dropMessage);
            Mockito.verify(fixture.catalog, Mockito.times(2)).getDbOrDdlException(DB);
            Mockito.verify(fixture.database, Mockito.times(2)).getTableOrDdlException(TBL);
        }
    }

    @Test
    public void testPrivilegeDeniedPrecedesTheTypedRejection() throws Exception {
        try (LanceFixture fixture = new LanceFixture(true, false)) {
            String message = runAndGetMessage(
                    "CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (v) USING ANN "
                            + VALID_ANN_PROPERTIES);
            Assertions.assertTrue(message.contains("command denied to user"), message);
            // The privilege check fires before any catalog resolution.
            Mockito.verify(fixture.catalogMgr, Mockito.never())
                    .getCatalogOrException(Mockito.anyString(), Mockito.any());
            Mockito.verify(fixture.catalog, Mockito.never()).isRestCatalogConfigured();
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void testInternalTableGuardsRejectLanceOnlySyntax() throws Exception {
        try (InternalFixture fixture = new InternalFixture()) {
            Assertions.assertEquals("CREATE OR REPLACE INDEX is only supported for Lance catalog tables",
                    runAndGetMessage("CREATE OR REPLACE INDEX idx ON internal." + DB + "." + TBL
                            + " (c) USING INVERTED"));
            Assertions.assertEquals("USING BTREE is only supported for Lance catalog tables",
                    runAndGetMessage("CREATE INDEX idx ON internal." + DB + "." + TBL + " (c) USING BTREE"));
            Assertions.assertEquals("USING BITMAP is only supported for Lance catalog tables",
                    runAndGetMessage("CREATE INDEX idx ON internal." + DB + "." + TBL + " (c) USING BITMAP"));
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }
}
