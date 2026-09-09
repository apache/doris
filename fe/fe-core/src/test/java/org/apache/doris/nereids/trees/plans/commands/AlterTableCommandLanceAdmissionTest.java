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
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalDatabase;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LanceIndexAdmissionSnapshot;
import org.apache.doris.datasource.lance.LanceIndexAdmissionSnapshot.PhysicalIndexInfo;
import org.apache.doris.datasource.lance.LanceLogicalIndex;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobManager;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationType;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.schema.LanceField;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Command rewiring coverage for Lance index admission: with the gate off the typed rejection is
 * 5102 from {@code validate()} without any metadata read or id allocation; with the gate on the
 * top-level CREATE/DROP INDEX op bypasses the generic op loop and {@code alterTable}, runs
 * admission in {@code run()}, and answers a single-column {@code JobId} result set (one row when
 * admitted, zero rows on an IF no-op). The 3A static-validation and REST/alter=true rejections
 * are unchanged in both modes.
 */
public class AlterTableCommandLanceAdmissionTest {
    private static final String CTL = "lance_ctl";
    private static final String DB = "db";
    private static final String REMOTE_DB = "remote_db";
    private static final String TBL = "tbl";
    private static final String REMOTE_TBL = "remote_tbl";
    private static final long CATALOG_ID = 10L;
    private static final long DATASET_VERSION = 7L;
    private static final String DATASET_URI = "s3://bucket/dataset";
    private static final String VALID_ANN_PROPERTIES =
            "PROPERTIES(\"index_type\"=\"IVF_PQ\", \"metric\"=\"l2\", "
                    + "\"num_partitions\"=\"256\", \"num_sub_vectors\"=\"16\")";
    private static final String MATCHING_ANN_PROPERTIES_JSON =
            "{\"compression\":{\"num_bits\":8,\"num_sub_vectors\":16},\"metric_type\":\"l2\"}";

    private final NereidsParser parser = new NereidsParser();
    private ConnectContext connectContext;
    private boolean originalGate;

    @BeforeEach
    public void setUp() {
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("tester", "%"));
        originalGate = Config.enable_lance_index_mutation;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_lance_index_mutation = originalGate;
        ConnectContext.remove();
    }

    /** Edit-log seam: captures durable records instead of writing the journal (3B pattern). */
    private static class TestManager extends LanceIndexJobManager {
        private final List<LanceIndexJob> editLog = new ArrayList<>();

        @Override
        protected void writeEditLog(LanceIndexJob job) {
            editLog.add(job);
        }
    }

    /**
     * The 3A LanceFixture, extended for admission: remote/local names, the snapshot loader
     * entry point, real catalog/job managers behind the Env mock, and a mocked id allocator.
     */
    private static class LanceFixture implements AutoCloseable {
        private final MockedStatic<Env> mockedEnv;
        private final Env env;
        private final CatalogMgr catalogMgr;
        private final LanceExternalCatalog catalog;
        private final LanceExternalDatabase database;
        private final LanceExternalTable table;
        private final TestManager manager;
        private final AtomicLong idAllocator;
        private final StmtExecutor executor;

        LanceFixture(boolean restCatalog) throws Exception {
            mockedEnv = Mockito.mockStatic(Env.class);
            env = Mockito.mock(Env.class);
            catalogMgr = new CatalogMgr();
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            catalog = Mockito.mock(LanceExternalCatalog.class);
            database = Mockito.mock(LanceExternalDatabase.class);
            table = Mockito.mock(LanceExternalTable.class);
            manager = new TestManager();
            idAllocator = new AtomicLong(100L);
            executor = Mockito.mock(StmtExecutor.class);

            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class),
                    Mockito.eq(CTL), Mockito.eq(DB), Mockito.eq(TBL),
                    Mockito.eq(PrivPredicate.ALTER))).thenReturn(true);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            registerCatalog();
            Mockito.doReturn(database).when(catalog).getDbOrDdlException(DB);
            Mockito.doReturn(table).when(database).getTableOrDdlException(TBL);
            Mockito.when(catalog.isRestCatalogConfigured()).thenReturn(restCatalog);
            Mockito.when(catalog.getId()).thenReturn(CATALOG_ID);
            Mockito.when(catalog.getProperties()).thenReturn(Collections.emptyMap());
            Mockito.when(database.getRemoteName()).thenReturn(REMOTE_DB);
            Mockito.when(database.getFullName()).thenReturn(DB);
            Mockito.when(table.getRemoteName()).thenReturn(REMOTE_TBL);
            Mockito.when(table.getName()).thenReturn(TBL);
            Mockito.when(env.getNextId()).thenAnswer(invocation -> idAllocator.incrementAndGet());
            Mockito.when(env.getLanceIndexJobManager()).thenReturn(manager);
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
        }

        @SuppressWarnings("unchecked")
        private void registerCatalog() throws ReflectiveOperationException {
            Field idToCatalog = CatalogMgr.class.getDeclaredField("idToCatalog");
            idToCatalog.setAccessible(true);
            ((Map<Long, CatalogIf>) idToCatalog.get(catalogMgr)).put(CATALOG_ID, catalog);
            Field nameToCatalog = CatalogMgr.class.getDeclaredField("nameToCatalog");
            nameToCatalog.setAccessible(true);
            ((Map<String, CatalogIf>) nameToCatalog.get(catalogMgr)).put(CTL, catalog);
        }

        void respondWithSnapshot(List<LanceLogicalIndex> logical, List<PhysicalIndexInfo> physical)
                throws Exception {
            LanceField element = Mockito.mock(LanceField.class);
            Mockito.when(element.getId()).thenReturn(101);
            Mockito.when(element.getName()).thenReturn("item");
            Mockito.when(element.getType())
                    .thenReturn(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
            Mockito.when(element.isNullable()).thenReturn(false);
            Mockito.when(element.getChildren()).thenReturn(Collections.emptyList());
            LanceField vector = Mockito.mock(LanceField.class);
            Mockito.when(vector.getId()).thenReturn(1);
            Mockito.when(vector.getName()).thenReturn("v");
            Mockito.when(vector.getType()).thenReturn(new ArrowType.FixedSizeList(4));
            Mockito.when(vector.isNullable()).thenReturn(false);
            Mockito.when(vector.getChildren()).thenReturn(Collections.singletonList(element));
            LanceField scalar = Mockito.mock(LanceField.class);
            Mockito.when(scalar.getId()).thenReturn(2);
            Mockito.when(scalar.getName()).thenReturn("c");
            Mockito.when(scalar.getType()).thenReturn(new ArrowType.Int(32, true));
            Mockito.when(scalar.isNullable()).thenReturn(false);
            Mockito.when(scalar.getChildren()).thenReturn(Collections.emptyList());
            List<LanceField> fields = new ArrayList<>();
            fields.add(vector);
            fields.add(scalar);
            Mockito.when(catalog.loadTableIndexAdmissionSnapshot(REMOTE_DB, REMOTE_TBL))
                    .thenReturn(new LanceIndexAdmissionSnapshot(DATASET_VERSION, DATASET_URI,
                            logical, physical, fields));
        }

        @Override
        public void close() {
            mockedEnv.close();
        }
    }

    /** Mocked catalog resolution chain ending at an internal OlapTable (same as 3A). */
    private static class InternalFixture implements AutoCloseable {
        private final MockedStatic<Env> mockedEnv;
        private final Env env;
        private final StmtExecutor executor;

        InternalFixture() throws DdlException {
            mockedEnv = Mockito.mockStatic(Env.class);
            env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
            DatabaseIf database = Mockito.mock(DatabaseIf.class);
            OlapTable table = Mockito.mock(OlapTable.class);
            executor = Mockito.mock(StmtExecutor.class);

            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            // ConnectContext.setEnv reads the default catalog name off the internal catalog.
            Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
            Mockito.when(internalCatalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
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

    private String runAndGetMessage(StmtExecutor executor, String sql) {
        AlterTableCommand command = (AlterTableCommand) parser.parseSingle(sql);
        try {
            command.run(connectContext, executor);
            throw new AssertionError("expected an AnalysisException but the statement succeeded: " + sql);
        } catch (AnalysisException e) {
            return e.getDetailMessage();
        } catch (Exception e) {
            throw new AssertionError("unexpected exception " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    private AnalysisException runAndGetCommonAnalysisException(StmtExecutor executor, String sql) {
        AlterTableCommand command = (AlterTableCommand) parser.parseSingle(sql);
        try {
            command.run(connectContext, executor);
            throw new AssertionError("expected an AnalysisException but the statement succeeded: " + sql);
        } catch (AnalysisException e) {
            return e;
        } catch (Exception e) {
            throw new AssertionError("unexpected exception " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    private AlterTableCommand run(StmtExecutor executor, String sql) throws Exception {
        AlterTableCommand command = (AlterTableCommand) parser.parseSingle(sql);
        command.run(connectContext, executor);
        return command;
    }

    private static void assertSingleJobIdColumn(ResultSet resultSet) {
        Assertions.assertEquals(1, resultSet.getMetaData().getColumnCount());
        Assertions.assertEquals("JobId", resultSet.getMetaData().getColumns().get(0).getName());
    }

    // ------------------------------------------------------------------
    // Gate off: 5102 from validate(), no metadata read, no id allocation
    // ------------------------------------------------------------------

    @Test
    public void gateOffRejectsCreateIndexAsDisabled() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false)) {
            String message = runAndGetMessage(fixture.executor,
                    "CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (v) USING ANN "
                            + VALID_ANN_PROPERTIES);
            Assertions.assertEquals(
                    "CREATE INDEX is disabled for Lance catalog tables (enable_lance_index_mutation = false)",
                    message);
            AnalysisException exception = runAndGetCommonAnalysisException(fixture.executor,
                    "CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (v) USING ANN "
                            + VALID_ANN_PROPERTIES);
            Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_DISABLED,
                    exception.getMysqlErrorCode());
            // The gate-off rejection never reaches the snapshot read, id allocation, or a result set.
            Mockito.verify(fixture.catalog, Mockito.never())
                    .loadTableIndexAdmissionSnapshot(Mockito.anyString(), Mockito.anyString());
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
            Mockito.verify(fixture.executor, Mockito.never()).sendResultSet(Mockito.any());
        }
    }

    @Test
    public void gateOffRejectsCreateOrReplaceIndexAsDisabled() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false)) {
            Assertions.assertEquals(
                    "CREATE OR REPLACE INDEX is disabled for Lance catalog tables "
                            + "(enable_lance_index_mutation = false)",
                    runAndGetMessage(fixture.executor,
                            "CREATE OR REPLACE INDEX idx ON " + CTL + "." + DB + "." + TBL
                                    + " (c) USING BTREE"));
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void gateOffRejectsDropIndexAsDisabled() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false)) {
            Assertions.assertEquals(
                    "DROP INDEX is disabled for Lance catalog tables (enable_lance_index_mutation = false)",
                    runAndGetMessage(fixture.executor, "DROP INDEX idx ON " + CTL + "." + DB + "." + TBL));
            Assertions.assertEquals(
                    "DROP INDEX is disabled for Lance catalog tables (enable_lance_index_mutation = false)",
                    runAndGetMessage(fixture.executor,
                            "DROP INDEX IF EXISTS idx ON " + CTL + "." + DB + "." + TBL));
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    // ------------------------------------------------------------------
    // Gate on: admission in run(), JobId result set, alterTable bypassed
    // ------------------------------------------------------------------

    @Test
    public void gateOnCreateIndexAdmitsAndReturnsJobId() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (LanceFixture fixture = new LanceFixture(false)) {
            fixture.respondWithSnapshot(Collections.emptyList(), Collections.emptyList());

            run(fixture.executor, "CREATE INDEX MyIdx ON " + CTL + "." + DB + "." + TBL
                    + " (v) USING ANN " + VALID_ANN_PROPERTIES);

            ArgumentCaptor<ResultSet> captor = ArgumentCaptor.forClass(ResultSet.class);
            Mockito.verify(fixture.executor, Mockito.times(1)).sendResultSet(captor.capture());
            assertSingleJobIdColumn(captor.getValue());
            Assertions.assertEquals(1, captor.getValue().getResultRows().size());
            long jobId = fixture.idAllocator.get();
            Assertions.assertEquals(String.valueOf(jobId),
                    captor.getValue().getResultRows().get(0).get(0));

            // Admission short-circuits the generic alter path entirely.
            Mockito.verify(fixture.env, Mockito.never()).alterTable(Mockito.any());
            Mockito.verify(fixture.env, Mockito.times(1)).getNextId();
            Assertions.assertEquals(1, fixture.manager.getJobCount());
            Assertions.assertEquals(1, fixture.manager.editLog.size());
            LanceIndexJob job = fixture.manager.getJob(jobId);
            Assertions.assertEquals(LanceIndexJobMutationType.CREATE, job.getMutationType());
            Assertions.assertEquals("MyIdx", job.getDisplayIndexName());
            Assertions.assertEquals(DB, job.getDbName());
            Assertions.assertEquals(TBL, job.getTableName());
        }
    }

    @Test
    public void gateOnCreateIfNotExistsMatchingReturnsZeroRows() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (LanceFixture fixture = new LanceFixture(false)) {
            fixture.respondWithSnapshot(
                    Collections.singletonList(new LanceLogicalIndex("idx",
                            Collections.singletonList("v"), "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                    Collections.singletonList(
                            new PhysicalIndexInfo("idx", "uuid-1", DATASET_VERSION, "VECTOR")));

            run(fixture.executor, "CREATE INDEX IF NOT EXISTS idx ON " + CTL + "." + DB + "." + TBL
                    + " (v) USING ANN " + VALID_ANN_PROPERTIES);

            ArgumentCaptor<ResultSet> captor = ArgumentCaptor.forClass(ResultSet.class);
            Mockito.verify(fixture.executor, Mockito.times(1)).sendResultSet(captor.capture());
            assertSingleJobIdColumn(captor.getValue());
            // The IF no-op creates no job: same schema, zero rows.
            Assertions.assertTrue(captor.getValue().getResultRows().isEmpty());
            Mockito.verify(fixture.env, Mockito.never()).alterTable(Mockito.any());
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
            Assertions.assertEquals(0, fixture.manager.getJobCount());
            Assertions.assertTrue(fixture.manager.editLog.isEmpty());
        }
    }

    @Test
    public void gateOnDropIfExistsAbsentReturnsZeroRows() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (LanceFixture fixture = new LanceFixture(false)) {
            fixture.respondWithSnapshot(Collections.emptyList(), Collections.emptyList());

            run(fixture.executor, "DROP INDEX IF EXISTS idx ON " + CTL + "." + DB + "." + TBL);

            ArgumentCaptor<ResultSet> captor = ArgumentCaptor.forClass(ResultSet.class);
            Mockito.verify(fixture.executor, Mockito.times(1)).sendResultSet(captor.capture());
            assertSingleJobIdColumn(captor.getValue());
            Assertions.assertTrue(captor.getValue().getResultRows().isEmpty());
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
            Assertions.assertEquals(0, fixture.manager.getJobCount());
        }
    }

    @Test
    public void gateOnRejectedAdmissionSendsNothingAndAllocatesNoId() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (LanceFixture fixture = new LanceFixture(false)) {
            fixture.respondWithSnapshot(
                    Collections.singletonList(new LanceLogicalIndex("idx",
                            Collections.singletonList("v"), "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                    Collections.singletonList(
                            new PhysicalIndexInfo("idx", "uuid-1", DATASET_VERSION, "VECTOR")));

            String message = runAndGetMessage(fixture.executor,
                    "CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (v) USING ANN "
                            + VALID_ANN_PROPERTIES);
            Assertions.assertEquals("index 'idx' already exists", message);
            Mockito.verify(fixture.executor, Mockito.never()).sendResultSet(Mockito.any());
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
            Mockito.verify(fixture.env, Mockito.never()).alterTable(Mockito.any());
            Assertions.assertEquals(0, fixture.manager.getJobCount());
            Assertions.assertTrue(fixture.manager.editLog.isEmpty());
        }
    }

    @Test
    public void gateOnProxyExecutionUsesTheProxyResultSet() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (LanceFixture fixture = new LanceFixture(false)) {
            fixture.respondWithSnapshot(
                    Collections.singletonList(new LanceLogicalIndex("idx",
                            Collections.singletonList("c"), "BTREE", "{}")),
                    Collections.singletonList(
                            new PhysicalIndexInfo("idx", "uuid-1", DATASET_VERSION, "SCALAR")));
            Mockito.when(fixture.executor.isProxy()).thenReturn(true);

            run(fixture.executor, "DROP INDEX idx ON " + CTL + "." + DB + "." + TBL);

            ArgumentCaptor<ShowResultSet> captor = ArgumentCaptor.forClass(ShowResultSet.class);
            Mockito.verify(fixture.executor, Mockito.times(1)).setProxyShowResultSet(captor.capture());
            Mockito.verify(fixture.executor, Mockito.never()).sendResultSet(Mockito.any());
            assertSingleJobIdColumn(captor.getValue());
            Assertions.assertEquals(1, captor.getValue().getResultRows().size());
            Assertions.assertEquals(String.valueOf(fixture.idAllocator.get()),
                    captor.getValue().getResultRows().get(0).get(0));
        }
    }

    // ------------------------------------------------------------------
    // Unchanged behavior under both modes
    // ------------------------------------------------------------------

    @Test
    public void restCatalogStillFailsFastWith5101WhenGateIsOn() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (LanceFixture fixture = new LanceFixture(true)) {
            AnalysisException exception = runAndGetCommonAnalysisException(fixture.executor,
                    "CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (v) USING ANN "
                            + VALID_ANN_PROPERTIES);
            Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_OPERATION_NOT_SUPPORTED,
                    exception.getMysqlErrorCode());
            Assertions.assertEquals("CREATE INDEX is not supported for Lance REST catalogs",
                    exception.getDetailMessage());
            Assertions.assertEquals("DROP INDEX is not supported for Lance REST catalogs",
                    runAndGetMessage(fixture.executor, "DROP INDEX idx ON " + CTL + "." + DB + "." + TBL));
            Mockito.verify(fixture.catalog, Mockito.never())
                    .loadTableIndexAdmissionSnapshot(Mockito.anyString(), Mockito.anyString());
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void reservedSystemNameIsRejectedStaticallyWithTheGateOff() throws Exception {
        try (LanceFixture fixture = new LanceFixture(false)) {
            Assertions.assertEquals(
                    "index name '__lance_idx' uses the reserved '__lance_' prefix of Lance system indexes",
                    runAndGetMessage(fixture.executor, "CREATE INDEX `__lance_idx` ON " + CTL + "." + DB
                            + "." + TBL + " (v) USING ANN " + VALID_ANN_PROPERTIES));
            Assertions.assertEquals(
                    "index name '__lance_idx' uses the reserved '__lance_' prefix of Lance system indexes",
                    runAndGetMessage(fixture.executor, "CREATE OR REPLACE INDEX `__lance_idx` ON " + CTL
                            + "." + DB + "." + TBL + " (c) USING BTREE"));
            Assertions.assertEquals(
                    "index name '__lance_idx' uses the reserved '__lance_' prefix of Lance system indexes",
                    runAndGetMessage(fixture.executor,
                            "DROP INDEX `__lance_idx` ON " + CTL + "." + DB + "." + TBL));
            AnalysisException exception = runAndGetCommonAnalysisException(fixture.executor,
                    "DROP INDEX `__lance_idx` ON " + CTL + "." + DB + "." + TBL);
            Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_INVALID, exception.getMysqlErrorCode());
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void reservedSystemNameIsRejectedStaticallyWithTheGateOn() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (LanceFixture fixture = new LanceFixture(false)) {
            Assertions.assertEquals(
                    "index name '__lance_idx' uses the reserved '__lance_' prefix of Lance system indexes",
                    runAndGetMessage(fixture.executor, "CREATE INDEX `__lance_idx` ON " + CTL + "." + DB
                            + "." + TBL + " (v) USING ANN " + VALID_ANN_PROPERTIES));
            Assertions.assertEquals(
                    "index name '__lance_idx' uses the reserved '__lance_' prefix of Lance system indexes",
                    runAndGetMessage(fixture.executor, "CREATE OR REPLACE INDEX `__lance_idx` ON " + CTL
                            + "." + DB + "." + TBL + " (c) USING BTREE"));
            Assertions.assertEquals(
                    "index name '__lance_idx' uses the reserved '__lance_' prefix of Lance system indexes",
                    runAndGetMessage(fixture.executor,
                            "DROP INDEX `__lance_idx` ON " + CTL + "." + DB + "." + TBL));
            Mockito.verify(fixture.catalog, Mockito.never())
                    .loadTableIndexAdmissionSnapshot(Mockito.anyString(), Mockito.anyString());
            Mockito.verify(fixture.env, Mockito.never()).getNextId();
        }
    }

    @Test
    public void alterTrueIndexOpsKeepTheGenericRejectionWhenGateIsOn() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (LanceFixture fixture = new LanceFixture(false)) {
            String addMessage = runAndGetMessage(fixture.executor,
                    "ALTER TABLE " + CTL + "." + DB + "." + TBL + " ADD INDEX idx (c) USING INVERTED");
            Assertions.assertTrue(addMessage.contains("do not support SCHEMA_CHANGE clause now"),
                    addMessage);
            String dropMessage = runAndGetMessage(fixture.executor,
                    "ALTER TABLE " + CTL + "." + DB + "." + TBL + " DROP INDEX idx");
            Assertions.assertTrue(dropMessage.contains("do not support SCHEMA_CHANGE clause now"),
                    dropMessage);
            Mockito.verify(fixture.catalog, Mockito.never())
                    .loadTableIndexAdmissionSnapshot(Mockito.anyString(), Mockito.anyString());
            Mockito.verify(fixture.env, Mockito.never()).alterTable(Mockito.any());
        }
    }

    @Test
    public void internalTableIsUnaffectedByTheGate() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (InternalFixture fixture = new InternalFixture()) {
            connectContext.setEnv(fixture.env);
            run(fixture.executor,
                    "CREATE INDEX idx ON internal." + DB + "." + TBL + " (c) USING INVERTED");
            Mockito.verify(fixture.env, Mockito.times(1)).alterTable(Mockito.any());
            Mockito.verify(fixture.executor, Mockito.never()).sendResultSet(Mockito.any());
        }
    }

    @Test
    public void validateRunsExactlyOncePerCommandObject() throws Exception {
        Config.enable_lance_index_mutation = true;
        try (LanceFixture fixture = new LanceFixture(false)) {
            fixture.respondWithSnapshot(Collections.emptyList(), Collections.emptyList());
            AlterTableCommand command = run(fixture.executor,
                    "CREATE INDEX idx ON " + CTL + "." + DB + "." + TBL + " (c) USING BTREE");
            Assertions.assertThrows(IllegalStateException.class,
                    () -> command.run(connectContext, fixture.executor));
            Mockito.verify(fixture.env, Mockito.times(1)).getNextId();
        }
    }
}
