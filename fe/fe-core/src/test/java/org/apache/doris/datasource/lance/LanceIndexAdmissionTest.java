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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.lance.LanceIndexAdmissionSnapshot.PhysicalIndexInfo;
import org.apache.doris.datasource.lance.job.LanceIndexFenceKey;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobManager;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationType;
import org.apache.doris.datasource.lance.job.LanceIndexSchemaContract;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.qe.ConnectContext;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.lance.schema.LanceField;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Section 2.2/4.1 admission coverage for {@link LanceIndexAdmission}: snapshots are injected
 * through the {@code SnapshotLoader} seam (no FE, no JNI), the job manager is the real 3B
 * implementation with its edit-log seam captured, and {@code Env} is mocked only for id
 * allocation and the manager getter. Every rejection path must leave no job, no fence, no
 * quota charge, no journal record, and no id allocation behind (the 3B ReplayTest pattern).
 */
public class LanceIndexAdmissionTest {
    private static final long CATALOG_ID = 10L;
    private static final String DB = "db";
    private static final String REMOTE_DB = "remote_db";
    private static final String TBL = "tbl";
    private static final String REMOTE_TBL = "remote_tbl";
    private static final String DATASET_URI = "s3://bucket/dataset";
    private static final long DATASET_VERSION = 7L;
    private static final String MATCHING_ANN_PROPERTIES_JSON =
            "{\"compression\":{\"num_bits\":8,\"num_sub_vectors\":16},\"metric_type\":\"l2\"}";
    private static final String NORMALIZED_ANN_PROPERTIES_JSON =
            "{\"index_type\":\"IVF_PQ\",\"metric\":\"l2\",\"num_bits\":\"8\","
                    + "\"num_partitions\":\"256\",\"num_sub_vectors\":\"16\"}";

    private MockedStatic<Env> mockedEnv;
    private Env env;
    private TestManager manager;
    private AtomicLong idAllocator;
    private LanceExternalCatalog catalog;
    private LanceExternalDatabase database;
    private LanceExternalTable table;
    private ConnectContext connectContext;
    private long originalTableQuota;
    private long originalCatalogQuota;
    private long originalGlobalQuota;

    @BeforeEach
    public void setUp() throws Exception {
        mockedEnv = Mockito.mockStatic(Env.class);
        env = Mockito.mock(Env.class);
        manager = new TestManager();
        idAllocator = new AtomicLong(100L);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getNextId()).thenAnswer(invocation -> idAllocator.incrementAndGet());
        Mockito.when(env.getLanceIndexJobManager()).thenReturn(manager);

        catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(CATALOG_ID);
        Mockito.when(catalog.getProperties()).thenReturn(new HashMap<>());
        CatalogMgr catalogMgr = new CatalogMgr();
        Field catalogs = CatalogMgr.class.getDeclaredField("idToCatalog");
        catalogs.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Long, CatalogIf> registered = (Map<Long, CatalogIf>) catalogs.get(catalogMgr);
        registered.put(CATALOG_ID, catalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        database = Mockito.mock(LanceExternalDatabase.class);
        Mockito.when(database.getRemoteName()).thenReturn(REMOTE_DB);
        Mockito.when(database.getFullName()).thenReturn(DB);
        table = Mockito.mock(LanceExternalTable.class);
        Mockito.when(table.getRemoteName()).thenReturn(REMOTE_TBL);
        Mockito.when(table.getName()).thenReturn(TBL);
        Map<String, Column> columns = new HashMap<>();
        columns.put("v", notNullColumn("v", new ArrayType(Type.FLOAT)));
        columns.put("c", notNullColumn("c", Type.INT));
        columns.put("s", notNullColumn("s", Type.STRING));
        columns.put("Embedding", notNullColumn("Embedding", new ArrayType(Type.FLOAT)));
        // Case-insensitive lookup, mirroring ExternalTable.getColumn.
        Mockito.when(table.getColumn(Mockito.anyString())).thenAnswer(invocation -> {
            String name = invocation.getArgument(0);
            for (Map.Entry<String, Column> entry : columns.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(name)) {
                    return entry.getValue();
                }
            }
            return null;
        });

        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("tester", "%"));

        originalTableQuota = Config.lance_index_job_max_unresolved_per_table;
        originalCatalogQuota = Config.lance_index_job_max_unresolved_per_catalog;
        originalGlobalQuota = Config.lance_index_job_max_unresolved_global;
    }

    @AfterEach
    public void tearDown() {
        Config.lance_index_job_max_unresolved_per_table = originalTableQuota;
        Config.lance_index_job_max_unresolved_per_catalog = originalCatalogQuota;
        Config.lance_index_job_max_unresolved_global = originalGlobalQuota;
        ConnectContext.remove();
        mockedEnv.close();
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    /** Edit-log seam: captures durable records instead of writing the journal (3B pattern). */
    private static class TestManager extends LanceIndexJobManager {
        private final List<LanceIndexJob> editLog = new ArrayList<>();

        @Override
        protected void writeEditLog(LanceIndexJob job) {
            editLog.add(job);
        }
    }

    private static Column notNullColumn(String name, Type type) {
        return new Column(name, type, false, null, false, null, "");
    }

    private static LanceField vectorField(String name, int id) {
        LanceField element = Mockito.mock(LanceField.class);
        Mockito.when(element.getId()).thenReturn(id * 100 + 1);
        Mockito.when(element.getName()).thenReturn("item");
        Mockito.when(element.getType())
                .thenReturn(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        Mockito.when(element.isNullable()).thenReturn(false);
        Mockito.when(element.getChildren()).thenReturn(Collections.emptyList());
        LanceField field = Mockito.mock(LanceField.class);
        Mockito.when(field.getId()).thenReturn(id);
        Mockito.when(field.getName()).thenReturn(name);
        Mockito.when(field.getType()).thenReturn(new ArrowType.FixedSizeList(4));
        Mockito.when(field.isNullable()).thenReturn(false);
        Mockito.when(field.getChildren()).thenReturn(Collections.singletonList(element));
        return field;
    }

    private static LanceField scalarField(String name, int id, ArrowType type) {
        LanceField field = Mockito.mock(LanceField.class);
        Mockito.when(field.getId()).thenReturn(id);
        Mockito.when(field.getName()).thenReturn(name);
        Mockito.when(field.getType()).thenReturn(type);
        Mockito.when(field.isNullable()).thenReturn(false);
        Mockito.when(field.getChildren()).thenReturn(Collections.emptyList());
        return field;
    }

    private static List<LanceField> defaultFields() {
        return Arrays.asList(vectorField("v", 1), scalarField("c", 2, new ArrowType.Int(32, true)),
                scalarField("s", 3, new ArrowType.Utf8()), vectorField("Embedding", 4));
    }

    private static LanceIndexAdmissionSnapshot snapshot(List<LanceLogicalIndex> logical,
            List<PhysicalIndexInfo> physical) {
        return new LanceIndexAdmissionSnapshot(DATASET_VERSION, DATASET_URI, logical, physical,
                defaultFields());
    }

    private static LanceIndexAdmissionSnapshot emptySnapshot() {
        return snapshot(Collections.emptyList(), Collections.emptyList());
    }

    private static LanceLogicalIndex logicalIndex(String name, String column, String indexType,
            String propertiesJson) {
        return new LanceLogicalIndex(name, Collections.singletonList(column), indexType, propertiesJson);
    }

    private static PhysicalIndexInfo physicalIndex(String name, String indexTypeName) {
        return new PhysicalIndexInfo(name, "uuid-" + name, DATASET_VERSION, indexTypeName);
    }

    private static Map<String, String> annProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("index_type", "IVF_PQ");
        properties.put("metric", "l2");
        properties.put("num_partitions", "256");
        properties.put("num_sub_vectors", "16");
        return properties;
    }

    private static IndexDefinition annDef(String name, boolean ifNotExists, boolean orReplace) {
        return new IndexDefinition(name, ifNotExists, Collections.singletonList("v"), "ANN",
                annProperties(), "", orReplace);
    }

    private static IndexDefinition annDef(String name, boolean ifNotExists, boolean orReplace,
            Map<String, String> properties) {
        return new IndexDefinition(name, ifNotExists, Collections.singletonList("v"), "ANN",
                properties, "", orReplace);
    }

    private static IndexDefinition scalarDef(String name, String lanceType, String column,
            boolean ifNotExists, boolean orReplace) {
        return new IndexDefinition(name, ifNotExists, Collections.singletonList(column), lanceType,
                new HashMap<>(), "", orReplace);
    }

    private LanceIndexAdmission.Outcome admitCreate(LanceIndexAdmissionSnapshot snapshot,
            IndexDefinition def, boolean ifNotExists) throws Exception {
        return LanceIndexAdmission.admitCreate((cat, dbName, tblName) -> snapshot, catalog, database,
                table, def, ifNotExists);
    }

    private LanceIndexAdmission.Outcome admitDrop(LanceIndexAdmissionSnapshot snapshot,
            String indexName, boolean ifExists) throws Exception {
        return LanceIndexAdmission.admitDrop((cat, dbName, tblName) -> snapshot, catalog, database,
                table, indexName, ifExists);
    }

    private void assertNothingPersisted() {
        Assertions.assertEquals(0, manager.getJobCount());
        Assertions.assertTrue(manager.editLog.isEmpty());
        Assertions.assertTrue(manager.getUnresolvedJobs().isEmpty());
        Mockito.verify(env, Mockito.never()).getNextId();
    }

    /** Unresolved-job count on this dataset locator — the public view of the table quota. */
    private int unresolvedJobsOnDataset() {
        int count = 0;
        for (LanceIndexJob job : manager.getUnresolvedJobs()) {
            if (job.getCatalogId() == CATALOG_ID && DATASET_URI.equals(job.getNormalizedLocator())) {
                count++;
            }
        }
        return count;
    }

    private static void assertInvalid(Executable call, String expectedMessage) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, call);
        Assertions.assertEquals(expectedMessage, exception.getDetailMessage());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_INVALID, exception.getMysqlErrorCode());
    }

    // ------------------------------------------------------------------
    // CREATE admission
    // ------------------------------------------------------------------

    @Test
    public void plainCreateOnEmptySnapshotIsAdmitted() throws Exception {
        LanceIndexAdmission.Outcome outcome = admitCreate(emptySnapshot(), annDef("MyIdx", false, false),
                false);

        Assertions.assertNotNull(outcome.getJobId());
        LanceIndexJob job = manager.getJob(outcome.getJobId());
        Assertions.assertNotNull(job);
        Assertions.assertEquals("tester", job.getCreator());
        Assertions.assertEquals(CATALOG_ID, job.getCatalogId());
        // Local names are persisted for privilege/FORCE resolution, not the remote ones.
        Assertions.assertEquals(DB, job.getDbName());
        Assertions.assertEquals(TBL, job.getTableName());
        Assertions.assertEquals(LanceIndexFenceKey.PROVIDER_DIRECTORY, job.getProvider());
        Assertions.assertEquals(DATASET_URI, job.getNormalizedLocator());
        Assertions.assertEquals("MyIdx", job.getDisplayIndexName());
        Assertions.assertEquals("myidx", job.getNormalizedIndexName());
        Assertions.assertEquals(LanceIndexJobMutationType.CREATE, job.getMutationType());
        Assertions.assertFalse(job.isIfNotExists());
        Assertions.assertFalse(job.isIfExists());
        Assertions.assertEquals("IVF_PQ", job.getIndexType());
        Assertions.assertEquals("v", job.getColumnName());
        Assertions.assertEquals(NORMALIZED_ANN_PROPERTIES_JSON, job.getPropertiesJson());
        Assertions.assertEquals(DATASET_VERSION, job.getAdmittedDatasetVersion());

        LanceIndexSchemaContract contract = job.getSchemaContract();
        Assertions.assertNotNull(contract);
        Assertions.assertEquals(LanceIndexSchemaContract.SCHEMA_CONTRACT_VERSION_V1,
                contract.getSchemaContractVersion());
        Assertions.assertEquals(1, contract.getFields().size());
        LanceIndexSchemaContract.IndexedField field = contract.getFields().get(0);
        Assertions.assertEquals(1L, field.getFieldId());
        Assertions.assertEquals("v", field.getNormalizedName());
        Assertions.assertEquals("fixed_size_list", field.getNormalizedType());
        Assertions.assertFalse(field.isNullable());
        Assertions.assertEquals(4, field.getFixedSizeListDimension());
        Assertions.assertEquals("float32", field.getVectorElementType());
        Assertions.assertEquals(Boolean.FALSE, field.getVectorElementNullable());

        Assertions.assertEquals(1, manager.getJobCount());
        Assertions.assertEquals(1, manager.editLog.size());
        Assertions.assertTrue(manager.isFenceHeld(job.fenceKey()));
        Assertions.assertEquals(1, manager.getUnresolvedJobs().size());
        Mockito.verify(env, Mockito.times(1)).getNextId();
    }

    @Test
    public void defaultLoaderDelegatesToTheCatalogWithRemoteNames() throws Exception {
        // Materialize the snapshot before stubbing: building it mocks LanceField, and Mockito
        // rejects nested stubbing inside a when(...) call.
        LanceIndexAdmissionSnapshot prepared = emptySnapshot();
        Mockito.when(catalog.loadTableIndexAdmissionSnapshot(REMOTE_DB, REMOTE_TBL))
                .thenReturn(prepared);

        LanceIndexAdmission.Outcome outcome = LanceIndexAdmission.admitCreate(catalog, database, table,
                scalarDef("Idx", "BTREE", "c", false, false), false);

        Assertions.assertNotNull(outcome.getJobId());
        Mockito.verify(catalog, Mockito.times(1))
                .loadTableIndexAdmissionSnapshot(REMOTE_DB, REMOTE_TBL);
    }

    @Test
    public void plainCreateWithExistingNameIsRejected() {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));

        assertInvalid(() -> admitCreate(snapshot, annDef("idx", false, false), false),
                "index 'idx' already exists");
        // Case-only variants resolve to the same stored name and are equally "already exists".
        assertInvalid(() -> admitCreate(snapshot, annDef("IDX", false, false), false),
                "index 'IDX' already exists");
        assertNothingPersisted();
    }

    @Test
    public void createIfNotExistsWithMatchingDefinitionIsNoOp() throws Exception {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("IdxA", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("IdxA", "VECTOR")));

        LanceIndexAdmission.Outcome outcome = admitCreate(snapshot, annDef("idxa", true, false), true);

        Assertions.assertNull(outcome.getJobId());
        assertNothingPersisted();
    }

    @Test
    public void createIfNotExistsWithDifferentDefinitionIsRejected() {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "c", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));

        // Same name and algorithm, but the indexed column differs.
        assertInvalid(() -> admitCreate(snapshot, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");
        assertNothingPersisted();
    }

    @Test
    public void sameNameDifferentScalarAlgorithmIsRejected() {
        // M1: a BTREE request against a same-name BITMAP index must never no-op.
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "c", "BITMAP", "{}")),
                Collections.singletonList(physicalIndex("idx", "SCALAR")));

        assertInvalid(() -> admitCreate(snapshot, scalarDef("idx", "BTREE", "c", true, false), true),
                "index 'idx' already exists with a different definition");
        assertNothingPersisted();
    }

    @Test
    public void sameNameDifferentVectorAlgorithmIsRejected() {
        // M1: IVF_PQ against a same-name IVF_HNSW_PQ index whose exposed whitelist values
        // happen to match must still fail on the algorithm inequality.
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(
                        logicalIndex("idx", "v", "IVF_HNSW_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));

        assertInvalid(() -> admitCreate(snapshot, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");
        assertNothingPersisted();
    }

    @Test
    public void missingPhysicalEntryIsNotAMatch() {
        // M1(b): the logical index alone cannot corroborate itself; without a physical entry
        // for the name the snapshot is not self-consistent and the definition mismatches.
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.emptyList());

        assertInvalid(() -> admitCreate(snapshot, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");
        assertNothingPersisted();
    }

    @Test
    public void incompatiblePhysicalFamilyIsNotAMatch() {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("idx", "SCALAR")));

        assertInvalid(() -> admitCreate(snapshot, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");
        assertNothingPersisted();
    }

    @Test
    public void propertiesAbsentFromSnapshotAreSkipped() throws Exception {
        // N1: when the snapshot exposes no whitelist value at all, no property is compared.
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ", "{}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));

        Assertions.assertNull(admitCreate(snapshot, annDef("idx", true, false), true).getJobId());
        assertNothingPersisted();
    }

    @Test
    public void exposedSnapshotPropertyMismatchIsRejected() throws Exception {
        // N1: an exposed stable value must be equal; an unexposed one is skipped.
        LanceIndexAdmissionSnapshot mismatchedMetric = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"metric_type\":\"cosine\"}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));
        assertInvalid(() -> admitCreate(mismatchedMetric, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");

        LanceIndexAdmissionSnapshot mismatchedSubVectors = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"compression\":{\"num_sub_vectors\":32}}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));
        assertInvalid(() -> admitCreate(mismatchedSubVectors, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");

        // num_partitions is never compared (design section 2.2).
        LanceIndexAdmissionSnapshot partialExposure = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"compression\":{\"num_bits\":8}}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));
        Assertions.assertNull(admitCreate(partialExposure, annDef("idx", true, false), true).getJobId());

        // An exposed but non-numeric value is malformed data: fail closed, never skip.
        LanceIndexAdmissionSnapshot nonNumeric = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"compression\":{\"num_sub_vectors\":\"sixteen\"}}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));
        assertInvalid(() -> admitCreate(nonNumeric, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");
        assertNothingPersisted();
    }

    @Test
    public void exposedIntegerNumericPropertiesMatch() throws Exception {
        for (String exposedValue : new String[] {"16", "\"16\"", "\"016\"", "\"+16\""}) {
            LanceIndexAdmissionSnapshot snapshot = snapshot(
                    Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                            "{\"compression\":{\"num_sub_vectors\":" + exposedValue + "}}")),
                    Collections.singletonList(physicalIndex("idx", "VECTOR")));
            Assertions.assertNull(admitCreate(snapshot, annDef("idx", true, false), true).getJobId(),
                    exposedValue);
        }
        assertNothingPersisted();
    }

    @Test
    public void exposedNonIntegerNumericPropertiesFailClosed() throws Exception {
        // Gson's getAsLong truncates fractions and wraps overflowing JSON numbers. Require
        // parsed-long semantics for both numeric primitives and strings instead.
        for (String exposedValue : new String[] {"16.9", "16.0", "1.6e1", "18446744073709551632",
                "\"16.9\"", "\"1.6e1\"", "\"18446744073709551632\"", "true", "null"}) {
            LanceIndexAdmissionSnapshot snapshot = snapshot(
                    Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                            "{\"compression\":{\"num_sub_vectors\":" + exposedValue + "}}")),
                    Collections.singletonList(physicalIndex("idx", "VECTOR")));
            assertInvalid(() -> admitCreate(snapshot, annDef("idx", true, false), true),
                    "index 'idx' already exists with a different definition");
        }
        assertNothingPersisted();
    }

    @Test
    public void exposedNonPrimitiveMetricFailsClosed() throws Exception {
        // An exposed but non-primitive metric is malformed data too: fail closed, never skip.
        LanceIndexAdmissionSnapshot nonPrimitiveMetric = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"metric_type\":{\"unexpected\":\"object\"}}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));
        assertInvalid(() -> admitCreate(nonPrimitiveMetric, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");
        assertNothingPersisted();
    }

    @Test
    public void exposedNonPrimitiveNumericFailsClosed() throws Exception {
        // Symmetric with the metric rule: an exposed but non-primitive numeric value is
        // malformed data, not "no stable value": fail closed, never skip.
        LanceIndexAdmissionSnapshot nonPrimitiveSubVectors = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"compression\":{\"num_sub_vectors\":{\"unexpected\":\"object\"}}}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));
        assertInvalid(() -> admitCreate(nonPrimitiveSubVectors, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");

        // A compression block that is present but not an object is malformed too.
        LanceIndexAdmissionSnapshot nonObjectCompression = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"compression\":\"unexpected\"}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));
        assertInvalid(() -> admitCreate(nonObjectCompression, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");
        assertNothingPersisted();
    }

    @Test
    public void exposedMetricComparisonIsCaseInsensitive() throws Exception {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"metric_type\":\"L2\"}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));

        Assertions.assertNull(admitCreate(snapshot, annDef("idx", true, false), true).getJobId());
        assertNothingPersisted();
    }

    @Test
    public void replaceOnAbsentNameCreatesWithUserSpelling() throws Exception {
        LanceIndexAdmission.Outcome outcome = admitCreate(emptySnapshot(),
                annDef("Fresh", false, true), false);

        LanceIndexJob job = manager.getJob(outcome.getJobId());
        Assertions.assertEquals(LanceIndexJobMutationType.REPLACE, job.getMutationType());
        Assertions.assertEquals("Fresh", job.getDisplayIndexName());
        Assertions.assertEquals("fresh", job.getNormalizedIndexName());
    }

    @Test
    public void replaceWithCaseVariantPersistsTheStoredName() throws Exception {
        // M2: the worker locates the target case-sensitively, so the stored display name is
        // persisted instead of the user's spelling.
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("IdxA", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("IdxA", "VECTOR")));

        LanceIndexAdmission.Outcome outcome = admitCreate(snapshot, annDef("IDXA", false, true), false);

        LanceIndexJob job = manager.getJob(outcome.getJobId());
        Assertions.assertEquals(LanceIndexJobMutationType.REPLACE, job.getMutationType());
        Assertions.assertEquals("IdxA", job.getDisplayIndexName());
        Assertions.assertEquals("idxa", job.getNormalizedIndexName());
    }

    @Test
    public void mixedCaseColumnResolvesToTheStoredColumnName() throws Exception {
        // N5: static validation is case-insensitive; the contract and the persisted column name
        // use the stored Lance field name.
        LanceIndexAdmission.Outcome outcome = admitCreate(emptySnapshot(),
                new IndexDefinition("idx", false, Collections.singletonList("embedding"), "ANN",
                        annProperties(), "", false),
                false);

        LanceIndexJob job = manager.getJob(outcome.getJobId());
        Assertions.assertEquals("Embedding", job.getColumnName());
        LanceIndexSchemaContract.IndexedField field = job.getSchemaContract().getFields().get(0);
        Assertions.assertEquals(4L, field.getFieldId());
        Assertions.assertEquals("embedding", field.getNormalizedName());
        Assertions.assertEquals("fixed_size_list", field.getNormalizedType());
    }

    @Test
    public void btreeCreatePersistsNullPropertiesAndScalarContract() throws Exception {
        LanceIndexAdmission.Outcome outcome = admitCreate(emptySnapshot(),
                scalarDef("Idx", "BTREE", "c", false, false), false);

        LanceIndexJob job = manager.getJob(outcome.getJobId());
        Assertions.assertEquals("BTREE", job.getIndexType());
        Assertions.assertEquals("c", job.getColumnName());
        Assertions.assertNull(job.getPropertiesJson());
        LanceIndexSchemaContract.IndexedField field = job.getSchemaContract().getFields().get(0);
        Assertions.assertEquals("c", field.getNormalizedName());
        Assertions.assertEquals("int<32>", field.getNormalizedType());
        Assertions.assertNull(field.getFixedSizeListDimension());
        Assertions.assertNull(field.getVectorElementType());
        Assertions.assertNull(field.getVectorElementNullable());
    }

    @Test
    public void annPropertiesJsonIsNormalizedAndDeterministic() throws Exception {
        Map<String, String> mixedCase = new HashMap<>();
        mixedCase.put("Index_Type", "ivf_pq");
        mixedCase.put("METRIC", "L2");
        mixedCase.put("num_partitions", "256");
        mixedCase.put("num_sub_vectors", "16");
        mixedCase.put("num_bits", "8");

        LanceIndexAdmission.Outcome outcome = admitCreate(emptySnapshot(),
                annDef("idx", false, false, mixedCase), false);

        LanceIndexJob job = manager.getJob(outcome.getJobId());
        // Keys are lowercase, index_type is the uppercased original value, metric is lowercased,
        // numerics stay as their original strings, num_bits is pinned to 8, and the key order is
        // the TreeMap order — byte-for-byte deterministic.
        Assertions.assertEquals(NORMALIZED_ANN_PROPERTIES_JSON, job.getPropertiesJson());
        Assertions.assertEquals("IVF_PQ", job.getIndexType());
    }

    @Test
    public void reservedSystemNameIsRejectedThreeWays() {
        // N6: CREATE, REPLACE and DROP all reject the __lance_ prefix at admission depth.
        assertInvalid(() -> admitCreate(emptySnapshot(), annDef("__lance_foo", false, false), false),
                "index name '__lance_foo' uses the reserved '__lance_' prefix of Lance system indexes");
        assertInvalid(() -> admitCreate(emptySnapshot(), annDef("__lance_foo", false, true), false),
                "index name '__lance_foo' uses the reserved '__lance_' prefix of Lance system indexes");
        assertInvalid(() -> admitDrop(emptySnapshot(), "__lance_foo", false),
                "index name '__lance_foo' uses the reserved '__lance_' prefix of Lance system indexes");
        // Normalization precedes the prefix check, so case variants are equally reserved.
        assertInvalid(() -> admitDrop(emptySnapshot(), "__LANCE_FOO", true),
                "index name '__LANCE_FOO' uses the reserved '__lance_' prefix of Lance system indexes");
        assertNothingPersisted();
    }

    @Test
    public void reservedSystemNameIsRejectedWithoutAnySnapshotRead() {
        // Fail cheap-first: the admission-depth reserved-prefix rejection runs before target
        // capture and the snapshot read, so a reserved name never costs a remote metadata read.
        LanceIndexAdmission.SnapshotLoader forbiddingLoader = (cat, dbName, tblName) -> {
            throw new AssertionError("snapshot read must not happen for reserved names");
        };

        assertInvalid(() -> LanceIndexAdmission.admitCreate(forbiddingLoader, catalog, database,
                table, annDef("__lance_foo", false, false), false),
                "index name '__lance_foo' uses the reserved '__lance_' prefix of Lance system indexes");
        assertInvalid(() -> LanceIndexAdmission.admitCreate(forbiddingLoader, catalog, database,
                table, annDef("__lance_foo", false, true), false),
                "index name '__lance_foo' uses the reserved '__lance_' prefix of Lance system indexes");
        assertInvalid(() -> LanceIndexAdmission.admitDrop(forbiddingLoader, catalog, database,
                table, "__lance_foo", false),
                "index name '__lance_foo' uses the reserved '__lance_' prefix of Lance system indexes");
        assertNothingPersisted();
    }

    @Test
    public void ambiguousCaseCollisionIsRejectedThreeWays() {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Arrays.asList(logicalIndex("IdxA", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON),
                        logicalIndex("idxa", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Arrays.asList(physicalIndex("IdxA", "VECTOR"), physicalIndex("idxa", "VECTOR")));
        String message = "index name 'IDXA' is ambiguous: multiple Lance indexes differ only by case";

        assertInvalid(() -> admitCreate(snapshot, annDef("IDXA", true, false), true), message);
        assertInvalid(() -> admitCreate(snapshot, annDef("IDXA", false, true), false), message);
        assertInvalid(() -> admitDrop(snapshot, "IDXA", true), message);
        assertNothingPersisted();
    }

    // ------------------------------------------------------------------
    // DROP admission
    // ------------------------------------------------------------------

    @Test
    public void dropAbsentNameIsRejected() {
        assertInvalid(() -> admitDrop(emptySnapshot(), "nope", false),
                "index 'nope' not found");
        assertNothingPersisted();
    }

    @Test
    public void dropIfExistsWithAbsentNameIsNoOp() throws Exception {
        LanceIndexAdmission.Outcome outcome = admitDrop(emptySnapshot(), "nope", true);

        Assertions.assertNull(outcome.getJobId());
        assertNothingPersisted();
    }

    @Test
    public void dropExistingNameIsAdmittedWithDropShape() throws Exception {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("IdxA", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("IdxA", "VECTOR")));

        LanceIndexAdmission.Outcome outcome = admitDrop(snapshot, "IdxA", true);

        LanceIndexJob job = manager.getJob(outcome.getJobId());
        Assertions.assertEquals(LanceIndexJobMutationType.DROP, job.getMutationType());
        Assertions.assertEquals("IdxA", job.getDisplayIndexName());
        Assertions.assertEquals("idxa", job.getNormalizedIndexName());
        Assertions.assertTrue(job.isIfExists());
        Assertions.assertFalse(job.isIfNotExists());
        Assertions.assertNull(job.getIndexType());
        Assertions.assertNull(job.getColumnName());
        Assertions.assertNull(job.getPropertiesJson());
        Assertions.assertNull(job.getSchemaContract());
        Assertions.assertEquals(DATASET_VERSION, job.getAdmittedDatasetVersion());
        Assertions.assertEquals(DATASET_URI, job.getNormalizedLocator());
    }

    @Test
    public void dropWithCaseVariantPersistsTheStoredName() throws Exception {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("IdxA", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("IdxA", "VECTOR")));

        LanceIndexAdmission.Outcome outcome = admitDrop(snapshot, "idxa", false);

        LanceIndexJob job = manager.getJob(outcome.getJobId());
        Assertions.assertEquals("IdxA", job.getDisplayIndexName());
        Assertions.assertEquals("idxa", job.getNormalizedIndexName());
    }

    // ------------------------------------------------------------------
    // Fence, quota, and id discipline
    // ------------------------------------------------------------------

    @Test
    public void fenceConflictPassesThroughTheManagerRejection() throws Exception {
        manager.createJob(newCreateJob(1L, "idxa"), 100, 100, 100);

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> admitCreate(emptySnapshot(), annDef("idxA", false, false), false));
        Assertions.assertTrue(exception.getMessage().contains("fenced by unresolved job 1"),
                exception.getMessage());
        // The rejection never discloses the locator.
        Assertions.assertFalse(exception.getMessage().contains("bucket"), exception.getMessage());
        Assertions.assertFalse(exception.getMessage().contains(DATASET_URI), exception.getMessage());

        // One id was burned before the durable transfer rejected the job (accepted, ids are not
        // required to be contiguous); nothing else changed.
        Assertions.assertEquals(1, manager.getJobCount());
        Assertions.assertEquals(1, manager.editLog.size());
        Assertions.assertEquals(1, manager.getUnresolvedJobs().size());
        Mockito.verify(env, Mockito.times(1)).getNextId();
    }

    @Test
    public void quotaOverloadPassesThroughTheManagerRejection() throws Exception {
        Config.lance_index_job_max_unresolved_per_table = 1;
        manager.createJob(newCreateJob(1L, "idxz"), 100, 100, 100);

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> admitCreate(emptySnapshot(), annDef("idxA", false, false), false));
        Assertions.assertTrue(exception.getMessage().contains("quota exceeded"), exception.getMessage());

        Assertions.assertEquals(1, manager.getJobCount());
        Assertions.assertEquals(1, manager.editLog.size());
        Assertions.assertEquals(1, manager.getUnresolvedJobs().size());
        Mockito.verify(env, Mockito.times(1)).getNextId();
    }

    @Test
    public void quotaBoundaryAdmitsAtTheLimitAndRejectsBeyondIt() throws Exception {
        Config.lance_index_job_max_unresolved_per_table = 2;
        manager.createJob(newCreateJob(1L, "idxz"), 100, 100, 100);

        // count + 1 == limit passes.
        LanceIndexAdmission.Outcome outcome = admitCreate(emptySnapshot(),
                annDef("idxA", false, false), false);
        Assertions.assertNotNull(outcome.getJobId());
        Assertions.assertEquals(2, manager.getJobCount());

        // count + 1 > limit is rejected.
        Assertions.assertThrows(DdlException.class,
                () -> admitCreate(emptySnapshot(), annDef("idxB", false, false), false));
        Assertions.assertEquals(2, manager.getJobCount());
        Assertions.assertEquals(2, unresolvedJobsOnDataset());
    }

    @Test
    public void nonPositiveQuotaConfigIsAssertedBeforeAnyIdAllocation() {
        // D7: a quota that reached fe.conf with a non-positive value (bypassing the ADMIN SET
        // callback) must fail admission closed with 5102 naming the config item.
        Config.lance_index_job_max_unresolved_per_table = 0;
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> admitCreate(emptySnapshot(), annDef("idxA", false, false), false));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_DISABLED, exception.getMysqlErrorCode());
        Assertions.assertTrue(exception.getDetailMessage().contains("lance_index_job_max_unresolved_per_table"),
                exception.getDetailMessage());
        assertNothingPersisted();

        Config.lance_index_job_max_unresolved_per_table = originalTableQuota;
        Config.lance_index_job_max_unresolved_global = -1;
        exception = Assertions.assertThrows(AnalysisException.class,
                () -> admitDrop(snapshot(
                        Collections.singletonList(
                                logicalIndex("idxA", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                        Collections.singletonList(physicalIndex("idxA", "VECTOR"))),
                        "idxA", false));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_DISABLED, exception.getMysqlErrorCode());
        Assertions.assertTrue(exception.getDetailMessage().contains("lance_index_job_max_unresolved_global"),
                exception.getDetailMessage());
        assertNothingPersisted();
    }

    @Test
    public void identityChangeDuringSnapshotRejectsCreateAndDropBeforeAllocatingId() throws Exception {
        LanceIndexAdmission.SnapshotLoader changingLoader = (cat, dbName, tblName) -> {
            // Emulate an identity ALTER completing during the remote metadata read.
            cat.getProperties().put("warehouse", cat.getProperties().getOrDefault("warehouse", "") + "changed");
            return snapshot(Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                    MATCHING_ANN_PROPERTIES_JSON)), Collections.singletonList(physicalIndex("idx", "VECTOR")));
        };
        Assertions.assertThrows(DdlException.class,
                () -> LanceIndexAdmission.admitCreate(changingLoader, catalog, database, table,
                        annDef("fresh", false, false), false));
        Assertions.assertThrows(DdlException.class,
                () -> LanceIndexAdmission.admitDrop(changingLoader, catalog, database, table, "idx", false));
        Assertions.assertThrows(DdlException.class,
                () -> LanceIndexAdmission.admitCreate(changingLoader, catalog, database, table,
                        annDef("idx", true, false), true));
        Assertions.assertThrows(DdlException.class,
                () -> LanceIndexAdmission.admitDrop(changingLoader, catalog, database, table, "absent", true));
        assertNothingPersisted();
    }

    @Test
    public void snapshotLoadFailurePropagatesWithoutPersistingAnything() {
        LanceIndexAdmission.SnapshotLoader failingLoader = (cat, dbName, tblName) -> {
            throw new AnalysisException("dataset unreadable");
        };
        Assertions.assertThrows(AnalysisException.class,
                () -> LanceIndexAdmission.admitCreate(failingLoader, catalog, database, table,
                        annDef("idx", false, false), false));
        Assertions.assertThrows(AnalysisException.class,
                () -> LanceIndexAdmission.admitDrop(failingLoader, catalog, database, table, "idx", false));
        assertNothingPersisted();
    }

    @Test
    public void mixedPopulationAdmissionsChargeFenceAndQuotaCorrectly() throws Exception {
        LanceIndexAdmissionSnapshot withExisting = snapshot(
                Collections.singletonList(logicalIndex("IdxC", "c", "BTREE", "{}")),
                Collections.singletonList(physicalIndex("IdxC", "SCALAR")));

        LanceIndexAdmission.Outcome createA = admitCreate(emptySnapshot(),
                annDef("idxA", false, false), false);
        LanceIndexAdmission.Outcome createB = admitCreate(emptySnapshot(),
                scalarDef("idxB", "BITMAP", "s", false, false), false);
        LanceIndexAdmission.Outcome dropC = admitDrop(withExisting, "idxc", false);

        Assertions.assertEquals(3, manager.getJobCount());
        Assertions.assertEquals(3, manager.editLog.size());
        Assertions.assertEquals(3, manager.getUnresolvedJobs().size());
        Assertions.assertEquals(3, unresolvedJobsOnDataset());
        Assertions.assertTrue(manager.isFenceHeld(manager.getJob(createA.getJobId()).fenceKey()));
        Assertions.assertTrue(manager.isFenceHeld(manager.getJob(createB.getJobId()).fenceKey()));
        Assertions.assertTrue(manager.isFenceHeld(manager.getJob(dropC.getJobId()).fenceKey()));
        Assertions.assertEquals("IdxC", manager.getJob(dropC.getJobId()).getDisplayIndexName());
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName) {
        return new LanceIndexJob(jobId, "seeder", CATALOG_ID, DB, TBL,
                LanceIndexFenceKey.PROVIDER_DIRECTORY, DATASET_URI,
                displayName, displayName.toLowerCase(Locale.ROOT),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, DATASET_VERSION, null);
    }
}
