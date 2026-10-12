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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.lance.LanceIndexAdmissionSnapshot.PhysicalIndexInfo;
import org.apache.doris.datasource.lance.index.LanceShowIndexInfo;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.qe.ConnectContext;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.lance.schema.LanceField;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Section 2.2/4.1 admission coverage for {@link LanceIndexAdmission}: snapshots are injected
 * through the {@code SnapshotLoader} seam (no FE, no JNI) and {@code Env} is mocked only for
 * the catalog manager. Every rejection path must leave no durable state and no id allocation
 * behind. A mutation that passes the whole preflight terminates with the shared
 * not-supported rejection (the synchronous execution path has not landed), while an IF
 * preflight no-op returns normally.
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
    /**
     * The KELVIN SIGN (U+212A) and the A-with-diaeresis pair: {@code String.equalsIgnoreCase}
     * folds both pairs case-insensitively (the Kelvin sign through its per-character
     * {@code Character.toLowerCase} fallback), so they probe the exact lookup relation.
     */
    private static final String KELVIN_SIGN = new String(Character.toChars(0x212A));
    private static final String CAPITAL_A_WITH_DIAERESIS = new String(Character.toChars(0xC4));
    private static final String SMALL_A_WITH_DIAERESIS = new String(Character.toChars(0xE4));

    private MockedStatic<Env> mockedEnv;
    private Env env;
    private LanceExternalCatalog catalog;
    private LanceExternalDatabase database;
    private LanceExternalTable table;
    private ConnectContext connectContext;
    private final Map<String, Column> tableColumns = new HashMap<>();

    @BeforeEach
    public void setUp() throws Exception {
        mockedEnv = Mockito.mockStatic(Env.class);
        env = Mockito.mock(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(CATALOG_ID);
        Mockito.when(catalog.getProperties()).thenReturn(new HashMap<>());
        CatalogMgr catalogMgr = new CatalogMgr();
        java.lang.reflect.Field catalogs = CatalogMgr.class.getDeclaredField("idToCatalog");
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
        tableColumns.put("v", notNullColumn("v", new ArrayType(Type.FLOAT)));
        tableColumns.put("c", notNullColumn("c", Type.INT));
        tableColumns.put("s", notNullColumn("s", Type.STRING));
        tableColumns.put("Embedding", notNullColumn("Embedding", new ArrayType(Type.FLOAT)));
        // Case-insensitive lookup, mirroring ExternalTable.getColumn.
        Mockito.when(table.getColumn(Mockito.anyString())).thenAnswer(invocation -> {
            String name = invocation.getArgument(0);
            for (Map.Entry<String, Column> entry : tableColumns.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(name)) {
                    return entry.getValue();
                }
            }
            return null;
        });

        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("tester", "%"));
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        mockedEnv.close();
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static Column notNullColumn(String name, Type type) {
        return new Column(name, type, false, null, false, null, "");
    }

    private static LanceField vectorField(String name, int id) {
        // Mirrors the pinned SDK: the LanceField tree carries no children for a fixed-size
        // list; the element lives only in the synthesized child of the reconstructed Arrow
        // view, always nullable.
        Field synthesizedElement = Field.nullable("item",
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        Field arrowView = new Field(name, FieldType.notNullable(new ArrowType.FixedSizeList(4)),
                Collections.singletonList(synthesizedElement));
        LanceField field = Mockito.mock(LanceField.class);
        Mockito.when(field.getId()).thenReturn(id);
        Mockito.when(field.getName()).thenReturn(name);
        Mockito.when(field.getType()).thenReturn(new ArrowType.FixedSizeList(4));
        Mockito.when(field.isNullable()).thenReturn(false);
        Mockito.when(field.getChildren()).thenReturn(Collections.emptyList());
        Mockito.when(field.asArrowField()).thenReturn(arrowView);
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

    private static LanceIndexAdmissionSnapshot snapshot(List<LanceShowIndexInfo> logical,
            List<PhysicalIndexInfo> physical) {
        return new LanceIndexAdmissionSnapshot(DATASET_VERSION, DATASET_URI, logical, physical,
                defaultFields());
    }

    private static LanceIndexAdmissionSnapshot emptySnapshot() {
        return snapshot(Collections.emptyList(), Collections.emptyList());
    }

    private static LanceIndexAdmissionSnapshot snapshotWithFields(LanceField... fields) {
        return new LanceIndexAdmissionSnapshot(DATASET_VERSION, DATASET_URI,
                Collections.emptyList(), Collections.emptyList(), Arrays.asList(fields));
    }

    private static LanceShowIndexInfo logicalIndex(String name, String column, String indexType,
            String propertiesJson) {
        return new LanceShowIndexInfo(name, Collections.singletonList(column), indexType, propertiesJson);
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

    private static IndexDefinition annDef(String name, String column, boolean ifNotExists,
            boolean orReplace) {
        return new IndexDefinition(name, ifNotExists, Collections.singletonList(column), "ANN",
                annProperties(), "", orReplace);
    }

    private static IndexDefinition scalarDef(String name, String lanceType, String column,
            boolean ifNotExists, boolean orReplace) {
        return new IndexDefinition(name, ifNotExists, Collections.singletonList(column), lanceType,
                new HashMap<>(), "", orReplace);
    }

    private void admitCreate(LanceIndexAdmissionSnapshot snapshot,
            IndexDefinition def, boolean ifNotExists) throws Exception {
        LanceIndexAdmission.admitCreate((cat, dbName, tblName) -> snapshot, catalog, database,
                table, def, ifNotExists);
    }

    private void admitDrop(LanceIndexAdmissionSnapshot snapshot,
            String indexName, boolean ifExists) throws Exception {
        LanceIndexAdmission.admitDrop((cat, dbName, tblName) -> snapshot, catalog, database,
                table, indexName, ifExists);
    }

    private void assertNothingPersisted() {
        Mockito.verify(env, Mockito.never()).getNextId();
    }

    private static void assertInvalid(Executable call, String expectedMessage) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, call);
        Assertions.assertEquals(expectedMessage, exception.getDetailMessage());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_INVALID, exception.getMysqlErrorCode());
    }

    /** A mutation whose whole preflight passed terminates with the shared not-supported error. */
    private static void assertUnsupported(Executable call, String operation) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, call);
        Assertions.assertEquals(operation + " is not supported for Lance catalog tables",
                exception.getDetailMessage());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_OPERATION_NOT_SUPPORTED,
                exception.getMysqlErrorCode());
    }

    // ------------------------------------------------------------------
    // CREATE admission
    // ------------------------------------------------------------------

    @Test
    public void plainCreateOnEmptySnapshotValidatesThenRejectsAsUnsupported() {
        assertUnsupported(() -> admitCreate(emptySnapshot(), annDef("MyIdx", false, false), false),
                "CREATE INDEX");
        assertNothingPersisted();
    }

    @Test
    public void defaultLoaderDelegatesToTheCatalogWithRemoteNames() throws Exception {
        // Materialize the snapshot before stubbing: building it mocks LanceField, and Mockito
        // rejects nested stubbing inside a when(...) call.
        LanceIndexAdmissionSnapshot prepared = emptySnapshot();
        Mockito.when(catalog.loadTableIndexAdmissionSnapshot(REMOTE_DB, REMOTE_TBL))
                .thenReturn(prepared);

        assertUnsupported(() -> LanceIndexAdmission.admitCreate(catalog, database, table,
                scalarDef("Idx", "BTREE", "c", false, false), false), "CREATE INDEX");
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

        Assertions.assertDoesNotThrow(() -> admitCreate(snapshot, annDef("idxa", true, false), true));
        assertNothingPersisted();
    }

    @Test
    public void createIfNotExistsWithMatchingScalarDefinitionIsNoOp() throws Exception {
        // BTREE carries no user build properties, so the whitelist comparison is vacuous: the
        // same name, algorithm and single column make the statement an immediate no-op. The
        // stored payload is deliberately a non-object: only the scalar short-circuit can
        // produce the no-op — any ANN-style property comparison would fail closed on it.
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "c", "BTREE", "[]")),
                Collections.singletonList(physicalIndex("idx", "SCALAR")));

        Assertions.assertDoesNotThrow(() -> admitCreate(snapshot,
                scalarDef("idx", "BTREE", "c", true, false), true));
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
    public void createIfNotExistsMatchesPathSegmentEscapedColumns() throws Exception {
        // The logical column is a loader path segment: field names containing characters outside
        // [A-Za-z0-9_] are backtick-escaped (embedded backticks doubled). The raw parser spelling
        // of the same column must be formatted with the same rule before the comparison, or the
        // preflight falsely rejects these columns as a different definition.
        String[][] pathSegments = {{"a.b", "`a.b`"}, {"c d", "`c d`"}, {"e`f", "`e``f`"}};
        for (String[] pathSegment : pathSegments) {
            String fieldName = pathSegment[0];
            LanceIndexAdmissionSnapshot snapshot = new LanceIndexAdmissionSnapshot(DATASET_VERSION,
                    DATASET_URI,
                    Collections.singletonList(logicalIndex("idx", pathSegment[1], "IVF_PQ",
                            MATCHING_ANN_PROPERTIES_JSON)),
                    Collections.singletonList(physicalIndex("idx", "VECTOR")),
                    Collections.singletonList(vectorField(fieldName, 1)));
            Assertions.assertDoesNotThrow(() -> admitCreate(snapshot,
                    annDef("idx", fieldName, true, false), true), fieldName);
        }
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
    public void sameNameMultiColumnStoredIndexIsRejected() {
        // Lance scalar indexes can span several columns; a same-name single-column request is a
        // definition mismatch even when the algorithm and the first column agree, never a no-op.
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(new LanceShowIndexInfo("idx", Arrays.asList("c", "s"),
                        "BTREE", "{}")),
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

        Assertions.assertDoesNotThrow(() -> admitCreate(snapshot, annDef("idx", true, false), true));
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
        Assertions.assertDoesNotThrow(() -> admitCreate(partialExposure,
                annDef("idx", true, false), true));

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
    public void omittedNumBitsComparesAsThePersistedEight() throws Exception {
        // The validator accepts an omitted num_bits and the persisted value is always 8, so the
        // preflight compares an effective 8 — never skips the property — against any exposed
        // compression.num_bits.
        LanceIndexAdmissionSnapshot fourBits = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"compression\":{\"num_bits\":4}}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));
        assertInvalid(() -> admitCreate(fourBits, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");

        LanceIndexAdmissionSnapshot eightBits = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"compression\":{\"num_bits\":8}}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));
        Assertions.assertDoesNotThrow(() -> admitCreate(eightBits, annDef("idx", true, false), true));
        assertNothingPersisted();
    }

    @Test
    public void exposedIntegerNumericPropertiesMatch() throws Exception {
        for (String exposedValue : new String[] {"16", "\"16\"", "\"016\"", "\"+16\""}) {
            LanceIndexAdmissionSnapshot snapshot = snapshot(
                    Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                            "{\"compression\":{\"num_sub_vectors\":" + exposedValue + "}}")),
                    Collections.singletonList(physicalIndex("idx", "VECTOR")));
            Assertions.assertDoesNotThrow(() -> admitCreate(snapshot,
                    annDef("idx", true, false), true), exposedValue);
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
    public void unparsableSnapshotPropertiesFailClosed() {
        // A payload that does not parse at all is malformed provider data, not "nothing exposed".
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ", "{not-json")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));

        assertInvalid(() -> admitCreate(snapshot, annDef("idx", true, false), true),
                "index 'idx' already exists with a different definition");
        assertNothingPersisted();
    }

    @Test
    public void exposedMetricComparisonIsCaseInsensitive() throws Exception {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("idx", "v", "IVF_PQ",
                        "{\"metric_type\":\"L2\"}")),
                Collections.singletonList(physicalIndex("idx", "VECTOR")));

        Assertions.assertDoesNotThrow(() -> admitCreate(snapshot, annDef("idx", true, false), true));
        assertNothingPersisted();
    }

    @Test
    public void replaceOnAbsentNameValidatesThenRejectsAsUnsupported() {
        assertUnsupported(() -> admitCreate(emptySnapshot(), annDef("Fresh", false, true), false),
                "CREATE OR REPLACE INDEX");
        assertNothingPersisted();
    }

    @Test
    public void replaceWithCaseVariantResolvesThenRejectsAsUnsupported() {
        // M2: a unique case-insensitive match resolves to the stored display name before the
        // terminal rejection.
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("IdxA", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("IdxA", "VECTOR")));

        assertUnsupported(() -> admitCreate(snapshot, annDef("IDXA", false, true), false),
                "CREATE OR REPLACE INDEX");
        assertNothingPersisted();
    }

    @Test
    public void mixedCaseColumnResolvesUniquelyThroughThePreflight() {
        // N5: static validation is case-insensitive; the schema-contract build keys on the
        // stored Lance field name, so a mixed-case request resolves, passes the preflight,
        // and ends in the shared not-supported rejection.
        assertUnsupported(() -> admitCreate(emptySnapshot(),
                new IndexDefinition("idx", false, Collections.singletonList("embedding"), "ANN",
                        annProperties(), "", false), false), "CREATE INDEX");
    }

    @Test
    public void ambiguousColumnCaseCollisionIsRejectedEveryWay() {
        // P1 fail-closed: a dataset can hold top-level fields that differ only by case, and
        // ExternalTable.getColumn returns the first equalsIgnoreCase hit, so admitting would
        // journal an arbitrary one of them. CREATE, IF NOT EXISTS and REPLACE all fail closed.
        LanceIndexAdmissionSnapshot snapshot = snapshotWithFields(
                vectorField("V", 1), vectorField("v", 2));
        String message = "index column 'v' is ambiguous: multiple Lance fields differ only by case";

        assertInvalid(() -> admitCreate(snapshot, annDef("idx", "v", false, false), false), message);
        assertInvalid(() -> admitCreate(snapshot, annDef("idx", "v", true, false), true), message);
        assertInvalid(() -> admitCreate(snapshot, annDef("idx", "v", false, true), false), message);
        // The opposite user spelling hits the same collision.
        assertInvalid(() -> admitCreate(snapshot, annDef("idx", "V", false, false), false),
                "index column 'V' is ambiguous: multiple Lance fields differ only by case");
        assertNothingPersisted();
    }

    @Test
    public void columnAmbiguityAppliesTheExactTableLookupRelation() throws Exception {
        // The collision check applies the exact relation of ExternalTable.getColumn —
        // String.equalsIgnoreCase — never a ROOT-lowercase fold. On this JDK that per-character
        // relation folds 'Ä' onto 'ä' and, through its Character.toLowerCase fallback, the KELVIN
        // SIGN (U+212A) onto ASCII 'k', so a dataset holding such a pair is genuinely
        // unresolvable by the lookup and fails closed; a lone KELVIN SIGN field still resolves
        // a 'k' request uniquely and passes the preflight without a false ambiguity.
        LanceIndexAdmissionSnapshot kelvinPair = snapshotWithFields(
                vectorField(KELVIN_SIGN, 1), vectorField("k", 2));
        assertInvalid(() -> admitCreate(kelvinPair, annDef("idx", "k", false, false), false),
                "index column 'k' is ambiguous: multiple Lance fields differ only by case");

        LanceIndexAdmissionSnapshot diaeresisPair = snapshotWithFields(
                vectorField(CAPITAL_A_WITH_DIAERESIS, 1),
                vectorField(SMALL_A_WITH_DIAERESIS, 2));
        assertInvalid(() -> admitCreate(diaeresisPair,
                annDef("idx", SMALL_A_WITH_DIAERESIS, false, false), false),
                "index column '" + SMALL_A_WITH_DIAERESIS + "' is ambiguous: "
                        + "multiple Lance fields differ only by case");
        assertNothingPersisted();

        // Unique resolution: exactly one lookup hit, so the request passes the preflight and
        // ends in the shared not-supported rejection instead of the ambiguity error.
        tableColumns.put(KELVIN_SIGN, notNullColumn(KELVIN_SIGN, new ArrayType(Type.FLOAT)));
        assertUnsupported(() -> admitCreate(snapshotWithFields(
                vectorField(KELVIN_SIGN, 1)), annDef("idx", "k", false, false), false),
                "CREATE INDEX");
    }

    @Test
    public void btreeCreateValidatesTheScalarContractThenRejectsAsUnsupported() {
        assertUnsupported(() -> admitCreate(emptySnapshot(),
                scalarDef("Idx", "BTREE", "c", false, false), false), "CREATE INDEX");
        assertNothingPersisted();
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
        Assertions.assertDoesNotThrow(() -> admitDrop(emptySnapshot(), "nope", true));
        assertNothingPersisted();
    }

    @Test
    public void dropExistingNameValidatesThenRejectsAsUnsupported() {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("IdxA", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("IdxA", "VECTOR")));

        assertUnsupported(() -> admitDrop(snapshot, "IdxA", true), "DROP INDEX");
        assertNothingPersisted();
    }

    @Test
    public void dropWithCaseVariantResolvesThenRejectsAsUnsupported() {
        LanceIndexAdmissionSnapshot snapshot = snapshot(
                Collections.singletonList(logicalIndex("IdxA", "v", "IVF_PQ", MATCHING_ANN_PROPERTIES_JSON)),
                Collections.singletonList(physicalIndex("IdxA", "VECTOR")));

        assertUnsupported(() -> admitDrop(snapshot, "idxa", false), "DROP INDEX");
        assertNothingPersisted();
    }

    // ------------------------------------------------------------------
    // Target and snapshot discipline
    // ------------------------------------------------------------------

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
}
