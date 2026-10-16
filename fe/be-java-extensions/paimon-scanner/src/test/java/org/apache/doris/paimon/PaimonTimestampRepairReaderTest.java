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

package org.apache.doris.paimon;

import org.apache.doris.jni.spi.utils.OffHeap;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonTimestampRepairReaderTest {
    @TempDir
    Path warehouse;

    @Test
    public void legacyProducerRetainsSafePredicateAndHistoricalTimestamp() throws Exception {
        checkReader("legacy", new Object[] {
                LocalDateTime.of(1969, 12, 31, 23, 59, 59), LocalDateTime.of(2025, 1, 1, 0, 0, 1)});
    }

    @Test
    public void hiddenIntegerPolicyColumnDoesNotResetTimestampReadType() throws Exception {
        checkReader("integer_policy", new Object[] {
                LocalDateTime.of(1969, 12, 31, 23, 59, 59), LocalDateTime.of(2025, 1, 1, 0, 0, 1)});
    }

    @Test
    public void timestampPolicyRunsAfterRepairAndRejectsNonmatchingRows() throws Exception {
        checkReader("timestamp_policy", new Object[] {LocalDateTime.of(2025, 1, 1, 0, 0, 1),
                LocalDateTime.of(2025, 1, 1, 0, 0, 1)});
    }

    @Test
    public void auditLogHistoricalProjectionRetainsNegativeEpoch() throws Exception {
        checkReader("audit_log", new Object[] {LocalDateTime.of(1969, 12, 31, 23, 59, 59),
                LocalDateTime.of(2025, 1, 1, 0, 0, 1), LocalDateTime.of(2025, 1, 1, 0, 0, 1)});
    }

    @Test
    public void auditLogSystemColumnAndSerializedIntegerPolicyRetainPrecision() throws Exception {
        checkReader("audit_log_policy", new Object[] {LocalDateTime.of(1969, 12, 31, 23, 59, 59),
                LocalDateTime.of(2025, 1, 1, 0, 0, 1)});
    }

    @Test
    public void fallbackTimestampPolicyRunsAfterPrecisionRepair() throws Exception {
        checkReader("fallback_policy", new Object[] {LocalDateTime.of(2025, 1, 1, 0, 0, 1),
                LocalDateTime.of(2025, 1, 1, 0, 0, 1)});
    }

    @Test
    public void fallbackDataSplitWithoutPolicyRemainsAReaderLeaf() throws Exception {
        checkReader("fallback_plain", new Object[] {LocalDateTime.of(1969, 12, 31, 23, 59, 59),
                LocalDateTime.of(2025, 1, 1, 0, 0, 1), LocalDateTime.of(2025, 1, 1, 0, 0, 1)});
    }

    @Test
    public void nanosecondOutputsRetainLogicalPrecisionWithDatetimeSibling() throws Exception {
        for (int precision : new int[] {7, 8}) {
            checkNanosecondReader(precision, false);
            checkNanosecondReader(precision, true);
        }
    }

    private void checkNanosecondReader(int precision, boolean withSibling) throws Exception {
        OffHeap.setTesting();
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", true);
            Identifier id = Identifier.create("db", "nano_" + precision + "_" + withSibling);
            catalog.createTable(id, Schema.newBuilder().column("ts", DataTypes.TIMESTAMP(9))
                    .column("nested", RowType.builder().field("ts", DataTypes.TIMESTAMP(9)).build())
                    .column("plain_ts", DataTypes.TIMESTAMP(6)).option("file.format", "parquet").build(), false);
            Table table = catalog.getTable(id);
            LocalDateTime value = LocalDateTime.of(2025, 1, 1, 0, 0, 1, 123456789);
            Timestamp timestamp = Timestamp.fromLocalDateTime(value);
            BatchWriteBuilder writer = table.newBatchWriteBuilder();
            try (BatchTableWrite write = writer.newWrite()) {
                write.write(GenericRow.of(timestamp, GenericRow.of(timestamp),
                        Timestamp.fromLocalDateTime(value.withNano(123456000))));
                try (BatchTableCommit commit = writer.newCommit()) {
                    commit.commit(write.prepareCommit());
                }
            }
            catalog.alterTable(id, Arrays.asList(
                    SchemaChange.updateColumnType("ts", DataTypes.TIMESTAMP(precision), true),
                    SchemaChange.updateColumnType(new String[] {"nested", "ts"},
                            DataTypes.TIMESTAMP(precision), true)), false);
            table = catalog.getTable(id);
            Map<String, String> params = new HashMap<>();
            params.put("required_fields", withSibling ? "ts,nested,plain_ts" : "ts");
            params.put("columns_types", withSibling
                    ? "timestamp_ns#struct<ts:timestamp_ns>#datetimev2(6)" : "timestamp_ns");
            params.put("time_zone", "UTC");
            params.put("serialized_table_cache_key", "nano-reader-" + id + warehouse);
            params.put("serialized_table", encode(table));
            params.put("paimon_split", encode(table.newReadBuilder().newScan().plan().splits().get(0)));
            params.put("paimon_predicate", encode(Collections.emptyList()));
            params.put("paimon." + PaimonJniScanner.DORIS_TIMESTAMP_PRECISION_REPAIR, "true");
            PaimonJniScanner scanner = new PaimonJniScanner(16, params);
            try {
                scanner.open();
                Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                Assertions.assertEquals(1, scanner.getTable().getNumRows());
                LocalDateTime expected = value.withNano(precision == 7 ? 123456700 : 123456780);
                Assertions.assertEquals(expected, scanner.getTable().getColumn(0).getObjectColumn(0, 1)[0]);
                if (withSibling) {
                    Assertions.assertEquals(Collections.singletonMap("ts", expected),
                            scanner.getTable().getColumn(1).getObjectColumn(0, 1)[0]);
                    Assertions.assertEquals(value.withNano(123456000),
                            scanner.getTable().getColumn(2).getObjectColumn(0, 1)[0]);
                }
            } finally {
                scanner.close();
                PaimonTableCache.clearForTest();
            }
        }
    }

    @Test
    public void integerPolicyAndMaskRemainEnforcedAfterRepair() throws Exception {
        checkReader("masking_policy", new Object[] {LocalDateTime.of(2020, 1, 1, 0, 0),
                LocalDateTime.of(2020, 1, 1, 0, 0)});
    }

    @Test
    public void hiddenTimestampPolicyActivatesRepairForIntegerProjection() throws Exception {
        checkReader("hidden_timestamp_policy", new Object[] {7});
    }

    @Test
    public void nestedPolicyRetainsUnprojectedSiblingAndRestoresOutputShape() throws Exception {
        checkReader("nested_policy", new Object[] {Collections.singletonMap("visible", 7)});
    }

    @Test
    public void policyMaterializationRepairsNestedValuesWithoutChangingMapKeys() {
        RowType current = RowType.builder().field("array", DataTypes.ARRAY(DataTypes.TIMESTAMP(0)))
                .field("map", DataTypes.MAP(DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP(0)))
                .field("row", RowType.builder().field("ts", DataTypes.TIMESTAMP(0)).build()).build();
        RowType widened = PaimonJniScanner.createSafeTimestampReadType(current);
        Timestamp negative = Timestamp.fromLocalDateTime(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 600000000));
        Timestamp key = Timestamp.fromLocalDateTime(LocalDateTime.of(2025, 1, 1, 0, 0, 1, 1000));
        InternalRow repaired = PaimonTimestampQueryAuth.repairRow(GenericRow.of(
                new org.apache.paimon.data.GenericArray(new Object[] {negative, null}),
                new org.apache.paimon.data.GenericMap(Collections.singletonMap(key, negative)),
                GenericRow.of(key)), widened, current);
        Timestamp expectedNegative = Timestamp.fromLocalDateTime(LocalDateTime.of(1969, 12, 31, 23, 59, 59));
        Assertions.assertEquals(expectedNegative, repaired.getArray(0).getTimestamp(0, 0));
        Assertions.assertTrue(repaired.getArray(0).isNullAt(1));
        Assertions.assertSame(key, repaired.getMap(1).keyArray().getTimestamp(0, 6));
        Assertions.assertEquals(expectedNegative, repaired.getMap(1).valueArray().getTimestamp(0, 0));
        Assertions.assertEquals(Timestamp.fromLocalDateTime(LocalDateTime.of(2025, 1, 1, 0, 0, 1)),
                repaired.getRow(2, 1).getTimestamp(0, 0));
    }

    private void checkReader(String mode, Object[] expected) throws Exception {
        OffHeap.setTesting();
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "events");
            boolean nested = mode.equals("nested_policy");
            Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT())
                    .column("ts", DataTypes.TIMESTAMP(6)).option("file.format", "parquet");
            if (nested) {
                schema.column("payload", RowType.builder().field("visible", DataTypes.INT())
                        .field("hidden", DataTypes.TIMESTAMP(6)).build());
            }
            catalog.createTable(id, schema.build(), false);
            Table table = catalog.getTable(id);
            BatchWriteBuilder writer = table.newBatchWriteBuilder();
            Timestamp negative = Timestamp.fromLocalDateTime(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 600000000));
            Timestamp positive = Timestamp.fromLocalDateTime(LocalDateTime.of(2025, 1, 1, 0, 0, 1, 600000000));
            try (BatchTableWrite write = writer.newWrite()) {
                write.write(nested ? GenericRow.of(7, negative, GenericRow.of(7, negative))
                        : GenericRow.of(7, negative));
                write.write(nested ? GenericRow.of(7, positive, GenericRow.of(7, positive))
                        : GenericRow.of(7, positive));
                write.write(nested ? GenericRow.of(8, positive, GenericRow.of(8, positive))
                        : GenericRow.of(8, positive));
                try (BatchTableCommit commit = writer.newCommit()) {
                    commit.commit(write.prepareCommit());
                }
            }
            List<SchemaChange> changes = new java.util.ArrayList<>();
            changes.add(SchemaChange.updateColumnType("ts", DataTypes.TIMESTAMP(0), true));
            if (nested) {
                changes.add(SchemaChange.updateColumnType(new String[] {"payload", "hidden"},
                        DataTypes.TIMESTAMP(0), true));
            }
            catalog.alterTable(id, changes, false);
            table = catalog.getTable(id);
            if (mode.startsWith("audit_log")) {
                table = org.apache.paimon.table.system.SystemTableLoader.load("audit_log",
                        (org.apache.paimon.table.FileStoreTable) table);
            }
            Split split = table.newReadBuilder().newScan().plan().splits().get(0);
            PredicateBuilder builder = new PredicateBuilder(table.rowType());
            Predicate integer = builder.equal(builder.indexOf("id"), 7);
            Predicate timestamp = builder.equal(builder.indexOf("ts"),
                    Timestamp.fromLocalDateTime(mode.equals("hidden_timestamp_policy")
                            ? LocalDateTime.of(1969, 12, 31, 23, 59, 59) : LocalDateTime.of(2025, 1, 1, 0, 0, 1)));
            Map<String, String> params = new HashMap<>();
            params.put("required_fields", "ts");
            params.put("columns_types", "datetimev2(0)");
            params.put("time_zone", "UTC");
            params.put("serialized_table_cache_key", "repair-reader-" + warehouse);
            params.put("serialized_table", encode(table));
            if (mode.equals("audit_log_policy")) {
                params.put("required_fields", "rowkind,ts");
                params.put("columns_types", "string#datetimev2(0)");
            } else if (mode.equals("hidden_timestamp_policy")) {
                params.put("required_fields", "id");
                params.put("columns_types", "int");
            } else if (nested) {
                params.put("required_fields", "payload");
                params.put("columns_types", "struct<visible:int>");
            }
            if (mode.equals("legacy")) {
                params.put("required_fields", "id,ts");
                params.put("columns_types", "int#datetimev2(0)");
                params.put("paimon_predicate", encode(
                        Collections.singletonList(PredicateBuilder.and(integer, timestamp))));
            } else if (!mode.equals("audit_log") && !mode.equals("fallback_plain")) {
                Predicate policy = mode.equals("timestamp_policy") || mode.equals("hidden_timestamp_policy")
                        || mode.equals("fallback_policy")
                        ? timestamp : integer;
                if (nested) {
                    policy = builder.equal(new HiddenTimestamp(Collections.singletonList(new FieldRef(
                            builder.indexOf("payload"), "payload",
                            table.rowType().getTypeAt(builder.indexOf("payload"))))),
                            Timestamp.fromLocalDateTime(LocalDateTime.of(1969, 12, 31, 23, 59, 59)));
                }
                TableQueryAuthResult auth = mode.equals("integer_policy") || mode.equals("audit_log_policy")
                        ? new TableQueryAuthResult(
                        Collections.singletonList(JsonSerdeUtil.toJson(policy)), Collections.emptyMap())
                        : new TypedPolicyAuth(policy, mode.equals("masking_policy")
                                ? Collections.singletonMap("ts", new TimestampMask(Collections.singletonList(
                                        new FieldRef(1, "ts", DataTypes.TIMESTAMP(0))))) : Collections.emptyMap());
                split = new QueryAuthSplit(split, auth);
                params.put("paimon_predicate", encode(Collections.emptyList()));
                params.put("paimon." + PaimonJniScanner.DORIS_TIMESTAMP_PRECISION_REPAIR, "true");
            } else {
                params.put("paimon_predicate", encode(Collections.emptyList()));
                params.put("paimon." + PaimonJniScanner.DORIS_TIMESTAMP_PRECISION_REPAIR, "true");
            }
            if (mode.startsWith("fallback_")) {
                split = FallbackReadFileStoreTable.toFallbackSplit(split, true);
                Identifier mainId = Identifier.create("db", "main_events");
                catalog.createTable(mainId, Schema.newBuilder().column("id", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP(0)).option("file.format", "parquet").build(), false);
                table = new FallbackReadFileStoreTable((FileStoreTable) catalog.getTable(mainId),
                        (FileStoreTable) table, true);
                params.put("serialized_table", encode(table));
            }
            params.put("paimon_split", encode(split));
            PaimonJniScanner scanner = new PaimonJniScanner(16, params);
            try {
                scanner.open();
                if (mode.equals("legacy")) {
                    java.lang.reflect.Method predicates = PaimonJniScanner.class.getDeclaredMethod("getPredicates");
                    predicates.setAccessible(true);
                    Assertions.assertEquals(Collections.singletonList(integer), predicates.invoke(scanner));
                }
                Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                Assertions.assertEquals(expected.length, scanner.getTable().getNumRows());
                if (mode.equals("audit_log_policy")) {
                    Assertions.assertArrayEquals(new Object[] {"+I", "+I"},
                            scanner.getTable().getColumn(0).getObjectColumn(0, expected.length));
                }
                Object[] actual = scanner.getTable().getColumn(
                        mode.equals("legacy") || mode.equals("audit_log_policy") ? 1 : 0)
                        .getObjectColumn(0, expected.length);
                Arrays.sort(actual);
                Assertions.assertArrayEquals(expected, actual, mode);
            } finally {
                scanner.close();
                PaimonTableCache.clearForTest();
            }
        }
    }

    private static String encode(Object value) throws Exception {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(InstantiationUtil.serializeObject(value));
    }

    private static final class TypedPolicyAuth extends TableQueryAuthResult {
        private static final long serialVersionUID = 1L;
        private final Predicate policy;
        private final Map<String, Transform> masking;

        private TypedPolicyAuth(Predicate policy, Map<String, Transform> masking) {
            super(Collections.emptyList(), Collections.emptyMap());
            this.policy = policy;
            this.masking = masking;
        }

        @Override
        public Predicate extractPredicate() {
            // Exercise the SDK policy callback independently of its timestamp JSON codec.
            return policy;
        }

        @Override
        public Map<String, Transform> extractColumnMasking() {
            return masking;
        }
    }

    private static final class TimestampMask implements Transform {
        private static final long serialVersionUID = 1L;
        private final List<Object> inputs;

        private TimestampMask(List<Object> inputs) {
            this.inputs = inputs;
        }

        @Override
        public String name() {
            return "TEST_TIMESTAMP_MASK";
        }

        @Override
        public List<Object> inputs() {
            return inputs;
        }

        @Override
        public DataType outputType() {
            return DataTypes.TIMESTAMP(0);
        }

        @Override
        public Object transform(InternalRow row) {
            FieldRef source = (FieldRef) inputs.get(0);
            Assertions.assertEquals(0, row.getTimestamp(source.index(), 0).toLocalDateTime().getNano());
            return Timestamp.fromLocalDateTime(LocalDateTime.of(2020, 1, 1, 0, 0));
        }

        @Override
        public Transform copyWithNewInputs(List<Object> newInputs) {
            return new TimestampMask(newInputs);
        }
    }

    private static final class HiddenTimestamp implements Transform {
        private static final long serialVersionUID = 1L;
        private final List<Object> inputs;

        private HiddenTimestamp(List<Object> inputs) {
            this.inputs = inputs;
        }

        @Override
        public String name() {
            return "TEST_HIDDEN_TIMESTAMP";
        }

        @Override
        public List<Object> inputs() {
            return inputs;
        }

        @Override
        public DataType outputType() {
            return DataTypes.TIMESTAMP(0);
        }

        @Override
        public Object transform(InternalRow row) {
            return row.getRow(((FieldRef) inputs.get(0)).index(), 2).getTimestamp(1, 0);
        }

        @Override
        public Transform copyWithNewInputs(List<Object> newInputs) {
            return new HiddenTimestamp(newInputs);
        }
    }
}
