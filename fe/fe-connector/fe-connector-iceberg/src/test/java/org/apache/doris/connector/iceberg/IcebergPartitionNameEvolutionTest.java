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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergPartitionNameEvolutionTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "record_key", Types.LongType.get()));
    private InMemoryCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
    }

    @AfterEach
    void tearDown() throws Exception {
        catalog.close();
    }

    private Table createTable(String name, int version) {
        return catalog.createTable(TableIdentifier.of("db", name), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("record_key").build(),
                Map.of("format-version", Integer.toString(version)));
    }

    @Test
    void reactivatedSpecKeepsCurrentNameAndValues() throws Exception {
        for (int version : Arrays.asList(2, 3)) {
            Table table = createTable("reactivate_v" + version, version);
            table.updateSpec().removeField("record_key").commit();
            table.updateSpec().addField("RECORD_KEY", Expressions.ref("record_key")).commit();
            List<Map<Integer, Object>> rows = new ArrayList<>();
            rows.add(append(table, 7L));
            table.updateSpec().removeField("RECORD_KEY").commit();
            table.updateSpec().addField("record_key", Expressions.ref("record_key")).commit();
            rows.add(append(table, 9L));

            Assertions.assertEquals(0, table.spec().specId());
            Assertions.assertEquals("record_key", Partitioning.partitionType(table).field(1000).name());
            Assertions.assertEquals("RECORD_KEY_1001", Partitioning.partitionType(table).field(1001).name());
            assertMetadata(table, rows);
        }
    }

    @Test
    void upgradeAndDropKeepsHistoricalOwnerAcrossSpecs() throws Exception {
        for (int version : Arrays.asList(2, 3)) {
            Table table = createTable("upgrade_v" + version, 1);
            List<Map<Integer, Object>> rows = new ArrayList<>();
            rows.add(append(table, 7L));
            table.updateSpec().removeField("record_key").commit();
            table.updateProperties().set("format-version", Integer.toString(version)).commit();
            table.updateSpec().addField("RECORD_KEY", Expressions.ref("record_key")).commit();
            rows.add(append(table, 9L));
            assertMetadata(table, rows);
            table.updateSpec().removeField("RECORD_KEY")
                    .addField("other", Expressions.bucket("record_key", 8)).commit();

            Assertions.assertEquals("record_key_1000", Partitioning.partitionType(table).field(1000).name());
            Assertions.assertEquals("RECORD_KEY", Partitioning.partitionType(table).field(1001).name());
            assertMetadata(table, rows);
        }
    }

    @Test
    void reactivatedSpecRestoresCurrentSpellingForSameFieldId() throws Exception {
        for (int version : Arrays.asList(1, 2, 3)) {
            Table table = createTable("rename_v" + version, version);
            List<Map<Integer, Object>> rows = Collections.singletonList(append(table, 7L));
            table.updateSpec().renameField("record_key", "key_alias").commit();
            assertMetadata(table, rows);
            table.updateSpec().renameField("key_alias", "record_key").commit();
            Assertions.assertEquals(0, table.spec().specId());
            assertMetadata(table, rows);
        }
    }

    @Test
    void repeatedEvolutionPreservesCurrentBindingsAndEveryHistoricalValue() throws Exception {
        for (int version : Arrays.asList(1, 2, 3)) {
            Table table = createTable("cycle_v" + version, version);
            List<Map<Integer, Object>> rows = new ArrayList<>();
            rows.add(append(table, 1L));
            String activeName = "record_key";
            // Revisit earlier spellings to exercise spec reuse as well as newly allocated IDs.
            for (String nextName : Arrays.asList("RECORD_KEY", "Record_Key", "record_key", "RECORD_KEY")) {
                table.updateSpec().removeField(activeName).commit();
                assertMetadata(table, rows);
                table.updateSpec().addField(nextName, Expressions.ref("record_key")).commit();
                rows.add(append(table, (long) rows.size() + 1));
                assertMetadata(table, rows);
                activeName = nextName;
            }
            table.updateSpec().removeField(activeName).commit();
            assertMetadata(table, rows);
        }
    }

    @Test
    void simultaneousCaseCollisionsReserveRealSuffixes() throws Exception {
        Table table = createTable("simultaneous", 2);
        table.updateSpec().addField("RECORD_KEY", Expressions.bucket("record_key", 8))
                .addField("record_key_1001", Expressions.truncate("record_key", 4)).commit();
        Types.StructType type = Partitioning.partitionType(table);
        Assertions.assertEquals("record_key", type.field(1000).name());
        Assertions.assertEquals("RECORD_KEY_1001_", type.field(1001).name());
        Assertions.assertEquals("record_key_1001", type.field(1002).name());
        assertMetadata(table, Collections.emptyList());
    }

    @Test
    void currentCaseCollisionsFollowSpecOrderAfterFieldReuse() throws Exception {
        Table table = createTable("current_order", 2);
        table.updateSpec().removeField("record_key").commit();
        table.updateSpec().addField("RECORD_KEY", Expressions.bucket("record_key", 8)).commit();
        table.updateSpec().addField("record_key", Expressions.ref("record_key")).commit();
        Assertions.assertEquals(Arrays.asList(1001, 1000), table.spec().fields().stream()
                .map(PartitionField::fieldId).collect(Collectors.toList()));
        Types.StructType type = Partitioning.partitionType(table);
        Assertions.assertEquals("RECORD_KEY", type.field(1001).name());
        Assertions.assertEquals("record_key_1000", type.field(1000).name());
        assertMetadata(table, Collections.emptyList());
    }

    @Test
    void droppedSourceColumnsRemainExcluded() throws Exception {
        Table table = createTable("dropped_source", 2);
        table.updateSpec().removeField("record_key").commit();
        table.updateSchema().addColumn("other", Types.LongType.get()).deleteColumn("record_key").commit();
        Assertions.assertTrue(Partitioning.partitionType(table).fields().isEmpty());
        Assertions.assertTrue(Partitioning.groupingKeyType(table.schema(), table.specs().values()).fields().isEmpty());
        for (MetadataTableType kind : Arrays.asList(MetadataTableType.FILES, MetadataTableType.PARTITIONS)) {
            Assertions.assertNull(MetadataTableUtils.createMetadataTableInstance(table, kind)
                    .schema().findField("partition"));
        }
    }

    private Map<Integer, Object> append(Table table, long value) {
        PartitionData data = new PartitionData(table.spec().partitionType());
        Map<Integer, Object> expected = new HashMap<>();
        for (int i = 0; i < table.spec().fields().size(); i++) {
            PartitionField field = table.spec().fields().get(i);
            if (field.transform().isIdentity()) {
                data.set(i, value);
                expected.put(field.fieldId(), value);
            }
        }
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath("row_" + value + ".parquet")
                .withPartition(data).withRecordCount(1).withFileSizeInBytes(10).build()).commit();
        return expected;
    }

    private void assertMetadata(Table table, List<Map<Integer, Object>> expected) throws Exception {
        Map<Integer, String> originalSpecs = table.specs().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
        Types.StructType unified = Partitioning.partitionType(table);
        List<Integer> ids = unified.fields().stream().map(Types.NestedField::fieldId).collect(Collectors.toList());
        Assertions.assertEquals(ids.stream().sorted().collect(Collectors.toList()), ids);
        Assertions.assertEquals(unified.fields().size(), unified.fields().stream()
                .map(field -> field.name().toLowerCase(Locale.ROOT)).distinct().count());
        Map<String, Long> currentNameCounts = table.spec().fields().stream()
                .filter(field -> !field.transform().isVoid())
                .collect(Collectors.groupingBy(field -> field.name().toLowerCase(Locale.ROOT), Collectors.counting()));
        for (PartitionField field : table.spec().fields()) {
            if (!field.transform().isVoid()
                    && currentNameCounts.get(field.name().toLowerCase(Locale.ROOT)) == 1) {
                Assertions.assertEquals(field.name(), unified.field(field.fieldId()).name(), table.name());
            }
        }

        Table copy = IcebergSystemTableSerialization.deserializeFromBase64(
                IcebergSystemTableSerialization.serializeToBase64(SerializableTable.copyOf(table)));
        Assertions.assertEquals(unified, Partitioning.partitionType(copy));
        List<PartitionSpec> reversedSpecs = new ArrayList<>(table.specs().values());
        Collections.reverse(reversedSpecs);
        Table reordered = new BaseTable(((BaseTable) table).operations(), table.name()) {
            @Override
            public Map<Integer, PartitionSpec> specs() {
                Map<Integer, PartitionSpec> result = new LinkedHashMap<>();
                reversedSpecs.forEach(spec -> result.put(spec.specId(), spec));
                return result;
            }
        };
        Assertions.assertEquals(unified, Partitioning.partitionType(reordered));
        Assertions.assertEquals(Partitioning.groupingKeyType(table.schema(), table.specs().values()),
                Partitioning.groupingKeyType(table.schema(), reversedSpecs));
        for (MetadataTableType kind : MetadataTableType.values()) {
            Assertions.assertDoesNotThrow(() -> MetadataTableUtils.createMetadataTableInstance(table, kind).schema());
        }

        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        IcebergConnectorMetadata connector = new IcebergConnectorMetadata(ops,
                IcebergCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
        for (MetadataTableType kind : Arrays.asList(MetadataTableType.FILES, MetadataTableType.PARTITIONS)) {
            Table metadata = MetadataTableUtils.createMetadataTableInstance(table, kind);
            IcebergTableHandle handle = IcebergTableHandle.forSystemTable(
                    "db", table.name(), kind.name().toLowerCase(Locale.ROOT), -1L, null, -1L);
            ConnectorType connectorType = connector.getTableSchema(null, handle).getColumns().stream()
                    .filter(column -> column.getName().equals("partition")).findFirst().get().getType();
            Assertions.assertEquals(unified.fields().stream().map(Types.NestedField::name).collect(Collectors.toList()),
                    connectorType.getFieldNames());
            List<Map<Integer, Object>> actual = new ArrayList<>();
            Schema projection = metadata.newScan().select("partition").schema();
            IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                    IcebergCatalogProperties.of(Collections.emptyMap()), ops);
            List<ConnectorScanRange> ranges = provider.planScan(null, ConnectorScanRequest.builder(handle,
                    Collections.singletonList(connector.getColumnHandles(null, handle).get("partition"))).build());
            for (ConnectorScanRange range : ranges) {
                FileScanTask taskCopy = IcebergSystemTableSerialization.deserializeFromBase64(
                        ((IcebergScanRange) range).getSerializedSplit());
                try (CloseableIterable<StructLike> rows = taskCopy.asDataTask().rows()) {
                    for (StructLike row : rows) {
                        StructLike partition = row.get(0, StructLike.class);
                        Map<Integer, Object> values = new HashMap<>();
                        for (int i = 0; i < unified.fields().size(); i++) {
                            Object value = partition.get(i, Object.class);
                            Types.NestedField field = unified.fields().get(i);
                            String path = "partition." + field.name();
                            Assertions.assertEquals(field.fieldId(), metadata.schema()
                                    .caseInsensitiveFindField(path.toUpperCase(Locale.ROOT)).fieldId());
                            if (value != null) {
                                values.put(field.fieldId(), value);
                            }
                            for (Map<Integer, Object> expectedRow : expected) {
                                if (expectedRow.containsKey(field.fieldId())) {
                                    Object needle = expectedRow.get(field.fieldId());
                                    Evaluator evaluator = new Evaluator(projection.asStruct(),
                                            Expressions.equal(path, needle), false);
                                    Assertions.assertEquals(needle.equals(value), evaluator.eval(row));
                                }
                            }
                        }
                        actual.add(values);
                    }
                }
            }
            Assertions.assertEquals(expected.size(), actual.size(), kind.name());
            Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual), kind.name());
            for (Map<Integer, Object> expectedRow : expected) {
                for (Map.Entry<Integer, Object> entry : expectedRow.entrySet()) {
                    String path = "partition." + unified.field(entry.getKey()).name();
                    Expression predicate = Expressions.equal(path, entry.getValue());
                    long expectedCount = expected.stream()
                            .filter(row -> entry.getValue().equals(row.get(entry.getKey()))).count();
                    Assertions.assertEquals(expectedCount, countFilteredRows(metadata, predicate), kind + ": " + path);
                }
            }
        }
        Assertions.assertEquals(originalSpecs, table.specs().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString())));
    }

    private long countFilteredRows(Table metadata, Expression predicate) throws Exception {
        Schema projection = metadata.newScan().select("partition").schema();
        Evaluator evaluator = new Evaluator(projection.asStruct(), predicate, false);
        long count = 0;
        try (CloseableIterable<FileScanTask> tasks = metadata.newScan().select("partition")
                .filter(predicate).planFiles()) {
            for (FileScanTask task : tasks) {
                try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
                    for (StructLike row : rows) {
                        // Metadata scans prune manifests; the query engine applies residual row filtering.
                        if (evaluator.eval(row)) {
                            count++;
                        }
                    }
                }
            }
        }
        return count;
    }
}
