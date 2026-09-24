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
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
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

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergPartitionNameConflictTest {
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

    private Table createTable(int formatVersion) {
        return catalog.createTable(TableIdentifier.of("db", "events"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("record_key").build(),
                Map.of("format-version", Integer.toString(formatVersion)));
    }

    private void evolve(Table table) {
        table.updateSpec().removeField("record_key").commit();
        table.updateSpec().addField("key_alias", Expressions.ref("record_key")).commit();
        // Adding a transform forces a new spec instead of reusing an equivalent historical spec.
        table.updateSpec().renameField("key_alias", "record_key")
                .addField(Expressions.truncate("record_key", 4)).commit();
    }

    @Test
    void metadataTablesKeepDistinctHistoricalFields() {
        Table table = createTable(2);
        evolve(table);

        Types.StructType partition = Partitioning.partitionType(table);
        Assertions.assertEquals("record_key_1000", partition.field(1000).name());
        Assertions.assertEquals("record_key", partition.field(1001).name());
        Assertions.assertEquals(Types.LongType.get(), partition.field(1000).type());
        for (MetadataTableType type : MetadataTableType.values()) {
            Assertions.assertDoesNotThrow(() -> MetadataTableUtils.createMetadataTableInstance(table, type).schema(),
                    type.name());
        }
        Assertions.assertEquals("record_key", table.specs().get(0).fields().get(0).name());
    }

    @Test
    void generatedNamesDoNotShadowRealNames() {
        Table table = createTable(2);
        evolve(table);
        table.updateSpec().addField("record_key_1000", Expressions.bucket("record_key", 8))
                .addField("record_key_1000_", Expressions.bucket("record_key", 16)).commit();

        Types.StructType partition = Partitioning.partitionType(table);
        Assertions.assertEquals("record_key_1000__", partition.field(1000).name());
        Assertions.assertEquals("record_key", partition.field(1001).name());
        Assertions.assertEquals("record_key_1000", partition.field(1003).name());
        Assertions.assertEquals("record_key_1000_", partition.field(1004).name());
        Assertions.assertDoesNotThrow(() -> new Schema(partition.fields()));
    }

    @Test
    void caseOnlyNamesRemainUsableInSchemaAndPredicates() {
        Table table = createTable(2);
        table.updateSpec().removeField("record_key").commit();
        table.updateSpec().addField("key_alias", Expressions.ref("record_key")).commit();
        table.updateSpec().renameField("key_alias", "RECORD_KEY")
                .addField(Expressions.truncate("record_key", 4)).commit();

        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        IcebergConnectorMetadata connector = new IcebergConnectorMetadata(ops,
                IcebergCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
        for (MetadataTableType type : Arrays.asList(MetadataTableType.FILES, MetadataTableType.PARTITIONS)) {
            Schema schema = MetadataTableUtils.createMetadataTableInstance(table, type).schema();
            Assertions.assertEquals(1001, schema.caseInsensitiveFindField("partition.record_key").fieldId());
            Assertions.assertEquals(1000, schema.caseInsensitiveFindField("PARTITION.RECORD_KEY_1000").fieldId());

            IcebergTableHandle handle = IcebergTableHandle.forSystemTable(
                    "db", "events", type.name().toLowerCase(Locale.ROOT), -1L, null, -1L);
            ConnectorType partitionType = connector.getTableSchema(null, handle).getColumns().stream()
                    .filter(column -> column.getName().equals("partition"))
                    .findFirst().get().getType();
            Assertions.assertEquals(Arrays.asList("record_key_1000", "RECORD_KEY", "record_key_trunc_4"),
                    partitionType.getFieldNames());
            // Doris folds nested field names when constructing its struct type.
            Assertions.assertEquals(partitionType.getFieldNames().size(), partitionType.getFieldNames().stream()
                    .map(name -> name.toLowerCase(Locale.ROOT)).collect(Collectors.toSet()).size());

            IcebergPredicateConverter converter = new IcebergPredicateConverter(schema, ZoneOffset.UTC);
            for (String name : Arrays.asList("partition.record_key", "PARTITION.RECORD_KEY_1000")) {
                Assertions.assertDoesNotThrow(() -> converter.convert(new ConnectorComparison(
                        ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef(name, ConnectorType.of("BIGINT")),
                        new ConnectorLiteral(ConnectorType.of("BIGINT"), 7L))));
                Assertions.assertDoesNotThrow(() -> new Evaluator(
                        schema.asStruct(), Expressions.equal(name, 7L), false));
            }
        }
    }

    @Test
    void generatedNamesReserveCaseVariants() {
        Table table = createTable(2);
        evolve(table);
        table.updateSpec().addField("RECORD_KEY_1000", Expressions.bucket("record_key", 8))
                .addField("Record_Key_1000_", Expressions.bucket("record_key", 16)).commit();

        Types.StructType partition = Partitioning.partitionType(table);
        Assertions.assertEquals("record_key_1000__", partition.field(1000).name());
        Assertions.assertEquals("RECORD_KEY_1000", partition.field(1003).name());
        Assertions.assertEquals("Record_Key_1000_", partition.field(1004).name());
        Assertions.assertEquals(1000, new Schema(partition.fields())
                .caseInsensitiveFindField("RECORD_KEY_1000__").fieldId());
    }

    @Test
    void ordinaryEvolutionRetainsNamesAndIds() {
        Table table = createTable(2);
        table.updateSpec().renameField("record_key", "key_alias")
                .addField(Expressions.bucket("record_key", 8)).commit();

        Types.StructType partition = Partitioning.partitionType(table);
        Assertions.assertEquals("key_alias", partition.field(1000).name());
        Assertions.assertEquals("record_key_bucket_8", partition.field(1001).name());
        Assertions.assertEquals(2, partition.fields().size());
    }

    @Test
    void v1VoidTransformRetainsOriginalType() {
        Table table = createTable(1);
        table.updateSpec().removeField("record_key").commit();

        Types.StructType partition = Partitioning.partitionType(table);
        Assertions.assertEquals("record_key", partition.field(1000).name());
        Assertions.assertEquals(Types.LongType.get(), partition.field(1000).type());
        Assertions.assertTrue(Partitioning.groupingKeyType(table.schema(), table.specs().values())
                .fields().isEmpty());
    }

    @Test
    void v1ActiveReplacementKeepsCanonicalName() throws Exception {
        assertV1ReplacementMetadata(createV1Replacement());
    }

    @Test
    void v1DroppedReplacementKeepsCanonicalName() throws Exception {
        Table table = createV1Replacement();
        assertV1ReplacementMetadata(table);
        table.updateSpec().removeField("RECORD_KEY").commit();
        assertV1ReplacementMetadata(table);
    }

    private Table createV1Replacement() {
        Table table = createTable(1);
        append(table, "old.parquet", "record_key=7");
        table.updateSpec().removeField("record_key").commit();
        table.updateSpec().addField("RECORD_KEY", Expressions.ref("record_key")).commit();
        PartitionData current = new PartitionData(table.spec().partitionType());
        current.set(1, 9L);
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath("new.parquet")
                .withPartition(current).withRecordCount(1).withFileSizeInBytes(10).build()).commit();
        return table;
    }

    private void assertV1ReplacementMetadata(Table table) throws Exception {
        Types.StructType unified = Partitioning.partitionType(table);
        Assertions.assertEquals("record_key_1000", unified.field(1000).name());
        Assertions.assertEquals("RECORD_KEY", unified.field(1001).name());
        Assertions.assertEquals(Types.LongType.get(), unified.field(1000).type());
        Assertions.assertEquals(Types.LongType.get(), unified.field(1001).type());
        Assertions.assertEquals(Arrays.asList(1000, 1001), unified.fields().stream()
                .map(Types.NestedField::fieldId).collect(Collectors.toList()));

        for (MetadataTableType type : Arrays.asList(MetadataTableType.FILES, MetadataTableType.PARTITIONS)) {
            Table metadata = MetadataTableUtils.createMetadataTableInstance(table, type);
            Assertions.assertEquals(1001, metadata.schema()
                    .caseInsensitiveFindField("partition.record_key").fieldId());
            Assertions.assertEquals(1000, metadata.schema()
                    .caseInsensitiveFindField("partition.record_key_1000").fieldId());
            List<List<Long>> partitions = new ArrayList<>();
            try (CloseableIterable<FileScanTask> tasks = metadata.newScan().select("partition").planFiles()) {
                for (FileScanTask task : tasks) {
                    FileScanTask copy = IcebergSystemTableSerialization.deserializeFromBase64(
                            IcebergSystemTableSerialization.serializeToBase64(task));
                    try (CloseableIterable<StructLike> rows = copy.asDataTask().rows()) {
                        for (StructLike row : rows) {
                            StructLike partition = row.get(0, StructLike.class);
                            partitions.add(Arrays.asList(partition.get(0, Long.class), partition.get(1, Long.class)));
                        }
                    }
                }
            }
            Assertions.assertEquals(2, partitions.size(), type.name());
            Assertions.assertTrue(partitions.contains(Arrays.asList(7L, null)), type.name());
            Assertions.assertTrue(partitions.contains(Arrays.asList(null, 9L)), type.name());
        }
        assertPredicatesDistinguishHistoricalAndCurrentFields(table, "RECORD_KEY");
    }

    @Test
    void serializedMetadataTasksPreserveHistoricalAndCurrentValues() throws Exception {
        Table table = createTable(2);
        append(table, "old.parquet", "record_key=7");
        evolve(table);
        append(table, "new.parquet", "record_key=9/record_key_trunc_4=8");

        for (MetadataTableType type : Arrays.asList(MetadataTableType.FILES, MetadataTableType.PARTITIONS)) {
            Table metadata = MetadataTableUtils.createMetadataTableInstance(table, type);
            List<List<Long>> partitions = new ArrayList<>();
            try (CloseableIterable<FileScanTask> tasks = metadata.newScan().select("partition").planFiles()) {
                for (FileScanTask task : tasks) {
                    FileScanTask copy = IcebergSystemTableSerialization.deserializeFromBase64(
                            IcebergSystemTableSerialization.serializeToBase64(task));
                    try (CloseableIterable<StructLike> rows = copy.asDataTask().rows()) {
                        for (StructLike row : rows) {
                            StructLike partition = row.get(0, StructLike.class);
                            partitions.add(Arrays.asList(partition.get(0, Long.class),
                                    partition.get(1, Long.class), partition.get(2, Long.class)));
                        }
                    }
                }
            }
            Assertions.assertEquals(2, partitions.size(), type.name());
            Assertions.assertTrue(partitions.contains(Arrays.asList(7L, null, null)), type.name());
            Assertions.assertTrue(partitions.contains(Arrays.asList(null, 9L, 8L)), type.name());
        }
    }

    @Test
    void predicatesDistinguishHistoricalAndCurrentFields() throws Exception {
        Table table = createTable(2);
        append(table, "old.parquet", "record_key=7");
        evolve(table);
        append(table, "new.parquet", "record_key=9/record_key_trunc_4=8");

        assertPredicatesDistinguishHistoricalAndCurrentFields(table, "record_key");
    }

    private void assertPredicatesDistinguishHistoricalAndCurrentFields(Table table, String currentField)
            throws Exception {
        for (MetadataTableType type : Arrays.asList(MetadataTableType.FILES, MetadataTableType.PARTITIONS)) {
            Table metadata = MetadataTableUtils.createMetadataTableInstance(table, type);
            for (String field : Arrays.asList("record_key_1000", currentField)) {
                boolean historical = field.equals("record_key_1000");
                long expected = historical ? 7L : 9L;
                int count = 0;
                Expression predicate = Expressions.equal("partition." + field, expected);
                TableScan scan = metadata.newScan().select("partition").filter(predicate);
                Evaluator evaluator = new Evaluator(scan.schema().asStruct(), predicate, false);
                try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                    for (FileScanTask task : tasks) {
                        try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
                            for (StructLike row : rows) {
                                // Iceberg prunes manifests; the query engine still filters individual rows.
                                if (!evaluator.eval(row)) {
                                    continue;
                                }
                                StructLike partition = row.get(0, StructLike.class);
                                Assertions.assertEquals(expected, partition.get(historical ? 0 : 1, Long.class));
                                count++;
                            }
                        }
                    }
                }
                Assertions.assertEquals(1, count, type + ": " + field);
            }
        }
    }

    private void append(Table table, String path, String partitionPath) {
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(path).withPartitionPath(partitionPath).withRecordCount(1).withFileSizeInBytes(10).build())
                .commit();
    }
}
