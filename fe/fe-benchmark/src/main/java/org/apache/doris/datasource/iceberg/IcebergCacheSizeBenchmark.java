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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.benchmark.BenchmarkHarness;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Measures Iceberg table, long-history table, snapshot and manifest publication. */
public class IcebergCacheSizeBenchmark {
    public int tableValueConstruction(TablePublicationState state) {
        return new IcebergTableCacheValue(state.table).getIcebergTable().schema().schemaId();
    }

    public long tablePublication(TablePublicationState state) {
        IcebergTableCacheValue value = new IcebergTableCacheValue(state.table);
        value.prepareForCachePublication(state.mapping);
        return requireComplete(value.getSizeEstimate());
    }

    public long tablePayloadCounter(TablePublicationState state) {
        return IcebergCacheSizeEstimator.retainedTablePayloadBytes(state.table);
    }

    public long longHistoryTablePublication(LongHistoryTablePublicationState state) {
        IcebergTableCacheValue value = new IcebergTableCacheValue(state.table);
        value.prepareForCachePublication(state.mapping);
        return requireComplete(value.getSizeEstimate());
    }

    public long snapshotPublication(SnapshotPublicationState state) {
        IcebergSnapshotCacheValue value = new IcebergSnapshotCacheValue(
                state.partitionInfo, new IcebergSnapshot(7L, 0L), Optional.empty(), state.table);
        value.prepareForCachePublication(state.snapshotKey);
        return requireComplete(value.getSizeEstimate());
    }

    public int snapshotValueConstruction(SnapshotPublicationState state) {
        return new IcebergSnapshotCacheValue(
                state.partitionInfo, new IcebergSnapshot(7L, 0L), Optional.empty(), state.table)
                .getPartitionInfo().getNameToIcebergPartition().size();
    }

    public long preparedSnapshotCacheHit(SnapshotPublicationState state) {
        return state.preparedSnapshotValue.getIcebergTable().get()
                .currentSnapshot().snapshotId();
    }

    public long manifestPublication(ManifestState state) {
        return requireComplete(IcebergCacheSizeEstimator.estimateManifestEntry(state.key, state.value));
    }

    public int manifestValueConstruction(ManifestState state) {
        return ManifestCacheValue.forDataFiles(state.files).getDataFiles().size();
    }

    public int denseManifestReaderBaseline(DenseManifestState state) {
        List<DataFile> collected = new ArrayList<>();
        for (DataFile file : state.files) {
            collected.add(file.copy());
        }
        return ImmutableList.copyOf(collected).size();
    }

    public int denseManifestValueConstruction(DenseManifestState state) {
        ManifestCacheValue.Builder builder = ManifestCacheValue.dataFilesBuilder();
        for (DataFile file : state.files) {
            builder.addDataFile(file.copy());
        }
        ManifestCacheValue value = builder.build();
        if (value.isAccountingComplete() != state.expectedAccountingComplete()) {
            throw new IllegalStateException("unexpected dense manifest accounting state");
        }
        return value.getDataFiles().size();
    }

    public long preparedWeightProvider(PreparedState state) {
        return state.preparedTableValue.getSizeEstimate().getBytes();
    }

    public long tableFormula(PreparedState state) {
        return requireComplete(IcebergCacheSizeEstimator.estimateTableEntry(
                state.mapping, state.preparedTableValue));
    }

    public int preparedTableCacheHit(PreparedState state) {
        return state.preparedTableValue.getIcebergTable().schema().schemaId();
    }

    public static void main(String[] args) throws Exception {
        IcebergCacheSizeBenchmark benchmark = new IcebergCacheSizeBenchmark();
        for (int fieldCount : new int[] {10, 100}) {
            String suffix = "[fields=" + fieldCount + "]";
            TablePublicationState tableState = new TablePublicationState();
            tableState.fieldCount = fieldCount;
            tableState.setup();
            BenchmarkHarness.measure("iceberg.tableValueConstruction" + suffix,
                    TimeUnit.NANOSECONDS, () -> benchmark.tableValueConstruction(tableState));
            BenchmarkHarness.measure("iceberg.tablePayloadCounter" + suffix,
                    TimeUnit.MICROSECONDS, () -> benchmark.tablePayloadCounter(tableState));
            BenchmarkHarness.measure("iceberg.tablePublication" + suffix,
                    TimeUnit.MICROSECONDS, () -> benchmark.tablePublication(tableState));

            PreparedState prepared = new PreparedState();
            prepared.fieldCount = fieldCount;
            prepared.setup();
            BenchmarkHarness.measure("iceberg.preparedWeightProvider" + suffix,
                    TimeUnit.NANOSECONDS, () -> benchmark.preparedWeightProvider(prepared));
            BenchmarkHarness.measure("iceberg.tableFormula" + suffix,
                    TimeUnit.NANOSECONDS, () -> benchmark.tableFormula(prepared));
            BenchmarkHarness.measure("iceberg.preparedTableCacheHit" + suffix,
                    TimeUnit.MICROSECONDS, () -> benchmark.preparedTableCacheHit(prepared));
        }
        for (int fieldCount : new int[] {100, 1000}) {
            // Wide identity-partitioned specs drive the O(fields) fieldsBySourceId reservation and
            // the secondary partition Schema formula; nested schemas drive the per-field path
            // and lower-case String terms. Both must stay far below the value construction cost
            // of the same table.
            String suffix = "[fields=" + fieldCount + "]";
            TablePublicationState partitioned = new TablePublicationState();
            partitioned.fieldCount = fieldCount;
            partitioned.identityPartitioned = true;
            partitioned.setup();
            BenchmarkHarness.measure("iceberg.partitionedTableValueConstruction" + suffix,
                    TimeUnit.NANOSECONDS, () -> benchmark.tableValueConstruction(partitioned));
            BenchmarkHarness.measure("iceberg.partitionedTablePayloadCounter" + suffix,
                    TimeUnit.MICROSECONDS, () -> benchmark.tablePayloadCounter(partitioned));
            BenchmarkHarness.measure("iceberg.partitionedTablePublication" + suffix,
                    TimeUnit.MICROSECONDS, () -> benchmark.tablePublication(partitioned));

            TablePublicationState nested = new TablePublicationState();
            nested.fieldCount = fieldCount;
            nested.nestedSchema = true;
            nested.setup();
            BenchmarkHarness.measure("iceberg.nestedTablePayloadCounter" + suffix,
                    TimeUnit.MICROSECONDS, () -> benchmark.tablePayloadCounter(nested));
            BenchmarkHarness.measure("iceberg.nestedTablePublication" + suffix,
                    TimeUnit.MICROSECONDS, () -> benchmark.tablePublication(nested));
        }
        for (int snapshotCount : new int[] {1000, 10000}) {
            String suffix = "[snapshots=" + snapshotCount + "]";
            LongHistoryTablePublicationState state = new LongHistoryTablePublicationState();
            state.snapshotCount = snapshotCount;
            state.setup();
            BenchmarkHarness.measure("iceberg.longHistoryTablePublication" + suffix,
                    TimeUnit.MICROSECONDS, () -> benchmark.longHistoryTablePublication(state));
        }
        for (int fieldCount : new int[] {10, 100}) {
            for (int partitionCount : new int[] {1000, 10000}) {
                String suffix = "[fields=" + fieldCount + ",partitions=" + partitionCount + "]";
                SnapshotPublicationState state = new SnapshotPublicationState();
                state.fieldCount = fieldCount;
                state.partitionCount = partitionCount;
                state.setup();
                BenchmarkHarness.measure("iceberg.snapshotValueConstruction" + suffix,
                        TimeUnit.NANOSECONDS, () -> benchmark.snapshotValueConstruction(state));
                BenchmarkHarness.measure("iceberg.snapshotPublication" + suffix,
                        TimeUnit.MICROSECONDS, () -> benchmark.snapshotPublication(state));
                BenchmarkHarness.measure("iceberg.preparedSnapshotCacheHit" + suffix,
                        TimeUnit.MICROSECONDS, () -> benchmark.preparedSnapshotCacheHit(state));
            }
        }
        for (int fileCount : new int[] {100, 10000}) {
            ManifestState state = new ManifestState();
            state.fileCount = fileCount;
            state.setup();
            BenchmarkHarness.measure("iceberg.manifestPublication[files=" + fileCount + "]",
                    TimeUnit.MICROSECONDS, () -> benchmark.manifestPublication(state));
            BenchmarkHarness.measure("iceberg.manifestValueConstruction[files=" + fileCount + "]",
                    TimeUnit.MICROSECONDS, () -> benchmark.manifestValueConstruction(state));
        }
        for (int metricColumns : new int[] {100, 1000}) {
            for (int fileCount : new int[] {100, 10000}) {
                DenseManifestState state = new DenseManifestState();
                state.metricColumns = metricColumns;
                state.fileCount = fileCount;
                state.setup();
                String accounting = state.expectedAccountingComplete() ? "complete" : "rejected";
                String suffix = "[files=" + fileCount + ",metricColumns=" + metricColumns
                        + ",accounting=" + accounting + "]";
                BenchmarkHarness.measure("iceberg.denseManifestReaderBaseline" + suffix,
                        TimeUnit.MICROSECONDS, () -> benchmark.denseManifestReaderBaseline(state));
                BenchmarkHarness.measure("iceberg.denseManifestValueConstruction" + suffix,
                        TimeUnit.MICROSECONDS, () -> benchmark.denseManifestValueConstruction(state));
            }
        }
    }

    public static class TablePublicationState {
        public int fieldCount;
        public boolean identityPartitioned;
        public boolean nestedSchema;

        private NameMapping mapping;
        private Table table;

        public void setup() {
            mapping = NameMapping.createForTest(1L, "benchmark_db", "benchmark_table");
            table = newTable(fieldCount, identityPartitioned, nestedSchema);
        }
    }

    public static class SnapshotPublicationState {
        public int fieldCount;

        public int partitionCount;

        private Table table;
        private IcebergSnapshotEntryKey snapshotKey;
        private IcebergPartitionInfo partitionInfo;
        private IcebergSnapshotCacheValue preparedSnapshotValue;

        public void setup() {
            NameMapping mapping = NameMapping.createForTest(1L, "benchmark_db", "benchmark_table");
            table = newTable(fieldCount);
            partitionInfo = newPartitionInfo(partitionCount);
            snapshotKey = IcebergSnapshotEntryKey.tryCreate(mapping, table)
                    .orElseThrow(() -> new IllegalStateException("benchmark table has no generation key"));
            preparedSnapshotValue = new IcebergSnapshotCacheValue(
                    partitionInfo, new IcebergSnapshot(7L, 0L), Optional.empty(), table);
            preparedSnapshotValue.prepareForCachePublication(snapshotKey);
            requireComplete(preparedSnapshotValue.getSizeEstimate());
        }
    }

    public static class LongHistoryTablePublicationState {
        public int snapshotCount;

        private NameMapping mapping;
        private Table table;

        public void setup() {
            mapping = NameMapping.createForTest(1L, "benchmark_db", "benchmark_table");
            table = newLongHistoryTable(snapshotCount);
        }
    }

    public static class PreparedState {
        public int fieldCount;

        private IcebergTableCacheValue preparedTableValue;
        private NameMapping mapping;

        public void setup() {
            mapping = NameMapping.createForTest(1L, "benchmark_db", "benchmark_table");
            Table table = newTable(fieldCount);
            preparedTableValue = new IcebergTableCacheValue(table);
            preparedTableValue.prepareForCachePublication(mapping);
            requireComplete(preparedTableValue.getSizeEstimate());
        }
    }

    public static class ManifestState {
        public int fileCount;

        private IcebergManifestEntryKey key;
        private ManifestCacheValue value;
        private List<DataFile> files;

        public void setup() {
            key = new IcebergManifestEntryKey("/benchmark/manifest.avro", ManifestContent.DATA);
            files = new ArrayList<>(fileCount);
            for (int index = 0; index < fileCount; index++) {
                files.add(DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/benchmark/data/file-" + index + ".parquet")
                        .withFileSizeInBytes(1024L + index)
                        .withRecordCount(10L + index)
                        .build());
            }
            value = ManifestCacheValue.forDataFiles(files);
        }
    }

    public static class DenseManifestState {
        public int fileCount;

        public int metricColumns;

        private List<DataFile> files;
        private boolean accountingComplete;

        public void setup() {
            int poolSize = Math.min(fileCount, 256);
            List<DataFile> filePool = new ArrayList<>(poolSize);
            for (int fileIndex = 0; fileIndex < poolSize; fileIndex++) {
                HashMap<Integer, ByteBuffer> lowerBounds = new HashMap<>(metricColumns);
                HashMap<Integer, ByteBuffer> upperBounds = new HashMap<>(metricColumns);
                for (int columnIndex = 0; columnIndex < metricColumns; columnIndex++) {
                    lowerBounds.put(columnIndex, ByteBuffer.allocate(16));
                    upperBounds.put(columnIndex, ByteBuffer.allocate(32));
                }
                Metrics metrics = new Metrics(
                        10L,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        lowerBounds,
                        upperBounds);
                filePool.add(DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/benchmark/data/dense-" + fileIndex + ".parquet")
                        .withFileSizeInBytes(1024L)
                        .withMetrics(metrics)
                        .build());
            }
            files = new ArrayList<>(fileCount);
            for (int index = 0; index < fileCount; index++) {
                files.add(filePool.get(index % poolSize));
            }
            accountingComplete = ManifestCacheValue.forDataFiles(files).isAccountingComplete();
        }

        private boolean expectedAccountingComplete() {
            return accountingComplete;
        }
    }

    private static Table newTable(int fieldCount) {
        return newTable(fieldCount, false, false);
    }

    private static Table newTable(int fieldCount, boolean identityPartitioned, boolean nestedSchema) {
        List<Types.NestedField> fields = new ArrayList<>(fieldCount);
        for (int index = 0; index < fieldCount; index++) {
            fields.add(Types.NestedField.optional(index + 1, "field_" + index, Types.StringType.get()));
        }
        Schema schema;
        if (nestedSchema) {
            List<Types.NestedField> nestedFields = new ArrayList<>(fieldCount);
            for (int index = 0; index < fieldCount; index++) {
                nestedFields.add(Types.NestedField.optional(
                        1000 + index, "Nested_" + index, Types.StringType.get()));
            }
            schema = new Schema(
                    Types.NestedField.optional(1, "payload", Types.StructType.of(nestedFields)),
                    Types.NestedField.optional(2, "list", Types.ListType.ofOptional(3,
                            Types.StructType.of(Types.NestedField.optional(
                                    4, "leaf", Types.StringType.get())))),
                    Types.NestedField.optional(5, "id", Types.LongType.get()));
        } else {
            schema = new Schema(fields);
        }
        PartitionSpec spec = PartitionSpec.unpartitioned();
        if (identityPartitioned) {
            PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
            for (Types.NestedField field : schema.columns()) {
                specBuilder.identity(field.name());
            }
            spec = specBuilder.build();
        }
        TableMetadata metadata = TableMetadata.newTableMetadata(
                schema, spec, "file:/benchmark/table", Collections.emptyMap());
        Snapshot snapshot = SnapshotParser.fromJson("{\"snapshot-id\":7,\"timestamp-ms\":1,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifest-list\":\"/benchmark/manifest-list.avro\",\"schema-id\":0}");
        metadata = TableMetadata.buildFrom(metadata).setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
                .discardChanges()
                .withMetadataLocation("file:/benchmark/table/metadata/v1.json").build();
        return new BaseTable(new StaticTableOperations(metadata, new InMemoryFileIO()), "benchmark.table");
    }

    private static Table newLongHistoryTable(int snapshotCount) {
        long currentSnapshotId = 1000L + snapshotCount - 1L;
        StringBuilder json = new StringBuilder()
                .append("{\"format-version\":2,\"table-uuid\":\"benchmark-table\",")
                .append("\"location\":\"file:/benchmark/table\",\"last-sequence-number\":")
                .append(snapshotCount).append(",\"last-updated-ms\":").append(snapshotCount)
                .append(",\"last-column-id\":1,\"current-schema-id\":0,")
                .append("\"schemas\":[{\"type\":\"struct\",\"schema-id\":0,\"fields\":[")
                .append("{\"id\":1,\"name\":\"field\",\"required\":false,\"type\":\"string\"}]}],")
                .append("\"default-spec-id\":0,\"partition-specs\":[{\"spec-id\":0,\"fields\":[]}],")
                .append("\"last-partition-id\":999,\"default-sort-order-id\":0,")
                .append("\"sort-orders\":[{\"order-id\":0,\"fields\":[]}],\"properties\":{},")
                .append("\"current-snapshot-id\":").append(currentSnapshotId)
                .append(",\"refs\":{\"main\":{\"snapshot-id\":").append(currentSnapshotId)
                .append(",\"type\":\"branch\"}},\"snapshots\":[");
        for (int index = 0; index < snapshotCount; index++) {
            if (index > 0) {
                json.append(',');
            }
            json.append("{\"sequence-number\":").append(index + 1L)
                    .append(",\"snapshot-id\":").append(1000L + index)
                    .append(",\"timestamp-ms\":").append(index + 1L)
                    .append(",\"summary\":{\"operation\":\"append\"},")
                    .append("\"manifest-list\":\"/benchmark/history/list-").append(index)
                    .append(".avro\",\"schema-id\":0}");
        }
        json.append("],\"statistics\":[],\"partition-statistics\":[],\"snapshot-log\":[");
        for (int index = 0; index < snapshotCount; index++) {
            if (index > 0) {
                json.append(',');
            }
            json.append("{\"timestamp-ms\":").append(index + 1L)
                    .append(",\"snapshot-id\":").append(1000L + index).append('}');
        }
        json.append("],\"metadata-log\":[]}");
        TableMetadata metadata = TableMetadataParser.fromJson(
                "file:/benchmark/table/metadata/v1.json", json.toString());
        return new BaseTable(
                new StaticTableOperations(metadata, new InMemoryFileIO()), "benchmark.table");
    }

    private static IcebergPartitionInfo newPartitionInfo(int partitionCount) {
        HashMap<String, IcebergPartition> partitions = new HashMap<>(partitionCount);
        long retainedPayloadBytes = 0L;
        for (int index = 0; index < partitionCount; index++) {
            String name = "partition_key=value_" + index;
            IcebergPartition partition = new IcebergPartition(name, 0, 10L + index, 1024L + index,
                    1L, 1_700_000_000_000L + index, 7L,
                    Collections.singletonList("value_" + index), Collections.singletonList("identity"));
            partitions.put(name, partition);
            retainedPayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                    retainedPayloadBytes, partition.getRetainedPayloadBytes());
        }
        return new IcebergPartitionInfo(
                Collections.emptyMap(), partitions, Collections.emptyMap(), retainedPayloadBytes);
    }

    private static long requireComplete(MetaCacheSizeEstimate estimate) {
        if (!estimate.isComplete()) {
            throw new IllegalStateException("incomplete benchmark estimate: " + estimate.getIncompleteReason());
        }
        return estimate.getBytes();
    }

}
