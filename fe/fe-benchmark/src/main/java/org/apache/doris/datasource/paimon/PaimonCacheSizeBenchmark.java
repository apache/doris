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

package org.apache.doris.datasource.paimon;

import org.apache.doris.benchmark.BenchmarkHarness;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Measures Paimon 1.4.2 nested-schema/non-empty snapshot publication and prepared weight lookup. */
public class PaimonCacheSizeBenchmark {
    public long snapshotPublication(PublicationState state) {
        PaimonSnapshotCacheValue value = new PaimonSnapshotCacheValue(state.partitionInfo, state.snapshot);
        value.prepareForCachePublication(state.key);
        return requireComplete(value.getSizeEstimate());
    }

    public long tablePayloadCounter(PublicationState state) {
        return PaimonCacheSizeEstimator.retainedTablePayloadBytes(state.snapshot.getTable());
    }

    public long preparedWeightProvider(PreparedState state) {
        return state.value.getSizeEstimate().getBytes();
    }

    public long snapshotFormula(PreparedState state) {
        return requireComplete(PaimonCacheSizeEstimator.estimateSnapshotEntry(state.key, state.value));
    }

    public long partitionMapBaseline(PartitionPayloadState state) {
        return buildPartitionInfo(state, false);
    }

    public long partitionMapWithRetainedCounter(PartitionPayloadState state) {
        return buildPartitionInfo(state, true);
    }

    public static void main(String[] args) throws Exception {
        PaimonCacheSizeBenchmark benchmark = new PaimonCacheSizeBenchmark();
        for (int fieldCount : new int[] {10, 100}) {
            for (int partitionCount : new int[] {1000, 10000}) {
                String suffix = "[fields=" + fieldCount + ",partitions=" + partitionCount + "]";
                PublicationState publication = new PublicationState();
                publication.fieldCount = fieldCount;
                publication.partitionCount = partitionCount;
                publication.setup();
                BenchmarkHarness.measure("paimon.tablePayloadCounter" + suffix,
                        TimeUnit.MICROSECONDS, () -> benchmark.tablePayloadCounter(publication));
                BenchmarkHarness.measure("paimon.snapshotPublication" + suffix,
                        TimeUnit.MICROSECONDS, () -> benchmark.snapshotPublication(publication));

                PreparedState prepared = new PreparedState();
                prepared.fieldCount = fieldCount;
                prepared.partitionCount = partitionCount;
                prepared.setup();
                BenchmarkHarness.measure("paimon.preparedWeightProvider" + suffix,
                        TimeUnit.NANOSECONDS, () -> benchmark.preparedWeightProvider(prepared));
                BenchmarkHarness.measure("paimon.snapshotFormula" + suffix,
                        TimeUnit.NANOSECONDS, () -> benchmark.snapshotFormula(prepared));
            }
        }
        for (int partitionCount : new int[] {1000, 10000}) {
            for (boolean tailSkew : new boolean[] {false, true}) {
                PartitionPayloadState state = new PartitionPayloadState();
                state.partitionCount = partitionCount;
                state.tailSkew = tailSkew;
                state.setup();
                String suffix = "[partitions=" + partitionCount
                        + ",distribution=" + (tailSkew ? "tail-skew" : "uniform") + "]";
                BenchmarkHarness.measure("paimon.partitionMapBaseline" + suffix,
                        TimeUnit.MICROSECONDS, () -> benchmark.partitionMapBaseline(state));
                BenchmarkHarness.measure("paimon.partitionMapWithRetainedCounter" + suffix,
                        TimeUnit.MICROSECONDS, () -> benchmark.partitionMapWithRetainedCounter(state));
            }
        }
    }

    public static class PublicationState {
        public int fieldCount;

        public int partitionCount;

        private PaimonSnapshotEntryKey key;
        private PaimonPartitionInfo partitionInfo;
        private PaimonSnapshot snapshot;

        public void setup() throws Exception {
            Fixture fixture = newFixture(fieldCount, partitionCount);
            key = fixture.key;
            partitionInfo = fixture.value.getPartitionInfo();
            snapshot = fixture.value.getSnapshot();
        }
    }

    public static class PreparedState {
        public int fieldCount;

        public int partitionCount;

        private PaimonSnapshotEntryKey key;
        private PaimonSnapshotCacheValue value;

        public void setup() throws Exception {
            Fixture fixture = newFixture(fieldCount, partitionCount);
            key = fixture.key;
            value = fixture.value;
            value.prepareForCachePublication(fixture.key);
            requireComplete(value.getSizeEstimate());
        }
    }

    public static class PartitionPayloadState {
        public int partitionCount;

        public boolean tailSkew;

        private List<PartitionPayload> partitions;

        public void setup() {
            partitions = new ArrayList<>(partitionCount);
            String longTail = String.join("", Collections.nCopies(64 * 1024, "x"));
            for (int index = 0; index < partitionCount; index++) {
                String value = tailSkew && index % 997 == 0 ? longTail : String.valueOf(index);
                LinkedHashMap<String, String> typedSpec = new LinkedHashMap<>();
                for (int field = 0; field < 4; field++) {
                    typedSpec.put("partition_key_" + field, value + '_' + field);
                }
                String displayName = "partition_key_0=" + value;
                partitions.add(new PartitionPayload(
                        displayName, new ArrayList<>(typedSpec.values()), index));
            }
        }
    }

    private static Fixture newFixture(int fieldCount, int partitionCount) throws Exception {
        List<DataField> fields = new ArrayList<>(fieldCount + 2);
        fields.add(new DataField(0, "partition_key", new IntType()));
        for (int index = 0; index < fieldCount; index++) {
            fields.add(new DataField(index + 1, "field_" + index, new VarCharType()));
        }
        List<DataField> nestedFields = new ArrayList<>();
        for (int index = 0; index < 8; index++) {
            nestedFields.add(DataTypes.FIELD(fieldCount + index + 1,
                    "nested_field_" + index, DataTypes.STRING()));
        }
        fields.add(new DataField(fieldCount + 9, "nested_payload", new RowType(nestedFields)));
        TableSchema schema = new TableSchema(
                0L, fields, fieldCount + 9, Collections.singletonList("partition_key"),
                Collections.emptyList(), Collections.emptyMap(), null);
        FileStoreTable table = new AppendOnlyFileStoreTable(
                LocalFileIO.create(), new Path("file:/tmp/doris-paimon-cache-size-benchmark"),
                schema, CatalogEnvironment.empty());
        NameMapping mapping = NameMapping.createForTest(1L, "benchmark_db", "benchmark_table");
        PaimonSnapshotEntryKey key = new PaimonSnapshotEntryKey(mapping, 7L, schema.id(), 1L);
        HashMap<String, Partition> partitions = new HashMap<>(partitionCount);
        long retainedPartitionPayloadBytes = 0L;
        for (int index = 0; index < partitionCount; index++) {
            String name = "partition_key=" + index;
            String value = String.valueOf(index);
            partitions.put(name, new Partition(Collections.singletonMap("partition_key", value),
                    10L + index, 1024L + index, 1L, 1_700_000_000_000L + index, 1, true,
                    1_700_000_000_000L, "benchmark", 1_700_000_000_000L + index, "benchmark",
                    Collections.singletonMap("source", "benchmark")));
            retainedPartitionPayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                    retainedPartitionPayloadBytes, MetaCacheWeightUtils.estimatedStringBytes(name));
            retainedPartitionPayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                    retainedPartitionPayloadBytes, MetaCacheWeightUtils.estimatedStringBytes("partition_key"));
            retainedPartitionPayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                    retainedPartitionPayloadBytes, MetaCacheWeightUtils.estimatedStringBytes(value));
        }
        PaimonSnapshotCacheValue value = new PaimonSnapshotCacheValue(
                new PaimonPartitionInfo(Collections.emptyMap(), partitions, retainedPartitionPayloadBytes),
                new PaimonSnapshot(7L, schema.id(), table));
        return new Fixture(key, value);
    }

    private static long requireComplete(MetaCacheSizeEstimate estimate) {
        if (!estimate.isComplete()) {
            throw new IllegalStateException("incomplete benchmark estimate: " + estimate.getIncompleteReason());
        }
        return estimate.getBytes();
    }

    private long buildPartitionInfo(PartitionPayloadState state, boolean countPayload) {
        HashMap<String, Partition> partitions = new HashMap<>(state.partitionCount);
        long retainedPayloadBytes = 0L;
        for (PartitionPayload payload : state.partitions) {
            LinkedHashMap<String, String> typedSpec = new LinkedHashMap<>();
            for (int field = 0; field < payload.values.size(); field++) {
                String fieldName = "partition_key_" + field;
                String fieldValue = payload.values.get(field);
                typedSpec.put(fieldName, fieldValue);
                if (countPayload) {
                    retainedPayloadBytes = PaimonPartitionInfo.addRetainedStringPayload(
                            retainedPayloadBytes, fieldName);
                    retainedPayloadBytes = PaimonPartitionInfo.addRetainedStringPayload(
                            retainedPayloadBytes, fieldValue);
                }
            }
            if (countPayload) {
                retainedPayloadBytes = PaimonPartitionInfo.addRetainedStringPayload(
                        retainedPayloadBytes, payload.displayName);
            }
            int index = payload.index;
            partitions.put(payload.displayName, new Partition(
                    typedSpec, 10L + index, 1024L + index, 1L,
                    1_700_000_000_000L + index, 1, false));
        }
        PaimonPartitionInfo partitionInfo = new PaimonPartitionInfo(
                Collections.emptyMap(), partitions, retainedPayloadBytes);
        return MetaCacheWeightUtils.saturatedAdd(
                partitionInfo.getNameToPartition().size(), partitionInfo.getRetainedPayloadBytes());
    }

    private static class PartitionPayload {
        private final String displayName;
        private final List<String> values;
        private final int index;

        private PartitionPayload(
                String displayName, List<String> values, int index) {
            this.displayName = displayName;
            this.values = values;
            this.index = index;
        }
    }

    private static class Fixture {
        private final PaimonSnapshotEntryKey key;
        private final PaimonSnapshotCacheValue value;

        private Fixture(PaimonSnapshotEntryKey key, PaimonSnapshotCacheValue value) {
            this.key = key;
            this.value = value;
        }
    }
}
