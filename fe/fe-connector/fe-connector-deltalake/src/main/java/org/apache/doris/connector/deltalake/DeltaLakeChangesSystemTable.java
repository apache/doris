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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.thrift.TDeltaLakeCdfDesc;
import org.apache.doris.thrift.TDeltaLakeCdfRowFilterMode;
import org.apache.doris.thrift.TDeltaLakeDeletionVectorDesc;
import org.apache.doris.thrift.TDeltaLakeFileDesc;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Physical change files, inferred whole-file changes and row-position differences share one native scan. */
final class DeltaLakeChangesSystemTable implements DeltaLakeSystemTable {
    static final String CHANGE = "_change_type";
    static final String VERSION = "_commit_version";
    static final String TIMESTAMP = "_commit_timestamp";
    private static final List<String> CHANGE_TYPES = List.of("insert", "delete", "update_preimage", "update_postimage");
    private static final StructType FILE = new StructType().add("path", StringType.STRING)
            .add("partitionValues", new MapType(StringType.STRING, StringType.STRING, true))
            .add("size", LongType.LONG).add("dataChange", BooleanType.BOOLEAN)
            .add("deletionVector", DeltaKernelCompat.deletionVectorSchema());
    private static final StructType CDC = new StructType().add("cdc", FILE);
    private static final StructType INFERRED = new StructType().add("add", FILE).add("remove", FILE);

    @Override
    public String name() {
        return "changes";
    }

    @Override
    public boolean supportsTimeTravel() {
        return false;
    }

    @Override
    public ConnectorTableSchema schema(DeltaLakeRuntime runtime, DeltaLakeTableHandle handle) {
        ConnectorTableSchema base = DeltaLakeSchemaBuilder.build(handle.getTable(), handle.snapshot(runtime),
                runtime.properties);
        List<ConnectorColumn> columns = new ArrayList<>(base.getColumns());
        for (ConnectorColumn field : columns) {
            if (List.of(CHANGE, VERSION, TIMESTAMP).stream().anyMatch(field.getName()::equalsIgnoreCase)) {
                throw new DorisConnectorException("Reserved change data feed column: " + field.getName());
            }
        }
        columns.add(column(CHANGE, StringType.STRING, runtime));
        columns.add(column(VERSION, LongType.LONG, runtime));
        columns.add(column(TIMESTAMP, TimestampType.TIMESTAMP, runtime).withTimeZone());
        // Current base-table partitions cannot describe historical removes or CDC-only files.
        // The scan still supplies path keys to BE and evaluates the predicate on each action's values.
        Map<String, String> properties = new HashMap<>(base.getProperties());
        properties.remove(ConnectorTableSchema.PARTITION_COLUMNS_KEY);
        return new ConnectorTableSchema(handle.getTable() + "$changes", columns, base.getTableFormatType(),
                properties, Set.of(ConnectorCapability.SUPPORTS_SYS_TABLE_NESTED_COLUMN_PRUNE));
    }

    private static ConnectorColumn column(String name, DataType type, DeltaLakeRuntime runtime) {
        return new ConnectorColumn(name, DeltaLakeTypeMapping.toConnectorType(type,
                runtime.properties.isMapTimestampTz(), runtime.properties.isMapVarbinary()), "", true, null);
    }

    @Override
    public long streamingSplitEstimate(ConnectorSession session) {
        return session != null && "false".equalsIgnoreCase(
                session.getProperty("enable_external_table_batch_mode", String.class)) ? -1 : 1;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, DeltaLakeRuntime runtime,
            DeltaLakeTableHandle handle, ConnectorScanRequest request) {
        List<ConnectorScanRange> splits = new ArrayList<>();
        try (Source source = new Source(session, runtime, handle, request)) {
            while (source.hasNext()) {
                splits.add(source.next());
            }
        }
        return splits;
    }

    @Override
    public ConnectorSplitSource streamSplits(ConnectorSession session, DeltaLakeRuntime runtime,
            DeltaLakeTableHandle handle, ConnectorScanRequest request, boolean countOnly) {
        return new Source(session, runtime, handle, request);
    }

    private record Action(String path, String kernelPath, Map<String, String> partitions, long size,
            TDeltaLakeDeletionVectorDesc dv) {
        Action withMissingFileMetadataFrom(Action pairedAdd) {
            if (partitions != null && size >= 0) {
                return this;
            }
            return new Action(path, kernelPath, partitions == null ? pairedAdd.partitions : partitions,
                    size < 0 ? pairedAdd.size : size, dv);
        }
    }

    private static final class Source implements ConnectorSplitSource {
        private final DeltaLakeRuntime runtime;
        private final DeltaLakeTableHandle handle;
        private final ConnectorScanRequest request;
        private final Snapshot snapshot;
        private final Engine engine;
        private final ZoneId zone;
        private final long splitSize;
        private final Iterator<DeltaLakeCdfRange.Commit> commits;
        private final Predicate<Map<String, String>> filter;
        private final Set<String> binaryPartitions;
        private Iterator<ConnectorScanRange> ranges = Collections.emptyIterator();
        private boolean closed;

        Source(ConnectorSession session, DeltaLakeRuntime runtime, DeltaLakeTableHandle handle,
                ConnectorScanRequest request) {
            this.runtime = runtime;
            this.handle = handle;
            this.request = request;
            snapshot = handle.snapshot(runtime);
            engine = runtime.engine(handle.reference());
            zone = DeltaLakePartitionValues.sessionZone(session);
            splitSize = DeltaLakeScanPlanProvider.targetSplitSize(session);
            DeltaLakeCdfRange range = handle.cdf();
            commits = request.isPartitionsPrunedToEmpty() || range.start() > range.end() ? Collections.emptyIterator()
                    : DeltaLakeCdfRange.commits(session, runtime, handle, snapshot, range.start(), range.end())
                            .iterator();
            List<String> partitionNames = snapshot.getPartitionColumnNames();
            StructType constants = new StructType(partitionNames.stream().map(snapshot.getSchema()::get)
                    .map(field -> field.withTypeChanges(List.of())).toList())
                    .add(CHANGE, StringType.STRING).add(VERSION, LongType.LONG).add(TIMESTAMP, TimestampType.TIMESTAMP);
            DeltaLakePredicateConverter converter = new DeltaLakePredicateConverter(constants, zone,
                    List.of(), List.of());
            filter = request.getFilter().flatMap(converter::convert)
                    .map(predicate -> DeltaKernelCompat.normalizedPartitionFilter(engine, constants, predicate))
                    .orElse(values -> true);
            binaryPartitions = runtime.properties.isMapVarbinary() ? partitionNames.stream()
                    .filter(name -> snapshot.getSchema().get(name).getDataType() instanceof BinaryType)
                    .collect(Collectors.toUnmodifiableSet()) : Set.of();
        }

        @Override
        public boolean hasNext() {
            return runtime.execute(() -> {
                while (!closed && !ranges.hasNext() && commits.hasNext()) {
                    request.getScanContext().checkCancelled();
                    ranges = read(commits.next()).iterator();
                }
                return !closed && ranges.hasNext();
            });
        }

        @Override
        public ConnectorScanRange next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return ranges.next();
        }

        @Override
        public void close() {
            closed = true;
            ranges = Collections.emptyIterator();
        }

        private List<ConnectorScanRange> read(DeltaLakeCdfRange.Commit commit) throws IOException {
            List<ConnectorScanRange> result = new ArrayList<>();
            if (commit.hasCdc()) {
                List<Action> cdc = new ArrayList<>();
                DeltaLakeCdfRange.readActions(engine, commit.file(), CDC, row -> {
                    request.getScanContext().checkCancelled();
                    if (!row.isNullAt(0)) {
                        cdc.add(action(row.getStruct(0)));
                    }
                });
                for (Action file : cdc) {
                    emit(result, commit, file, null, null, false);
                }
                return result;
            }
            Map<String, Action> adds = new LinkedHashMap<>();
            Map<String, Action> removes = new LinkedHashMap<>();
            DeltaLakeCdfRange.readActions(engine, commit.file(), INFERRED, row -> {
                request.getScanContext().checkCancelled();
                if (!row.isNullAt(0) || !row.isNullAt(1)) {
                    boolean add = !row.isNullAt(0);
                    Row file = row.getStruct(add ? 0 : 1);
                    if (file.isNullAt(3)) {
                        throw new DorisConnectorException("Missing dataChange at Delta version " + commit.version());
                    }
                    if (file.getBoolean(3)) {
                        Action change = action(file);
                        (add ? adds : removes).put(change.path, change);
                    }
                }
            });
            for (Action add : adds.values()) {
                Action remove = removes.remove(add.path);
                if (remove == null) {
                    emit(result, commit, add, "insert", null, false);
                } else if (!Objects.equals(add.dv, remove.dv)) {
                    if (add.dv != null) {
                        emit(result, commit, add, "delete", remove.dv, true);
                    }
                    if (remove.dv != null) {
                        // A non-extended remove can omit the size and partitions of this same file.
                        emit(result, commit, remove.withMissingFileMetadataFrom(add), "insert", add.dv, true);
                    }
                }
            }
            for (Action remove : removes.values()) {
                emit(result, commit, remove, "delete", null, false);
            }
            return result;
        }

        private Action action(Row file) {
            String path = DeltaKernelCompat.dataPath(handle.getPath(), file.getString(0));
            Map<String, String> partitions = file.isNullAt(1) ? null
                    : DeltaKernelCompat.partitionValues(file.getMap(1));
            TDeltaLakeDeletionVectorDesc dv = file.isNullAt(4) ? null
                    : DeltaKernelCompat.actionDeletionVector(file.getStruct(4), handle.getPath()).orElse(null);
            if (dv != null && dv.isSetPath()) {
                dv.setPath(runtime.storageContext.getStorageContext().normalizeStorageUri(dv.getPath()));
            }
            String storagePath = runtime.storageContext.getStorageContext().normalizeStorageUri(path);
            return new Action(storagePath, path, partitions, file.isNullAt(2) ? -1 : file.getLong(2), dv);
        }

        private void emit(List<ConnectorScanRange> result, DeltaLakeCdfRange.Commit commit, Action file,
                String type, TDeltaLakeDeletionVectorDesc base, boolean difference) throws IOException {
            boolean mapped = DeltaLakeSchemaBuilder.hasColumnMapping(snapshot);
            for (String name : snapshot.getPartitionColumnNames()) {
                if (file.partitions == null || !file.partitions.containsKey(
                        DeltaLakePartitionValues.logKey(snapshot.getSchema().get(name), mapped))) {
                    throw new DorisConnectorException("Missing CDF partition value " + name + " for " + file.path);
                }
            }
            Map<String, String> partitions = DeltaLakePartitionValues.normalize(snapshot,
                    file.partitions == null ? Map.of() : file.partitions, zone);
            if (!request.getRequiredPartitions().isEmpty()
                    && !request.getRequiredPartitions().contains(DeltaLakePartitionValues.partitionName(partitions))) {
                return;
            }
            Map<String, String> constants = new HashMap<>(partitions);
            constants.put(VERSION, Long.toString(commit.version()));
            constants.put(TIMESTAMP, Instant.ofEpochMilli(commit.timestamp()).toString());
            if ((type == null ? CHANGE_TYPES : List.of(type)).stream().noneMatch(candidate -> {
                constants.put(CHANGE, candidate);
                return filter.test(constants);
            })) {
                return;
            }
            long size = file.size < 0 ? engine.getFileSystemClient().getFileStatus(file.kernelPath).getSize()
                    : file.size;
            if (size <= 0) {
                throw new DorisConnectorException("Invalid change data Parquet file size for " + file.path);
            }
            TDeltaLakeCdfDesc cdf = new TDeltaLakeCdfDesc().setCommitVersion(commit.version())
                    .setCommitTimestampMillis(commit.timestamp()).setRowFilterMode(difference
                            ? TDeltaLakeCdfRowFilterMode.KEEP_DELETION_VECTOR_DIFF
                            : TDeltaLakeCdfRowFilterMode.EXCLUDE_DELETION_VECTOR);
            cdf.setConstantChangeType(type);
            cdf.setBaseDeletionVector(base);
            TDeltaLakeFileDesc desc = new TDeltaLakeFileDesc().setCdf(cdf).setDeletionVector(file.dv);
            if (mapped) {
                desc.setSchemaId(DeltaLakeSchemaBuilder.FILE_SCHEMA_ID);
            }
            for (long offset = 0; offset < size;) {
                long length = Math.min(splitSize, size - offset);
                result.add(new DeltaLakeScanRange(file.path, offset, length, size, -1, splitSize, partitions,
                        desc, binaryPartitions));
                offset += length;
            }
        }
    }
}
