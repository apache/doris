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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScopes;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanProfile;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TDeltaLakeColumnMappingMode;
import org.apache.doris.thrift.TDeltaLakeDeletionVectorDesc;
import org.apache.doris.thrift.TDeltaLakeFileDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

final class DeltaLakeScanPlanProvider implements ConnectorScanPlanProvider {
    private static final String SCHEMA_MAPPING = "deltalake.schema_mapping";
    private static final long DEFAULT_SPLIT_SIZE = 128L << 20;
    private final DeltaLakeRuntime runtime;
    private final DeltaLakeStatisticsCache statistics;
    private final boolean mapVarbinary;
    private final DeltaLakeTableKind kind;
    // The connector creates a provider per scan node. The gate runs before that node opens its source.
    private boolean streamingCount;
    private volatile DeltaLakeScanMetrics.Result scanMetrics;
    private boolean planningReuseHit;

    DeltaLakeScanPlanProvider(DeltaLakeRuntime runtime, DeltaLakeStatisticsCache statistics, boolean mapVarbinary,
            DeltaLakeTableKind kind) {
        this.runtime = runtime;
        this.statistics = statistics;
        this.mapVarbinary = mapVarbinary;
        this.kind = kind;
    }

    @Override
    public boolean supportsSystemTableTimeTravel() {
        return kind.systemTable != null && kind.systemTable.supportsTimeTravel();
    }

    private record ScanFilter(Optional<Predicate> kernelPredicate,
            java.util.function.Predicate<Map<String, String>> partitionPredicate) {
    }

    private record FileSchema(boolean mapped, Set<String> varbinaryPartitions) {
    }

    private FileSchema fileSchema(Snapshot snapshot) {
        StructType schema = snapshot.getSchema();
        Set<String> varbinaryPartitions = mapVarbinary ? snapshot.getPartitionColumnNames().stream()
                .filter(name -> schema.get(name).getDataType() instanceof BinaryType)
                .collect(Collectors.toUnmodifiableSet()) : Set.of();
        return new FileSchema(DeltaLakeSchemaBuilder.hasColumnMapping(snapshot), varbinaryPartitions);
    }

    private DeltaLakeStatementScope.FileCount knownFileCount(ConnectorSession session, DeltaLakeTableHandle handle,
            Snapshot snapshot) {
        DeltaLakeStatementScope.FileCount count = DeltaLakeStatementScope.fileCount(session, handle);
        OptionalLong checksum = DeltaKernelCompat.fileCount(snapshot);
        if (checksum.isPresent()) {
            count.value = checksum.getAsLong();
        } else if (count.value < 0) {
            statistics.cachedFileCount(session, handle).ifPresent(files -> count.value = files);
        }
        return count;
    }

    private DeltaLakeScanMetrics newScanMetrics(ConnectorSession session, DeltaLakeTableHandle handle,
            Snapshot snapshot) {
        return new DeltaLakeScanMetrics(handle, knownFileCount(session, handle, snapshot),
                statistics.fileCountObserver(session, handle, runtime));
    }

    @Override
    public long streamingSplitEstimate(ConnectorSession session, ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter, boolean countPushdown) {
        streamingCount = false;
        DeltaLakeSystemTable systemTable = ((DeltaLakeTableHandle) handle).kind().systemTable;
        if (systemTable != null) {
            streamingCount = countPushdown && filter.isEmpty();
            return systemTable.streamingSplitEstimate(session);
        }
        String enabled = session.getProperty("enable_external_table_batch_mode", String.class);
        if (enabled != null && !Boolean.parseBoolean(enabled)) {
            return -1;
        }
        String configured = session.getProperty("num_files_in_batch_mode", String.class);
        long threshold = configured == null ? 10000 : Long.parseLong(configured);
        return runtime.execute(() -> {
            DeltaLakeTableHandle delta = (DeltaLakeTableHandle) handle;
            Snapshot snapshot = delta.snapshot(runtime);
            long files = knownFileCount(session, delta, snapshot).value;
            // Old writers may not produce a checksum. Unknown size streams conservatively instead
            // of enumerating AddFiles or waiting for statistics merely to choose a scheduling mode.
            if (files >= 0 && files < threshold) {
                return -1L;
            }
            streamingCount = countPushdown && filter.isEmpty();
            return files >= 0 ? files : threshold;
        });
    }

    @Override
    public ConnectorSplitSource streamSplits(ConnectorSession session, ConnectorScanRequest request) {
        planningReuseHit = false;
        request.getScanContext().checkCancelled();
        DeltaLakeTableHandle handle = (DeltaLakeTableHandle) request.getTableHandle();
        if (handle.kind().systemTable != null) {
            return runtime.execute(() -> handle.kind().systemTable.streamSplits(
                    session, runtime, handle, request, streamingCount || request.isCountPushdown()));
        }
        return runtime.execute(() -> new StreamingSource(session, request));
    }

    private final class StreamingSource implements ConnectorSplitSource {
        private final ConnectorSession session;
        private final DeltaLakeTableHandle handle;
        private final Snapshot snapshot;
        private final ZoneId zone;
        private final ScanFilter filter;
        private final long targetSize;
        private final FileSchema fileSchema;
        private final boolean countPushdown;
        private final ConnectorScanContext scanContext;
        private final boolean partitionsPrunedToEmpty;
        private final DeltaLakeScanMetrics metrics;
        private CloseableIterator<Row> files;
        private DeltaLakeScanRange file;
        private DeltaLakeScanRange buffered;
        private long offset;
        private boolean countChecked;
        private boolean countOnly;
        private boolean closed;

        private StreamingSource(ConnectorSession session, ConnectorScanRequest request) {
            this.session = session;
            handle = (DeltaLakeTableHandle) request.getTableHandle();
            snapshot = handle.snapshot(runtime);
            zone = DeltaLakePartitionValues.sessionZone(session);
            filter = scanFilter(zone, snapshot, request);
            targetSize = targetSplitSize(session);
            fileSchema = fileSchema(snapshot);
            scanContext = request.getScanContext();
            partitionsPrunedToEmpty = request.isPartitionsPrunedToEmpty();
            countPushdown = streamingCount && request.getFilter().isEmpty()
                    && request.getRequiredPartitions().isEmpty();
            metrics = newScanMetrics(session, handle, snapshot);
            scanMetrics = null;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            try {
                scanContext.checkCancelled();
                if (Thread.currentThread().isInterrupted()) {
                    throw new DorisConnectorException("Delta split planning interrupted for " + handle.getPath());
                }
                if (buffered != null) {
                    return true;
                }
                return runtime.execute(() -> {
                    if (partitionsPrunedToEmpty) {
                        metrics.complete();
                        close();
                        return false;
                    }
                    if (countPushdown && !countChecked) {
                        countChecked = true;
                        CountSummary summary = countSummary(session, handle);
                        countOnly = summary.count.isPresent();
                        if (countOnly) {
                            metrics.count(summary.files, summary.partitions);
                        }
                        if (countOnly && summary.first != null) {
                            buffered = summary.first.withRowCount(summary.count.getAsLong());
                            return true;
                        }
                    }
                    if (countOnly) {
                        metrics.complete();
                        close();
                        return false;
                    }
                    if (files == null) {
                        files = DeltaLakeScanFiles.open(snapshot, metrics.engine(runtime.engine(handle.reference())),
                                false, filter.kernelPredicate());
                    }
                    while (file == null || offset == file.getFileSize()) {
                        scanContext.checkCancelled();
                        if (!files.hasNext()) {
                            metrics.complete();
                            close();
                            return false;
                        }
                        Row row = files.next();
                        Map<String, String> partitions = DeltaLakePartitionValues.normalize(
                                snapshot, DeltaKernelCompat.partitionValues(row), zone);
                        if (!filter.partitionPredicate().test(partitions)) {
                            continue;
                        }
                        file = fileRange(row, handle, partitions, targetSize, fileSchema);
                        metrics.file(file.getPartitionValues());
                        offset = 0;
                    }
                    long length = Math.min(targetSize, file.getFileSize() - offset);
                    buffered = file.withByteRange(offset, length);
                    offset += length;
                    return true;
                });
            } catch (RuntimeException | Error e) {
                try {
                    close();
                } catch (RuntimeException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
        }

        @Override
        public ConnectorScanRange next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ConnectorScanRange result = buffered;
            buffered = null;
            metrics.split();
            return result;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            buffered = null;
            file = null;
            try {
                if (files != null) {
                    runtime.execute(() -> {
                        files.close();
                        return null;
                    });
                }
            } finally {
                scanMetrics = metrics.result();
            }
        }
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        scanMetrics = null;
        planningReuseHit = false;
        return runtime.execute(() -> {
            DeltaLakeScanPlanMemo.checkCancelled(request.getScanContext());
            DeltaLakeTableHandle handle = (DeltaLakeTableHandle) request.getTableHandle();
            if (handle.kind().systemTable != null) {
                return handle.kind().systemTable.planScan(session, runtime, handle, request);
            }
            Snapshot snapshot = handle.snapshot(runtime);
            if (session == null || !session.isExternalScanTaskReuseEnabled()) {
                return planScanInternal(session, request, handle, snapshot).ranges();
            }
            String namespace = DeltaLakeScanPlanMemo.NAMESPACE + ":" + session.getCatalogId()
                    + ":" + session.getQueryId();
            DeltaLakeScanPlanMemo memo = session.getStatementScope().computeIfAbsentForPlanning(
                    namespace, DeltaLakeScanPlanMemo::new);
            DeltaLakeScanPlanMemo.Outcome outcome = memo.get(
                    DeltaLakeScanPlanMemo.Key.of(session, request, targetSplitSize(session)), request.getScanContext(),
                    () -> planScanInternal(session, request, handle, snapshot));
            planningReuseHit = outcome.reused();
            scanMetrics = outcome.plan().metrics();
            return outcome.plan().ranges();
        });
    }

    private DeltaLakeScanPlanMemo.Plan planScanInternal(ConnectorSession session, ConnectorScanRequest request,
            DeltaLakeTableHandle handle, Snapshot snapshot) {
        DeltaLakeScanMetrics metrics = newScanMetrics(session, handle, snapshot);
        try {
            List<ConnectorScanRange> ranges = planRanges(session, request, handle, snapshot, metrics);
            DeltaLakeScanPlanMemo.checkCancelled(request.getScanContext());
            metrics.complete();
            return new DeltaLakeScanPlanMemo.Plan(ranges, metrics.result());
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to plan Delta scan files for " + handle.getPath(), e);
        } finally {
            scanMetrics = metrics.result();
        }
    }

    private List<ConnectorScanRange> planRanges(ConnectorSession session, ConnectorScanRequest request,
            DeltaLakeTableHandle handle, Snapshot snapshot, DeltaLakeScanMetrics metrics) throws IOException {
        if (request.isPartitionsPrunedToEmpty()) {
            return Collections.emptyList();
        }
        long targetSize = targetSplitSize(session);
        if (request.isCountPushdown() && request.getFilter().isEmpty()
                && request.getRequiredPartitions().isEmpty()) {
            CountSummary summary = countSummary(session, handle);
            if (summary.count.isPresent()) {
                metrics.count(summary.files, summary.partitions);
                if (summary.first == null) {
                    return Collections.emptyList();
                }
                metrics.split();
                return Collections.singletonList(summary.first.withRowCount(summary.count.getAsLong()));
            }
        }
        List<ConnectorScanRange> ranges = new ArrayList<>();
        FileSchema fileSchema = fileSchema(snapshot);
        ZoneId zone = DeltaLakePartitionValues.sessionZone(session);
        ScanFilter filter = scanFilter(zone, snapshot, request);
        Engine engine = metrics.engine(runtime.engine(handle.reference()));
        DeltaLakeScanFiles.forEach(snapshot, engine, false, filter.kernelPredicate(), row -> {
            DeltaLakeScanPlanMemo.checkCancelled(request.getScanContext());
            Map<String, String> partitions = DeltaLakePartitionValues.normalize(
                    snapshot, DeltaKernelCompat.partitionValues(row), zone);
            if (!filter.partitionPredicate().test(partitions)) {
                return;
            }
            DeltaLakeScanRange file = fileRange(row, handle, partitions, targetSize, fileSchema);
            metrics.file(partitions);
            for (long offset = 0; offset < file.getFileSize();) {
                DeltaLakeScanPlanMemo.checkCancelled(request.getScanContext());
                long length = Math.min(targetSize, file.getFileSize() - offset);
                ranges.add(file.withByteRange(offset, length));
                metrics.split();
                offset += length;
            }
        });
        return ranges;
    }

    private DeltaLakeScanRange fileRange(Row row, DeltaLakeTableHandle handle,
            Map<String, String> partitions, long targetSize, FileSchema fileSchema) {
        FileStatus file = DeltaKernelCompat.fileStatus(row);
        if (file.getSize() <= 0) {
            throw new DorisConnectorException("Invalid Delta Parquet file size for " + file.getPath());
        }
        String path = runtime.storageContext.getStorageContext().normalizeStorageUri(file.getPath());
        TDeltaLakeFileDesc descriptor = new TDeltaLakeFileDesc();
        if (fileSchema.mapped()) {
            descriptor.setSchemaId(DeltaLakeSchemaBuilder.FILE_SCHEMA_ID);
        }
        DeltaKernelCompat.deletionVector(row, handle.getPath()).ifPresent(dv -> {
            if (dv.isSetPath()) {
                dv.setPath(runtime.storageContext.getStorageContext().normalizeStorageUri(dv.getPath()));
            }
            descriptor.setDeletionVector(dv);
        });
        return new DeltaLakeScanRange(path, 0, file.getSize(), file.getSize(), file.getModificationTime(),
                targetSize, partitions, descriptor, fileSchema.varbinaryPartitions());
    }

    private ScanFilter scanFilter(ZoneId zone, Snapshot snapshot, ConnectorScanRequest request) {
        Set<String> required = new HashSet<>(request.getRequiredPartitions());
        java.util.function.Predicate<Map<String, String>> selected = values -> required.isEmpty()
                || required.contains(DeltaLakePartitionValues.partitionName(values));
        if (request.getFilter().isEmpty()) {
            return new ScanFilter(Optional.empty(), selected);
        }
        ConnectorExpression expression = request.getFilter().get();
        DeltaLakePredicateConverter converter = new DeltaLakePredicateConverter(snapshot.getSchema(), zone,
                snapshot.getPartitionColumnNames(), runtime.partitionColumnsToNormalize(snapshot));
        return new ScanFilter(converter.convert(expression),
                converter.normalizedPartitionFilter(expression, runtime.engine).map(selected::and).orElse(selected));
    }

    @Override
    public boolean canServeMetadataOnlyCount(ConnectorSession session, ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter) {
        DeltaLakeTableHandle delta = (DeltaLakeTableHandle) handle;
        return delta.kind() == DeltaLakeTableKind.DATA && filter.isEmpty()
                && runtime.execute(() -> countSummary(session, delta).count.isPresent());
    }

    private CountSummary countSummary(ConnectorSession session, DeltaLakeTableHandle handle) {
        DeltaLakeStatementScope.FileCount observedCount = DeltaLakeStatementScope.fileCount(session, handle);
        ZoneId zone = DeltaLakePartitionValues.sessionZone(session);
        return ConnectorStatementScopes.resolveInStatement(session, "deltalake.count", handle.getDatabase(),
                handle.getTable() + ":" + handle.getTableId() + ":" + handle.getVersion() + ":" + zone.getId(),
                () -> runtime.execute(() -> {
                    Snapshot snapshot = handle.snapshot(runtime);
                    boolean partitioned = !snapshot.getPartitionColumnNames().isEmpty();
                    DeltaLakeRowCount count = new DeltaLakeRowCount();
                    long[] fileCount = {0};
                    Set<Map<String, String>> partitions = new HashSet<>();
                    DeltaLakeScanRange[] first = {null};
                    LongConsumer fileCountObserver = statistics.fileCountObserver(session, handle, runtime);
                    DeltaLakeScanFiles.forEach(snapshot, runtime.engine(handle.reference()), true, row -> {
                        fileCount[0]++;
                        Map<String, String> values = partitioned ? DeltaLakePartitionValues.normalize(
                                snapshot, DeltaKernelCompat.partitionValues(row), zone) : Collections.emptyMap();
                        if (!values.isEmpty()) {
                            partitions.add(values);
                        }
                        if (first[0] == null) {
                            first[0] = fileRange(row, handle, values, targetSplitSize(session), fileSchema(snapshot));
                        }
                        count.add(row);
                    });
                    observedCount.value = fileCount[0];
                    fileCountObserver.accept(fileCount[0]);
                    return new CountSummary(count.value(), first[0],
                            fileCount[0], partitions.size());
                }));
    }

    private static final class CountSummary {
        private final OptionalLong count;
        private final DeltaLakeScanRange first;
        private final long files;
        private final long partitions;

        private CountSummary(OptionalLong count, DeltaLakeScanRange first, long files, long partitions) {
            this.count = count;
            this.first = first;
            this.files = files;
            this.partitions = partitions;
        }
    }

    @Override
    public Map<String, String> getScanNodeProperties(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
        return runtime.execute(() -> {
            DeltaLakeTableHandle delta = (DeltaLakeTableHandle) handle;
            if (delta.kind().systemTable != null) {
                String name = delta.kind().systemTable.name();
                return Map.of(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "parquet",
                        ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS, "Native Delta Lake " + name + " reads",
                        "deltalake.system_table", name,
                        "deltalake.snapshot_version", String.valueOf(delta.getVersion()));
            }
            Snapshot snapshot = delta.snapshot(runtime);
            Map<String, String> properties = new HashMap<>();
            properties.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "parquet");
            properties.put("deltalake.snapshot_version", String.valueOf(snapshot.getVersion()));
            long files = knownFileCount(session, delta, snapshot).value;
            properties.put("deltalake.total_files", files < 0 ? "unknown" : Long.toString(files));
            TDeltaLakeColumnMappingMode mode = DeltaLakeSchemaBuilder.columnMappingMode(snapshot);
            if (mode != TDeltaLakeColumnMappingMode.NONE) {
                properties.put(SCHEMA_MAPPING, DeltaLakeSchemaBuilder.encodeMapping(snapshot.getSchema(), mode));
            }
            properties.put(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS, "Native Delta Lake reads");
            properties.put(ScanNodePropertyKeys.PATH_PARTITION_KEYS,
                    String.join(",", snapshot.getPartitionColumnNames()));
            runtime.backendProperties(delta.reference())
                    .forEach((key, value) -> properties.put(ScanNodePropertyKeys.LOCATION_PREFIX + key, value));
            return properties;
        });
    }

    @Override
    public void populateScanLevelParams(TFileScanRangeParams params, Map<String, String> properties) {
        DeltaLakeSchemaBuilder.applyMapping(params, properties.get(SCHEMA_MAPPING));
    }

    @Override
    public boolean supportsFileCache() {
        return true;
    }

    @Override
    public List<String> getDeleteFiles(TTableFormatFileDesc tableFormatParams) {
        if (tableFormatParams == null || !tableFormatParams.isSetDeltalakeParams()) {
            return List.of();
        }
        TDeltaLakeDeletionVectorDesc dv = tableFormatParams.getDeltalakeParams().getDeletionVector();
        return dv != null && dv.isSetPath() ? List.of(dv.getPath()) : List.of();
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> properties) {
        String systemTable = properties.get("deltalake.system_table");
        if (systemTable != null) {
            DeltaLakeSystemTable view = DeltaLakeTableKind.systemTableNamed(systemTable).orElseThrow().systemTable;
            output.append(prefix).append("Delta ").append(systemTable)
                    .append(view.supportsTimeTravel() ? " at version: " : " through version: ")
                    .append(properties.get("deltalake.snapshot_version")).append('\n');
            return;
        }
        output.append(prefix).append("Delta snapshot: ").append(properties.get("deltalake.snapshot_version"))
                .append('\n');
        DeltaLakeScanMetrics.Result result = scanMetrics;
        if (result == null) {
            // Streaming EXPLAIN must not eagerly enumerate the lazy source merely for diagnostics.
            output.append(prefix).append("Delta files: pending/")
                    .append(properties.get("deltalake.total_files"))
                    .append(", selected partitions: pending (streaming)\n");
        } else {
            result.explain(output, prefix);
        }
    }

    @Override
    public List<ConnectorScanProfile> collectScanProfiles(ConnectorSession session) {
        DeltaLakeScanMetrics.Result result = scanMetrics;
        return result == null ? Collections.emptyList() : Collections.singletonList(result.profile(planningReuseHit));
    }

    @Override
    public OptionalLong scannedPartitionCount(List<ConnectorScanRange> ranges) {
        return OptionalLong.of(ranges.stream().map(ConnectorScanRange::getPartitionValues)
                .filter(values -> !values.isEmpty()).distinct().count());
    }

    private static long targetSplitSize(ConnectorSession session) {
        String value = session == null ? null : session.getProperty("file_split_size", String.class);
        long configured = value == null || value.isEmpty() ? 0 : Long.parseLong(value);
        return configured > 0 ? configured : DEFAULT_SPLIT_SIZE;
    }
}
