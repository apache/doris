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
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanProfile;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TDeltaLakeFileDesc;
import org.apache.doris.thrift.TFileScanRangeParams;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

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

final class DeltaLakeScanPlanProvider implements ConnectorScanPlanProvider {
    private static final String SCHEMA_MAPPING = "deltalake.schema_mapping";
    private static final long DEFAULT_SPLIT_SIZE = 128L << 20;
    private final DeltaLakeRuntime runtime;
    // The connector creates a provider per scan node. The gate runs before that node opens its source.
    private boolean streamingCount;
    private volatile DeltaLakeScanMetrics.Result scanMetrics;

    DeltaLakeScanPlanProvider(DeltaLakeRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public long streamingSplitEstimate(ConnectorSession session, ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter, boolean countPushdown) {
        streamingCount = false;
        String enabled = session.getProperty("enable_external_table_batch_mode", String.class);
        if (enabled != null && !Boolean.parseBoolean(enabled)) {
            return -1;
        }
        String configured = session.getProperty("num_files_in_batch_mode", String.class);
        long threshold = configured == null ? 10000 : Long.parseLong(configured);
        return runtime.execute(() -> {
            Snapshot snapshot = ((DeltaLakeTableHandle) handle).snapshot(runtime.engine);
            OptionalLong files = DeltaKernelCompat.fileCount(snapshot);
            // Old writers may not produce a checksum. Unknown size streams conservatively instead
            // of enumerating all AddFiles merely to decide whether enumeration should be lazy.
            if (files.isPresent() && files.getAsLong() < threshold) {
                return -1L;
            }
            streamingCount = countPushdown && filter.isEmpty();
            return files.orElse(threshold);
        });
    }

    @Override
    public ConnectorSplitSource streamSplits(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter, long limit) {
        return runtime.execute(() -> new StreamingSource(session, (DeltaLakeTableHandle) handle, filter,
                streamingCount && filter.isEmpty()));
    }

    private final class StreamingSource implements ConnectorSplitSource {
        private final ConnectorSession session;
        private final DeltaLakeTableHandle handle;
        private final Snapshot snapshot;
        private final Optional<Predicate> filter;
        private final long targetSize;
        private final boolean mapped;
        private final boolean countPushdown;
        private final DeltaLakeScanMetrics metrics;
        private CloseableIterator<Row> files;
        private DeltaLakeScanRange file;
        private DeltaLakeScanRange buffered;
        private long offset;
        private boolean countChecked;
        private boolean countOnly;
        private boolean closed;

        private StreamingSource(ConnectorSession session, DeltaLakeTableHandle handle,
                Optional<ConnectorExpression> filter, boolean countPushdown) {
            this.session = session;
            this.handle = handle;
            snapshot = handle.snapshot(runtime.engine);
            DeltaLakeFeatureCheck.check(snapshot);
            this.filter = predicate(session, snapshot, filter);
            targetSize = targetSplitSize(session);
            mapped = DeltaLakeSchemaBuilder.hasColumnMapping(snapshot);
            this.countPushdown = countPushdown;
            metrics = new DeltaLakeScanMetrics(session, handle, snapshot);
            scanMetrics = null;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            try {
                if (Thread.currentThread().isInterrupted()) {
                    throw new DorisConnectorException("Delta split planning interrupted for " + handle.getPath());
                }
                if (buffered != null) {
                    return true;
                }
                return runtime.execute(() -> {
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
                        files = DeltaLakeScanFiles.open(snapshot, metrics.engine(runtime.engine), false, filter);
                    }
                    if (file == null || offset == file.getFileSize()) {
                        if (!files.hasNext()) {
                            metrics.complete();
                            close();
                            return false;
                        }
                        Row row = files.next();
                        file = fileRange(row, handle, DeltaLakePartitionValues.normalize(
                                snapshot, DeltaKernelCompat.partitionValues(row)), targetSize, mapped);
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
        return runtime.execute(() -> {
            DeltaLakeTableHandle handle = (DeltaLakeTableHandle) request.getTableHandle();
            Snapshot snapshot = handle.snapshot(runtime.engine);
            DeltaLakeFeatureCheck.check(snapshot);
            DeltaLakeScanMetrics metrics = new DeltaLakeScanMetrics(session, handle, snapshot);
            scanMetrics = null;
            try {
                if (request.isPartitionsPrunedToEmpty()) {
                    metrics.complete();
                    return Collections.emptyList();
                }
                long targetSize = targetSplitSize(session);
                boolean mapped = DeltaLakeSchemaBuilder.hasColumnMapping(snapshot);
                if (request.isCountPushdown() && request.getFilter().isEmpty()
                        && request.getRequiredPartitions().isEmpty()) {
                    CountSummary summary = countSummary(session, handle);
                    if (summary.count.isPresent()) {
                        metrics.count(summary.files, summary.partitions);
                        metrics.complete();
                        if (summary.first == null) {
                            return Collections.emptyList();
                        }
                        metrics.split();
                        return Collections.singletonList(summary.first.withRowCount(summary.count.getAsLong()));
                    }
                }
                Set<String> required = new HashSet<>(request.getRequiredPartitions());
                List<ConnectorScanRange> ranges = new ArrayList<>();
                Optional<Predicate> predicate = predicate(session, snapshot, request.getFilter());
                DeltaLakeScanFiles.forEach(snapshot, metrics.engine(runtime.engine), false, predicate, row -> {
                    Map<String, String> partitions = DeltaLakePartitionValues.normalize(
                            snapshot, DeltaKernelCompat.partitionValues(row));
                    if (!required.isEmpty() && !required.contains(DeltaLakePartitionValues.partitionName(partitions))) {
                        return;
                    }
                    DeltaLakeScanRange file = fileRange(row, handle, partitions, targetSize, mapped);
                    metrics.file(partitions);
                    for (long offset = 0; offset < file.getFileSize();) {
                        if (Thread.currentThread().isInterrupted()) {
                            throw new DorisConnectorException(
                                    "Delta split planning interrupted for " + handle.getPath());
                        }
                        long length = Math.min(targetSize, file.getFileSize() - offset);
                        ranges.add(file.withByteRange(offset, length));
                        metrics.split();
                        offset += length;
                    }
                });
                metrics.complete();
                return ranges;
            } finally {
                scanMetrics = metrics.result();
            }
        });
    }

    private DeltaLakeScanRange fileRange(Row row, DeltaLakeTableHandle handle,
            Map<String, String> partitions, long targetSize, boolean mapped) {
        FileStatus file = DeltaKernelCompat.fileStatus(row);
        if (file.getSize() <= 0) {
            throw new DorisConnectorException("Invalid Delta Parquet file size for " + file.getPath());
        }
        String path = runtime.storageContext.getStorageContext().normalizeStorageUri(file.getPath());
        TDeltaLakeFileDesc descriptor = new TDeltaLakeFileDesc();
        if (mapped) {
            descriptor.setSchemaId(DeltaLakeSchemaBuilder.FILE_SCHEMA_ID);
        }
        DeltaKernelCompat.deletionVector(row, handle.getPath()).ifPresent(dv -> {
            if (dv.isSetPath()) {
                dv.setPath(runtime.storageContext.getStorageContext().normalizeStorageUri(dv.getPath()));
            }
            descriptor.setDeletionVector(dv);
        });
        return new DeltaLakeScanRange(path, 0, file.getSize(), file.getSize(), file.getModificationTime(),
                targetSize, partitions, descriptor);
    }

    private static Optional<Predicate> predicate(ConnectorSession session, Snapshot snapshot,
            Optional<ConnectorExpression> filter) {
        return filter.flatMap(expression -> new DeltaLakePredicateConverter(snapshot.getSchema(),
                ZoneId.of(session.getTimeZone())).convert(expression));
    }

    @Override
    public boolean canServeMetadataOnlyCount(ConnectorSession session, ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter) {
        return filter.isEmpty() && runtime.execute(() -> countSummary(session, (DeltaLakeTableHandle) handle)
                .count.isPresent());
    }

    private CountSummary countSummary(ConnectorSession session, DeltaLakeTableHandle handle) {
        DeltaLakeStatementScope.FileCount observedCount = DeltaLakeStatementScope.fileCount(session, handle);
        return ConnectorStatementScopes.resolveInStatement(session, "deltalake.count", handle.getDatabase(),
                handle.getTable() + ":" + handle.getTableId() + ":" + handle.getVersion(), () -> runtime.execute(() -> {
                    Snapshot snapshot = handle.snapshot(runtime.engine);
                    DeltaLakeFeatureCheck.check(snapshot);
                    boolean partitioned = !snapshot.getPartitionColumnNames().isEmpty();
                    long[] count = {0};
                    long[] fileCount = {0};
                    Set<Map<String, String>> partitions = new HashSet<>();
                    boolean[] complete = {true};
                    DeltaLakeScanRange[] first = {null};
                    DeltaLakeScanFiles.forEach(snapshot, runtime.engine, true, row -> {
                        fileCount[0]++;
                        Map<String, String> values = partitioned ? DeltaLakePartitionValues.normalize(
                                snapshot, DeltaKernelCompat.partitionValues(row)) : Collections.emptyMap();
                        if (!values.isEmpty()) {
                            partitions.add(values);
                        }
                        if (first[0] == null) {
                            first[0] = fileRange(row, handle, values, targetSplitSize(session),
                                    DeltaLakeSchemaBuilder.hasColumnMapping(snapshot));
                        }
                        OptionalLong records = DeltaKernelCompat.numRecords(row);
                        long deleted = DeltaKernelCompat.deletionCardinality(row);
                        if (records.isEmpty()) {
                            complete[0] = false;
                        } else {
                            if (deleted < 0 || records.getAsLong() < deleted) {
                                throw new DorisConnectorException("Invalid Delta row count or deletion cardinality for "
                                        + DeltaKernelCompat.fileStatus(row).getPath());
                            }
                            count[0] = Math.addExact(count[0], records.getAsLong() - deleted);
                        }
                    });
                    observedCount.value = fileCount[0];
                    return new CountSummary(complete[0] ? OptionalLong.of(count[0]) : OptionalLong.empty(), first[0],
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
            Snapshot snapshot = ((DeltaLakeTableHandle) handle).snapshot(runtime.engine);
            Map<String, String> properties = new HashMap<>();
            properties.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "parquet");
            properties.put("deltalake.snapshot_version", String.valueOf(snapshot.getVersion()));
            long files = DeltaKernelCompat.fileCount(snapshot).orElse(
                    DeltaLakeStatementScope.fileCount(session, (DeltaLakeTableHandle) handle).value);
            properties.put("deltalake.total_files", files < 0 ? "unknown" : Long.toString(files));
            if (DeltaLakeSchemaBuilder.hasColumnMapping(snapshot)) {
                properties.put(SCHEMA_MAPPING, DeltaLakeSchemaBuilder.encodeMapping(snapshot.getSchema()));
            }
            properties.put(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS, "Native Delta Lake reads");
            properties.put(ScanNodePropertyKeys.PATH_PARTITION_KEYS,
                    String.join(",", snapshot.getPartitionColumnNames()));
            runtime.storageContext.getStorageContext().getBackendStorageProperties()
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
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> properties) {
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
        return result == null ? Collections.emptyList() : Collections.singletonList(result.profile());
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
