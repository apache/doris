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
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TDeltaLakeFileDesc;
import org.apache.doris.thrift.TFileScanRangeParams;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.utils.FileStatus;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

final class DeltaLakeScanPlanProvider implements ConnectorScanPlanProvider {
    private static final String SCHEMA_MAPPING = "deltalake.schema_mapping";
    private static final long DEFAULT_SPLIT_SIZE = 128L << 20;
    private final DeltaLakeRuntime runtime;

    DeltaLakeScanPlanProvider(DeltaLakeRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        if (request.isPartitionsPrunedToEmpty()) {
            return Collections.emptyList();
        }
        return runtime.execute(() -> {
            DeltaLakeTableHandle handle = (DeltaLakeTableHandle) request.getTableHandle();
            Snapshot snapshot = handle.snapshot(runtime.engine);
            DeltaLakeFeatureCheck.check(snapshot);
            long targetSize = targetSplitSize(session);
            boolean mapped = DeltaLakeSchemaBuilder.hasColumnMapping(snapshot);
            if (request.isCountPushdown() && request.getFilter().isEmpty()
                    && request.getRequiredPartitions().isEmpty()) {
                CountSummary summary = countSummary(session, handle);
                if (summary.count.isPresent()) {
                    return summary.first == null ? Collections.emptyList()
                            : Collections.singletonList(summary.first.withRowCount(summary.count.getAsLong()));
                }
            }
            Set<String> required = new HashSet<>(request.getRequiredPartitions());
            List<ConnectorScanRange> ranges = new ArrayList<>();
            Optional<Predicate> predicate = predicate(session, snapshot, request.getFilter());
            DeltaLakeScanFiles.forEach(snapshot, runtime.engine, false, predicate, row -> {
                Map<String, String> partitions = DeltaLakePartitionValues.normalize(
                        snapshot, DeltaKernelCompat.partitionValues(row));
                if (!required.isEmpty() && !required.contains(DeltaLakePartitionValues.partitionName(partitions))) {
                    return;
                }
                DeltaLakeScanRange file = fileRange(row, handle, partitions, targetSize, mapped);
                for (long offset = 0; offset < file.getFileSize();) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new DorisConnectorException("Delta split planning interrupted for " + handle.getPath());
                    }
                    long length = Math.min(targetSize, file.getFileSize() - offset);
                    ranges.add(file.withByteRange(offset, length));
                    offset += length;
                }
            });
            return ranges;
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
        return ConnectorStatementScopes.resolveInStatement(session, "deltalake.count", handle.getDatabase(),
                handle.getTable() + ":" + handle.getTableId() + ":" + handle.getVersion(), () -> runtime.execute(() -> {
                    Snapshot snapshot = handle.snapshot(runtime.engine);
                    DeltaLakeFeatureCheck.check(snapshot);
                    long[] count = {0};
                    boolean[] complete = {true};
                    DeltaLakeScanRange[] first = {null};
                    DeltaLakeScanFiles.forEach(snapshot, runtime.engine, true, row -> {
                        if (first[0] == null) {
                            first[0] = fileRange(row, handle, DeltaLakePartitionValues.normalize(
                                    snapshot, DeltaKernelCompat.partitionValues(row)), targetSplitSize(session),
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
                    return new CountSummary(complete[0] ? OptionalLong.of(count[0]) : OptionalLong.empty(), first[0]);
                }));
    }

    private static final class CountSummary {
        private final OptionalLong count;
        private final DeltaLakeScanRange first;

        private CountSummary(OptionalLong count, DeltaLakeScanRange first) {
            this.count = count;
            this.first = first;
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
