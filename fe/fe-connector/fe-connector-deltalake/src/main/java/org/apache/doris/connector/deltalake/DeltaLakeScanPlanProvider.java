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
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TDeltaLakeFileDesc;

import io.delta.kernel.Snapshot;
import io.delta.kernel.utils.FileStatus;

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
            Set<String> required = new HashSet<>(request.getRequiredPartitions());
            List<ConnectorScanRange> ranges = new ArrayList<>();
            DeltaLakeScanFiles.forEach(snapshot, runtime.engine, false, row -> {
                Map<String, String> partitions = DeltaLakePartitionValues.normalize(
                        snapshot, DeltaKernelCompat.partitionValues(row));
                if (!required.isEmpty() && !required.contains(DeltaLakePartitionValues.partitionName(partitions))) {
                    return;
                }
                FileStatus file = DeltaKernelCompat.fileStatus(row);
                if (file.getSize() <= 0) {
                    throw new DorisConnectorException("Invalid Delta Parquet file size for " + file.getPath());
                }
                String path = runtime.storageContext.getStorageContext().normalizeStorageUri(file.getPath());
                TDeltaLakeFileDesc descriptor = new TDeltaLakeFileDesc();
                DeltaKernelCompat.deletionVector(row, handle.getPath()).ifPresent(dv -> {
                    if (dv.isSetPath()) {
                        dv.setPath(runtime.storageContext.getStorageContext().normalizeStorageUri(dv.getPath()));
                    }
                    descriptor.setDeletionVector(dv);
                });
                for (long offset = 0; offset < file.getSize();) {
                    long length = Math.min(targetSize, file.getSize() - offset);
                    ranges.add(new DeltaLakeScanRange(path, offset, length, file.getSize(),
                            file.getModificationTime(), targetSize, partitions, descriptor));
                    offset += length;
                }
            });
            return ranges;
        });
    }

    @Override
    public Map<String, String> getScanNodeProperties(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
        return runtime.execute(() -> {
            Snapshot snapshot = ((DeltaLakeTableHandle) handle).snapshot(runtime.engine);
            Map<String, String> properties = new HashMap<>();
            properties.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "parquet");
            properties.put(ScanNodePropertyKeys.PATH_PARTITION_KEYS,
                    String.join(",", snapshot.getPartitionColumnNames()));
            runtime.storageContext.getStorageContext().getBackendStorageProperties()
                    .forEach((key, value) -> properties.put(ScanNodePropertyKeys.LOCATION_PREFIX + key, value));
            return properties;
        });
    }

    @Override
    public boolean supportsFileCache() {
        return true;
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
