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
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.thrift.TDeltaLakeSysTableColumn;
import org.apache.doris.thrift.TDeltaLakeSysTableField;
import org.apache.doris.thrift.TDeltaLakeSysTableRow;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

final class DeltaLakeFilesSystemTable extends DeltaLakeSnapshotSystemTable {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeFilesSystemTable.class);

    DeltaLakeFilesSystemTable() {
        super("files", new StructType()
                .add("path", StringType.STRING).add("size", LongType.LONG)
                .add("modification_time", TimestampType.TIMESTAMP)
                .add("partition_values", new MapType(StringType.STRING, StringType.STRING, true))
                .add("num_records", LongType.LONG).add("has_deletion_vector", BooleanType.BOOLEAN)
                .add("deletion_vector_cardinality", LongType.LONG).add("deletion_vector_path", StringType.STRING)
                .add("deletion_vector_storage_type", StringType.STRING));
    }

    @Override
    public long streamingSplitEstimate(ConnectorSession session) {
        String enabled = session == null ? null : session.getProperty("enable_external_table_batch_mode", String.class);
        // One source can deliver its first split without counting the active AddFiles up front.
        return enabled == null || Boolean.parseBoolean(enabled) ? 1 : -1;
    }

    @Override
    public ConnectorSplitSource streamSplits(ConnectorSession session, DeltaLakeRuntime runtime,
            DeltaLakeTableHandle handle, ConnectorScanRequest request, boolean countOnly) {
        return new Source(session, runtime, handle, request, projectedColumns(handle, request, countOnly), "stream");
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, DeltaLakeRuntime runtime,
            DeltaLakeTableHandle handle, ConnectorScanRequest request) {
        List<ConnectorScanRange> ranges = new ArrayList<>();
        try (Source source = new Source(session, runtime, handle, request, projectedColumns(handle, request), "sync")) {
            while (source.hasNext()) {
                ranges.add(source.next());
            }
        }
        return ranges;
    }

    private static final class Source implements ConnectorSplitSource {
        private final DeltaLakeRuntime runtime;
        private final DeltaLakeTableHandle handle;
        private final ConnectorScanRequest request;
        private final Snapshot snapshot;
        private final ZoneId zone;
        private final List<TDeltaLakeSysTableColumn> columns;
        private final boolean needStatus;
        private final boolean needDv;
        private final boolean needDvPath;
        private final CloseableIterator<Row> files;
        private final DeltaLakeSystemTableRows.Batches batches;
        private final String queryId;
        private final String mode;
        private final long startNanos = System.nanoTime();
        private long enumerated;
        private long reported;
        private long firstBatchFiles;
        private long firstBatchMillis;
        private long delivered;
        private boolean exhausted;
        private boolean closed;

        Source(ConnectorSession session, DeltaLakeRuntime runtime, DeltaLakeTableHandle handle,
                ConnectorScanRequest request, List<TDeltaLakeSysTableColumn> columns, String mode) {
            this.runtime = runtime;
            this.handle = handle;
            this.request = request;
            this.columns = columns;
            this.mode = mode;
            queryId = session == null ? "" : session.getQueryId();
            snapshot = handle.snapshot(runtime);
            zone = DeltaLakePartitionValues.sessionZone(session);
            needStatus = DeltaLakeSystemTableRows.needs(columns, "path")
                    || DeltaLakeSystemTableRows.needs(columns, "size")
                    || DeltaLakeSystemTableRows.needs(columns, "modification_time");
            needDv = columns.stream().anyMatch(c -> c.getName().contains("deletion_vector"));
            needDvPath = DeltaLakeSystemTableRows.needs(columns, "deletion_vector_path");
            files = DeltaLakeScanFiles.open(snapshot, runtime.engine(handle.reference()),
                    DeltaLakeSystemTableRows.needs(columns, "num_records"), Optional.empty());
            batches = new DeltaLakeSystemTableRows.Batches("files", columns, files.map(this::encode),
                    request.getScanContext());
        }

        private TDeltaLakeSysTableRow encode(Row row) {
            enumerated++;
            FileStatus status = needStatus ? DeltaKernelCompat.fileStatus(row) : null;
            DeltaKernelCompat.FileDeletionVector dv = needDv
                    ? DeltaKernelCompat.fileDeletionVector(row, handle.getPath(), needDvPath) : null;
            return DeltaLakeSystemTableRows.row(columns, col -> switch (col) {
                case "path" -> DeltaLakeSystemTableRows.text(normalizePath(status.getPath()));
                case "size" -> DeltaLakeSystemTableRows.number(status.getSize());
                case "modification_time" -> new TDeltaLakeSysTableField()
                        .setTimestampMillis(status.getModificationTime());
                case "partition_values" -> DeltaLakeSystemTableRows.map(DeltaLakePartitionValues.normalize(
                        snapshot, DeltaKernelCompat.partitionValues(row), zone));
                case "num_records" -> {
                    OptionalLong records = DeltaKernelCompat.numRecords(row);
                    yield records.isPresent() ? DeltaLakeSystemTableRows.number(records.getAsLong())
                            : new TDeltaLakeSysTableField();
                }
                case "has_deletion_vector" -> new TDeltaLakeSysTableField().setBoolVal(dv != null);
                case "deletion_vector_cardinality" -> DeltaLakeSystemTableRows.number(
                        dv == null ? 0 : dv.cardinality());
                case "deletion_vector_path" -> DeltaLakeSystemTableRows.text(
                        dv == null || dv.path() == null ? null : normalizePath(dv.path()));
                case "deletion_vector_storage_type" -> DeltaLakeSystemTableRows.text(
                        dv == null ? null : dv.storageType());
                default -> throw new IllegalArgumentException(col);
            });
        }

        private String normalizePath(String path) {
            return runtime.storageContext.getStorageContext().normalizeStorageUri(path);
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            try {
                if (runtime.execute(batches::hasNext)) {
                    return true;
                }
                exhausted = true;
                close();
                return false;
            } catch (RuntimeException | Error failure) {
                closeAfterFailure(failure);
                throw failure;
            }
        }

        @Override
        public ConnectorScanRange next() {
            if (closed) {
                throw new NoSuchElementException();
            }
            try {
                ConnectorScanRange range = runtime.execute(batches::next);
                if (delivered++ == 0) {
                    firstBatchFiles = enumerated;
                    firstBatchMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                }
                reportProgress();
                return range;
            } catch (RuntimeException | Error failure) {
                closeAfterFailure(failure);
                throw failure;
            }
        }

        private void reportProgress() {
            request.getScanContext().filesEnumerated(enumerated - reported);
            reported = enumerated;
        }

        private void closeAfterFailure(Throwable failure) {
            try {
                close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                reportProgress();
                runtime.execute(() -> {
                    files.close();
                    return null;
                });
                LOG.debug("Closed Delta files iterator query={} version={} mode={} firstBatchFiles={} "
                                + "enumeratedFiles={} deliveredSplits={} exhausted={} firstBatchMs={} elapsedMs={}",
                        queryId, handle.getVersion(), mode, firstBatchFiles, enumerated, delivered, exhausted,
                        firstBatchMillis, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
            }
        }
    }
}
