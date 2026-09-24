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

package org.apache.doris.fluss;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.SortMergeReader;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.KeyValueRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

/**
 * Fluss's bounded primary-key reader with construction-time resource ownership made explicit.
 *
 * <p>Fluss 1.0 creates the asynchronous snapshot reader before it creates and subscribes the log
 * reader. If that later operation fails, its public constructor never returns and therefore no
 * caller can close the snapshot reader. Keep this compatibility copy until the pinned Fluss client
 * provides failure-atomic construction itself.
 */
final class SafeKvSnapshotAndLogBatchScanner implements BatchScanner {

    private final TableBucket tableBucket;
    private final long logStoppingOffset;
    private final int[] keyIndexesInScanRow;
    @Nullable private final int[] adjustProjectedFields;
    private final Comparator<InternalRow> primaryKeyComparator;
    private final Map<InternalRow, KeyValueRow> logRows;

    @Nullable private final BatchScanner snapshotScanner;
    @Nullable private final LogScanner logScanner;

    private boolean logScanFinished;
    private boolean finished;
    private boolean closed;

    @Nullable private CloseableIterator<LogRecord> snapshotRecordIterator;
    @Nullable private SortMergeReader sortMergeReader;
    @Nullable private CloseableIterator<InternalRow> sortMergeIterator;

    SafeKvSnapshotAndLogBatchScanner(
            Table table,
            TableBucket tableBucket,
            long snapshotId,
            long logStartingOffset,
            long logStoppingOffset,
            @Nullable int[] projectedFields) {
        Preconditions.checkArgument(
                table.getTableInfo().hasPrimaryKey(),
                "SafeKvSnapshotAndLogBatchScanner only supports primary-key tables.");
        this.tableBucket = tableBucket;
        this.logStoppingOffset = logStoppingOffset;

        ProjectionPlan projectionPlan = createProjectionPlan(table.getTableInfo(), projectedFields);
        this.keyIndexesInScanRow = projectionPlan.keyIndexesInScanRow;
        this.adjustProjectedFields = projectionPlan.adjustProjectedFields;
        this.primaryKeyComparator = createPrimaryKeyComparator(table.getTableInfo());
        this.logRows = new TreeMap<>(primaryKeyComparator);

        ScannerResources resources = acquireScanners(
                new TableScannerFactory(table), tableBucket, snapshotId, logStartingOffset,
                logStoppingOffset, projectionPlan.scanProjectedFields);
        this.snapshotScanner = resources.snapshotScanner;
        this.logScanner = resources.logScanner;
        this.logScanFinished = resources.logScanner == null;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (closed || finished) {
            return null;
        }

        if (!logScanFinished) {
            pollLogRecords(timeout);
            return CloseableIterator.emptyIterator();
        }

        if (sortMergeReader == null) {
            CloseableIterator<LogRecord> snapshotRecords = createSnapshotRecordIterator(timeout);
            sortMergeReader = new SortMergeReader(
                    adjustProjectedFields,
                    keyIndexesInScanRow,
                    snapshotRecords,
                    primaryKeyComparator,
                    CloseableIterator.wrap(logRows.values().iterator()));
        }

        try {
            sortMergeIterator = sortMergeReader.readBatch();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        if (sortMergeIterator == null) {
            finished = true;
        }
        return sortMergeIterator;
    }

    private void pollLogRecords(Duration timeout) {
        ScanRecords scanRecords = logScanner.poll(timeout);
        for (ScanRecord scanRecord : scanRecords.records(tableBucket)) {
            long logOffset = scanRecord.logOffset();
            if (logOffset >= logStoppingOffset) {
                logScanFinished = true;
                break;
            }

            reduceLogRecord(scanRecord);
            if (logOffset >= logStoppingOffset - 1) {
                logScanFinished = true;
                break;
            }
        }

        Long consumedUpToOffset = scanRecords.consumedUpToOffset(tableBucket);
        if (consumedUpToOffset != null && consumedUpToOffset >= logStoppingOffset) {
            logScanFinished = true;
        }
    }

    private void reduceLogRecord(ScanRecord scanRecord) {
        ChangeType changeType = scanRecord.getChangeType();
        boolean isDelete = changeType == ChangeType.DELETE || changeType == ChangeType.UPDATE_BEFORE;
        KeyValueRow keyValueRow = new KeyValueRow(keyIndexesInScanRow, scanRecord.getRow(), isDelete);
        logRows.put(keyValueRow.keyRow(), keyValueRow);
    }

    private CloseableIterator<LogRecord> createSnapshotRecordIterator(Duration timeout) {
        if (snapshotScanner == null) {
            snapshotRecordIterator = CloseableIterator.emptyIterator();
        } else {
            snapshotRecordIterator = new SnapshotRecordIterator(snapshotScanner, timeout);
        }
        return snapshotRecordIterator;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.closeQuietly(sortMergeIterator);
        IOUtils.closeQuietly(snapshotRecordIterator);
        IOUtils.closeQuietly(snapshotScanner);
        IOUtils.closeQuietly(logScanner);
    }

    private static ProjectionPlan createProjectionPlan(
            TableInfo tableInfo, @Nullable int[] projectedFields) {
        return ProjectionPlan.create(
                tableInfo.getRowType().getFieldCount(),
                getPhysicalPrimaryKeyIndexes(tableInfo),
                projectedFields);
    }

    private static Comparator<InternalRow> createPrimaryKeyComparator(TableInfo tableInfo) {
        int[] physicalPrimaryKeyIndexes = getPhysicalPrimaryKeyIndexes(tableInfo);
        RowType primaryKeyRowType =
                Schema.getKeyRowType(tableInfo.getSchema(), physicalPrimaryKeyIndexes);
        KeyEncoder primaryKeyEncoder = KeyEncoder.ofPrimaryKeyEncoder(
                primaryKeyRowType,
                tableInfo.getPhysicalPrimaryKeys(),
                tableInfo.getTableConfig(),
                tableInfo.isDefaultBucketKey());
        return (row1, row2) -> {
            byte[] key1 = primaryKeyEncoder.encodeKey(row1);
            byte[] key2 = primaryKeyEncoder.encodeKey(row2);
            return MemorySegment.wrap(key1)
                    .compare(MemorySegment.wrap(key2), 0, 0, key1.length, key2.length);
        };
    }

    private static int[] getPhysicalPrimaryKeyIndexes(TableInfo tableInfo) {
        return tableInfo.getPhysicalPrimaryKeys().stream()
                .mapToInt(primaryKey -> tableInfo.getRowType().getFieldIndex(primaryKey))
                .toArray();
    }

    /** Acquires both readers as one transaction so a failure cannot strand the first one. */
    static ScannerResources acquireScanners(
            ScannerFactory factory,
            TableBucket tableBucket,
            long snapshotId,
            long logStartingOffset,
            long logStoppingOffset,
            int[] projectedFields) {
        // Allocate the holder first, then finish every fallible log operation before starting the
        // asynchronous snapshot reader. Fluss 1.0's KvSnapshotBatchScanner.close() neither joins its
        // initializer nor prevents that initializer from publishing a SnapshotFilesReader afterwards;
        // consequently it cannot safely be rolled back when a later log subscription fails. Making the
        // snapshot acquisition the final operation removes that rollback state entirely.
        ScannerResources resources = new ScannerResources();
        try {
            boolean emptyLogRange = logStartingOffset >= logStoppingOffset || logStoppingOffset <= 0;
            if (!emptyLogRange) {
                resources.logScanner = factory.createLogScanner(projectedFields);
                Long partitionId = tableBucket.getPartitionId();
                if (partitionId == null) {
                    resources.logScanner.subscribe(tableBucket.getBucket(), logStartingOffset);
                } else {
                    resources.logScanner.subscribe(
                            partitionId, tableBucket.getBucket(), logStartingOffset);
                }
            }

            if (snapshotId >= 0) {
                // Keep this last. Once createSnapshotScanner returns, only a field assignment and return
                // remain, neither of which can strand the asynchronously initializing reader.
                resources.snapshotScanner =
                        factory.createSnapshotScanner(tableBucket, snapshotId, projectedFields);
            }
            return resources;
        } catch (RuntimeException | Error failure) {
            // The snapshot call is last and assigns only after it returns, so a failure reaching here can
            // own at most the synchronous log scanner. Do not pretend the SDK's snapshot close is a safe
            // cancellation primitive; it is not in Fluss 1.0.
            closeAfterFailure(resources.logScanner, failure);
            throw failure;
        }
    }

    private static void closeAfterFailure(AutoCloseable closeable, Throwable failure) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    interface ScannerFactory {
        BatchScanner createSnapshotScanner(
                TableBucket tableBucket, long snapshotId, int[] projectedFields);

        LogScanner createLogScanner(int[] projectedFields);
    }

    static final class ScannerResources {
        @Nullable private BatchScanner snapshotScanner;
        @Nullable private LogScanner logScanner;

        private ScannerResources() {
        }
    }

    private static final class TableScannerFactory implements ScannerFactory {
        private final Table table;

        private TableScannerFactory(Table table) {
            this.table = table;
        }

        @Override
        public BatchScanner createSnapshotScanner(
                TableBucket tableBucket, long snapshotId, int[] projectedFields) {
            return table.newScan()
                    .project(projectedFields)
                    .createBatchScanner(tableBucket, snapshotId);
        }

        @Override
        public LogScanner createLogScanner(int[] projectedFields) {
            return table.newScan().project(projectedFields).createLogScanner();
        }
    }

    private static final class ProjectionPlan {
        private final int[] scanProjectedFields;
        private final int[] keyIndexesInScanRow;
        @Nullable private final int[] adjustProjectedFields;

        private ProjectionPlan(
                int[] scanProjectedFields,
                int[] keyIndexesInScanRow,
                @Nullable int[] adjustProjectedFields) {
            this.scanProjectedFields = scanProjectedFields;
            this.keyIndexesInScanRow = keyIndexesInScanRow;
            this.adjustProjectedFields = adjustProjectedFields;
        }

        private static ProjectionPlan create(
                int fieldCount, int[] primaryKeyIndexes, @Nullable int[] projectedFields) {
            if (projectedFields == null) {
                return new ProjectionPlan(
                        IntStream.range(0, fieldCount).toArray(), primaryKeyIndexes, null);
            }

            List<Integer> scanProjectedFields = Arrays.stream(projectedFields)
                    .boxed()
                    .collect(Collectors.toCollection(ArrayList::new));
            int[] keyIndexesInScanRow = new int[primaryKeyIndexes.length];
            for (int i = 0; i < primaryKeyIndexes.length; i++) {
                int primaryKeyIndex = primaryKeyIndexes[i];
                int indexInProjectedFields = findIndex(projectedFields, primaryKeyIndex);
                if (indexInProjectedFields >= 0) {
                    keyIndexesInScanRow[i] = indexInProjectedFields;
                } else {
                    scanProjectedFields.add(primaryKeyIndex);
                    keyIndexesInScanRow[i] = scanProjectedFields.size() - 1;
                }
            }

            int[] scanProjection = scanProjectedFields.stream()
                    .mapToInt(Integer::intValue)
                    .toArray();
            int[] adjustProjectedFields = new int[projectedFields.length];
            for (int i = 0; i < projectedFields.length; i++) {
                adjustProjectedFields[i] = findIndex(scanProjection, projectedFields[i]);
            }
            return new ProjectionPlan(scanProjection, keyIndexesInScanRow, adjustProjectedFields);
        }

        private static int findIndex(int[] array, int target) {
            for (int i = 0; i < array.length; i++) {
                if (array[i] == target) {
                    return i;
                }
            }
            return -1;
        }
    }

    private static final class SnapshotRecordIterator implements CloseableIterator<LogRecord> {
        private final BatchScanner scanner;
        private final Duration timeout;
        @Nullable private CloseableIterator<InternalRow> rows;
        private boolean snapshotFinished;

        private SnapshotRecordIterator(BatchScanner scanner, Duration timeout) {
            this.scanner = scanner;
            this.timeout = timeout;
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(rows);
            rows = null;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (rows != null) {
                    if (rows.hasNext()) {
                        return true;
                    }
                    rows.close();
                    rows = null;
                }

                if (snapshotFinished) {
                    return false;
                }

                try {
                    rows = scanner.pollBatch(timeout);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if (rows == null) {
                    snapshotFinished = true;
                    return false;
                }
            }
        }

        @Override
        public LogRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return new ScanRecord(rows.next());
        }
    }
}
