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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison.Operator;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TDeltaLakeHistoryJob;
import org.apache.doris.thrift.TDeltaLakeHistoryMapEntry;
import org.apache.doris.thrift.TDeltaLakeHistoryNotebook;
import org.apache.doris.thrift.TDeltaLakeHistoryRow;

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder.CommitBoundary;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

final class DeltaLakeHistoryReader {
    private static final int ROWS_PER_SPLIT = 1000;

    private DeltaLakeHistoryReader() {
    }

    static List<ConnectorScanRange> plan(DeltaLakeRuntime runtime, DeltaLakeTableHandle handle,
            ConnectorScanRequest request) {
        Bounds bounds = request.getFilter()
                .map(filter -> restrict(filter, new Bounds(0, handle.getVersion())))
                .orElseGet(() -> new Bounds(0, handle.getVersion()));
        if (bounds.start > bounds.end) {
            return List.of();
        }
        boolean details = request.getColumns().stream().anyMatch(column -> !(column instanceof NamedColumnHandle)
                || !isVersion(((NamedColumnHandle) column).getName())
                && !isTimestamp(((NamedColumnHandle) column).getName()));
        boolean timestamp = request.getColumns().stream().anyMatch(column -> !(column instanceof NamedColumnHandle)
                || isTimestamp(((NamedColumnHandle) column).getName()));
        Snapshot snapshot = handle.snapshot(runtime);
        DeltaLakeTableReference reference = handle.reference();
        Engine engine = runtime.engine(reference);
        try {
            OptionalLong earliest = DeltaKernelCompat.earliestHistoryVersion(engine, snapshot);
            if (earliest.isEmpty() || earliest.getAsLong() > bounds.end) {
                return List.of();
            }
            long first = Math.max(earliest.getAsLong(), bounds.start);
            // For a timestamp projection, earlier mtimes can move the selected timestamp forward
            // by a millisecond even when those earlier versions are filtered out.
            long rangeStart = timestamp ? earliest.getAsLong() : first;
            CommitRange range = reference.managed
                    ? runtime.resolver.managedCommitRange(engine, handle.getDatabase(), handle.getTable(), reference,
                            rangeStart, bounds.end)
                    : TableManager.loadCommitRange(reference.path, CommitBoundary.atVersion(rangeStart))
                            .withEndBoundary(CommitBoundary.atVersion(bounds.end)).build(engine);
            return readRange(engine, snapshot, range, first, details, timestamp, request.getScanContext());
        } catch (IOException e) {
            throw new DorisConnectorException("Cannot read Delta commit history for " + handle.getPath(), e);
        }
    }

    private static List<ConnectorScanRange> readRange(Engine engine, Snapshot snapshot, CommitRange range,
            long first, boolean details, boolean timestamp, ConnectorScanContext context) throws IOException {
        List<FileStatus> files = range.getDeltaFiles();
        int firstIndex = Math.toIntExact(first - range.getStartVersion());
        long[] mtimes = timestamp ? normalizedMtimes(files, firstIndex) : null;
        OptionalLong ictStart = timestamp ? DeltaKernelCompat.historyIctStart(snapshot) : OptionalLong.empty();
        List<ConnectorScanRange> splits = new ArrayList<>();
        List<TDeltaLakeHistoryRow> batch = new ArrayList<>(ROWS_PER_SPLIT);
        for (int index = files.size() - 1; index >= firstIndex; index--) {
            context.checkCancelled();
            long version = range.getStartVersion() + index;
            boolean usesIct = timestamp && ictStart.isPresent() && version >= ictStart.getAsLong();
            TDeltaLakeHistoryRow row = new TDeltaLakeHistoryRow().setVersion(version);
            if (timestamp) {
                row.setTimestampMillis(mtimes[index - firstIndex]);
            }
            if (details || usesIct) {
                StructType projection = details ? DeltaLakeHistorySchema.DETAIL_JSON
                        : DeltaLakeHistorySchema.TIMESTAMP_JSON;
                readCommitInfo(engine, files.get(index), projection, context, info -> {
                    if (details) {
                        fillDetails(info, row);
                    }
                    int ordinal = info.getSchema().indexOf(DeltaLakeHistorySchema.ICT);
                    if (usesIct && !info.isNullAt(ordinal)) {
                        row.setTimestampMillis(info.getLong(ordinal));
                    }
                });
            }
            batch.add(row);
            if (batch.size() == ROWS_PER_SPLIT) {
                splits.add(DeltaLakeScanRange.history(batch));
                batch = new ArrayList<>(ROWS_PER_SPLIT);
            }
        }
        if (!batch.isEmpty()) {
            splits.add(DeltaLakeScanRange.history(batch));
        }
        return splits;
    }

    private static long[] normalizedMtimes(List<FileStatus> files, int first) {
        long[] timestamps = new long[files.size() - first];
        long previous = Long.MIN_VALUE;
        for (int i = 0; i < files.size(); i++) {
            long mtime = files.get(i).getModificationTime();
            previous = i > 0 && mtime <= previous ? Math.incrementExact(previous) : mtime;
            if (i >= first) {
                timestamps[i - first] = previous;
            }
        }
        return timestamps;
    }

    private static void readCommitInfo(Engine engine, FileStatus file, StructType projection,
            ConnectorScanContext context, Consumer<Row> consumer) throws IOException {
        try (CloseableIterator<ColumnarBatch> batches = engine.getJsonHandler().readJsonFiles(
                DeltaKernelCompat.singleLogFile(file), projection, Optional.empty())) {
            while (batches.hasNext()) {
                context.checkCancelled();
                try (CloseableIterator<Row> rows = batches.next().getRows()) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        if (!row.isNullAt(0)) {
                            consumer.accept(row.getStruct(0));
                            return;
                        }
                    }
                }
            }
        }
    }

    private static void fillDetails(Row info, TDeltaLakeHistoryRow row) {
        readString(info, "userId", row::setUserId);
        readString(info, "userName", row::setUserName);
        readString(info, "operation", row::setOperation);
        readString(info, "clusterId", row::setClusterId);
        readString(info, "isolationLevel", row::setIsolationLevel);
        readString(info, "userMetadata", row::setUserMetadata);
        readString(info, "engineInfo", row::setEngineInfo);

        int readVersion = info.getSchema().indexOf("readVersion");
        if (!info.isNullAt(readVersion)) {
            row.setReadVersion(info.getLong(readVersion));
        }
        int blindAppend = info.getSchema().indexOf("isBlindAppend");
        if (!info.isNullAt(blindAppend)) {
            row.setIsBlindAppend(info.getBoolean(blindAppend));
        }
        readMap(info, "operationParameters", row::setOperationParameters);
        readMap(info, "operationMetrics", row::setOperationMetrics);

        int jobOrdinal = info.getSchema().indexOf("job");
        if (!info.isNullAt(jobOrdinal)) {
            Row source = info.getStruct(jobOrdinal);
            TDeltaLakeHistoryJob job = new TDeltaLakeHistoryJob();
            readString(source, "jobId", job::setJobId);
            readString(source, "jobName", job::setJobName);
            readString(source, "jobRunId", job::setJobRunId);
            readString(source, "runId", job::setRunId);
            readString(source, "jobOwnerId", job::setJobOwnerId);
            readString(source, "triggerType", job::setTriggerType);
            row.setJob(job);
        }
        int notebookOrdinal = info.getSchema().indexOf("notebook");
        if (!info.isNullAt(notebookOrdinal)) {
            TDeltaLakeHistoryNotebook notebook = new TDeltaLakeHistoryNotebook();
            readString(info.getStruct(notebookOrdinal), "notebookId", notebook::setNotebookId);
            row.setNotebook(notebook);
        }
    }

    private static void readString(Row row, String name, Consumer<String> consumer) {
        int ordinal = row.getSchema().indexOf(name);
        if (!row.isNullAt(ordinal)) {
            consumer.accept(row.getString(ordinal));
        }
    }

    private static void readMap(Row row, String name, Consumer<List<TDeltaLakeHistoryMapEntry>> consumer) {
        int ordinal = row.getSchema().indexOf(name);
        if (row.isNullAt(ordinal)) {
            return;
        }
        MapValue map = row.getMap(ordinal);
        ColumnVector keys = map.getKeys();
        ColumnVector values = map.getValues();
        List<TDeltaLakeHistoryMapEntry> entries = new ArrayList<>(map.getSize());
        for (int i = 0; i < map.getSize(); i++) {
            TDeltaLakeHistoryMapEntry entry = new TDeltaLakeHistoryMapEntry().setKey(keys.getString(i));
            if (!values.isNullAt(i)) {
                entry.setValue(values.getString(i));
            }
            entries.add(entry);
        }
        consumer.accept(entries);
    }

    private static boolean isVersion(String name) {
        return DeltaLakeHistorySchema.VERSION.equalsIgnoreCase(name);
    }

    private static boolean isTimestamp(String name) {
        return DeltaLakeHistorySchema.TIMESTAMP.equalsIgnoreCase(name);
    }

    private static Bounds restrict(ConnectorExpression expression, Bounds bounds) {
        if (expression instanceof ConnectorAnd) {
            for (ConnectorExpression conjunct : ((ConnectorAnd) expression).getConjuncts()) {
                bounds = restrict(conjunct, bounds);
            }
            return bounds;
        }
        if (!(expression instanceof ConnectorComparison)) {
            return bounds;
        }
        ConnectorComparison comparison = (ConnectorComparison) expression;
        if (comparison.getLeft() instanceof ConnectorColumnRef
                && isVersion(((ConnectorColumnRef) comparison.getLeft()).getColumnName())
                && comparison.getRight() instanceof ConnectorLiteral) {
            Object value = ((ConnectorLiteral) comparison.getRight()).getValue();
            if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
                return bounds.intersect(comparison.getOperator(), ((Number) value).longValue());
            }
        }
        return bounds;
    }

    private record Bounds(long start, long end) {
        Bounds intersect(Operator operator, long value) {
            return switch (operator) {
                case EQ, EQ_FOR_NULL -> new Bounds(Math.max(start, value), Math.min(end, value));
                case GE -> new Bounds(Math.max(start, value), end);
                case LE -> new Bounds(start, Math.min(end, value));
                case GT -> value == Long.MAX_VALUE ? new Bounds(1, 0)
                        : new Bounds(Math.max(start, value + 1), end);
                case LT -> value == Long.MIN_VALUE ? new Bounds(1, 0)
                        : new Bounds(start, Math.min(end, value - 1));
                default -> this;
            };
        }
    }
}
