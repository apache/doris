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
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison.Operator;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TDeltaLakeSysTableBatch;
import org.apache.doris.thrift.TDeltaLakeSysTableColumn;
import org.apache.doris.thrift.TDeltaLakeSysTableField;
import org.apache.doris.thrift.TDeltaLakeSysTableMapEntry;
import org.apache.doris.thrift.TDeltaLakeSysTableRow;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

final class DeltaLakeHistorySystemTable implements DeltaLakeSystemTable {
    private static final int ROWS_PER_SPLIT = 1000;

    @Override
    public String name() {
        return DeltaLakeHistorySchema.NAME;
    }

    @Override
    public ConnectorTableSchema schema(DeltaLakeTableHandle handle) {
        return DeltaLakeHistorySchema.tableSchema(handle.getTable());
    }

    @Override
    public boolean supportsTimeTravel() {
        return false;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, DeltaLakeRuntime runtime,
            DeltaLakeTableHandle handle,
            ConnectorScanRequest request) {
        Bounds bounds = request.getFilter()
                .map(filter -> restrict(filter, new Bounds(0, handle.getVersion())))
                .orElseGet(() -> new Bounds(0, handle.getVersion()));
        if (bounds.start > bounds.end) {
            return List.of();
        }
        List<TDeltaLakeSysTableColumn> columns = projectedColumns(handle, request);
        boolean timestamp = columns.stream().anyMatch(col -> isTimestamp(col.getName()));
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
            CommitRange range = handle.commitRange(runtime, engine, rangeStart, bounds.end);
            return readRange(engine, snapshot, range, first, columns, timestamp, request.getScanContext());
        } catch (IOException e) {
            throw new DorisConnectorException("Cannot read Delta commit history for " + handle.getPath(), e);
        }
    }

    // The detail row and the corresponding history row must describe the same instant, even
    // when several retained pre-ICT commits have equal or decreasing modification times.
    static long lastModified(DeltaLakeRuntime runtime, DeltaLakeTableHandle handle, Snapshot snapshot) {
        Engine engine = runtime.engine(handle.reference());
        OptionalLong ict = DeltaKernelCompat.historyIctStart(snapshot);
        if (ict.isPresent() && handle.getVersion() >= ict.getAsLong()) {
            return snapshot.getTimestamp(engine);
        }
        try {
            OptionalLong first = DeltaKernelCompat.earliestHistoryVersion(engine, snapshot);
            // Log cleanup can advance past a snapshot that this statement already pinned.
            if (first.isEmpty() || first.getAsLong() > handle.getVersion()) {
                return snapshot.getTimestamp(engine);
            }
            List<FileStatus> files = handle.commitRange(runtime, engine, first.getAsLong(),
                    handle.getVersion()).getDeltaFiles();
            return files.isEmpty() ? snapshot.getTimestamp(engine) : normalizedMtimes(files, files.size() - 1)[0];
        } catch (IOException e) {
            throw new DorisConnectorException("Cannot read Delta commit timestamp for " + handle.getPath(), e);
        }
    }

    private List<ConnectorScanRange> readRange(Engine engine, Snapshot snapshot, CommitRange range,
            long first, List<TDeltaLakeSysTableColumn> columns, boolean timestamp, ConnectorScanContext context)
            throws IOException {
        List<FileStatus> files = range.getDeltaFiles();
        int firstIndex = Math.toIntExact(first - range.getStartVersion());
        long[] mtimes = timestamp ? normalizedMtimes(files, firstIndex) : null;
        OptionalLong ictStart = timestamp ? DeltaKernelCompat.historyIctStart(snapshot) : OptionalLong.empty();
        List<ConnectorScanRange> splits = new ArrayList<>();
        StructType details = new StructType();
        for (TDeltaLakeSysTableColumn col : columns) {
            if (!isVersion(col.getName()) && !isTimestamp(col.getName())) {
                details = details.add(DeltaLakeHistorySchema.DETAIL_FIELDS.get(col.getName()));
            }
        }
        StructType detailJson = new StructType().add(DeltaLakeHistorySchema.COMMIT_INFO, details);
        StructType ictJson = new StructType().add(DeltaLakeHistorySchema.COMMIT_INFO,
                details.add(DeltaLakeHistorySchema.ICT, LongType.LONG));
        List<TDeltaLakeSysTableRow> rows = new ArrayList<>(ROWS_PER_SPLIT);
        for (int index = files.size() - 1; index >= firstIndex; index--) {
            context.checkCancelled();
            long version = range.getStartVersion() + index;
            long mtime = timestamp ? mtimes[index - firstIndex] : 0;
            boolean usesIct = ictStart.isPresent() && version >= ictStart.getAsLong();
            TDeltaLakeSysTableRow row = details.length() > 0 || usesIct
                    ? readCommitInfo(engine, files.get(index), usesIct ? ictJson : detailJson, context,
                            info -> historyRow(columns, version, mtime, info, usesIct))
                    : historyRow(columns, version, mtime, null, false);
            rows.add(row);
            if (rows.size() == ROWS_PER_SPLIT || index == firstIndex) {
                TDeltaLakeSysTableBatch batch = new TDeltaLakeSysTableBatch().setColumns(columns).setRows(rows);
                splits.add(DeltaLakeScanRange.systemTable(name(), version + rows.size() - 1, batch));
                rows = new ArrayList<>(ROWS_PER_SPLIT);
            }
        }
        return splits;
    }

    private static long[] normalizedMtimes(List<FileStatus> files, int first) {
        long[] timestamps = new long[files.size() - first];
        long previous = Long.MIN_VALUE;
        for (int i = 0; i < files.size(); i++) {
            long mtime = files.get(i).getModificationTime();
            previous = DeltaKernelCompat.monotonicMtime(previous, mtime);
            if (i >= first) {
                timestamps[i - first] = previous;
            }
        }
        return timestamps;
    }

    private static TDeltaLakeSysTableRow readCommitInfo(Engine engine, FileStatus file, StructType projection,
            ConnectorScanContext context, Function<Row, TDeltaLakeSysTableRow> encode) throws IOException {
        try (CloseableIterator<ColumnarBatch> batches = engine.getJsonHandler().readJsonFiles(
                DeltaKernelCompat.singleLogFile(file), projection, Optional.empty())) {
            while (batches.hasNext()) {
                context.checkCancelled();
                try (CloseableIterator<Row> rows = batches.next().getRows()) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        if (!row.isNullAt(0)) {
                            return encode.apply(row.getStruct(0));
                        }
                    }
                }
            }
        }
        return encode.apply(null);
    }

    private static TDeltaLakeSysTableRow historyRow(List<TDeltaLakeSysTableColumn> columns, long version,
            long timestamp, Row info, boolean usesIct) {
        List<TDeltaLakeSysTableField> cells = new ArrayList<>(columns.size());
        for (TDeltaLakeSysTableColumn column : columns) {
            String name = column.getName();
            if (isVersion(name)) {
                cells.add(new TDeltaLakeSysTableField().setInt64Val(version));
            } else if (isTimestamp(name)) {
                int ict = usesIct && info != null ? info.getSchema().indexOf(DeltaLakeHistorySchema.ICT) : -1;
                cells.add(new TDeltaLakeSysTableField().setTimestampMillis(
                        ict >= 0 && !info.isNullAt(ict) ? info.getLong(ict) : timestamp));
            } else {
                cells.add(info == null ? new TDeltaLakeSysTableField()
                        : detailValue(info, info.getSchema().indexOf(name)));
            }
        }
        return new TDeltaLakeSysTableRow().setCells(cells);
    }

    private static TDeltaLakeSysTableField detailValue(Row row, int index) {
        TDeltaLakeSysTableField value = new TDeltaLakeSysTableField();
        if (row.isNullAt(index)) {
            return value;
        }
        DataType type = row.getSchema().at(index).getDataType();
        if (type instanceof StringType) {
            return value.setStringVal(row.getString(index));
        }
        if (type instanceof LongType) {
            return value.setInt64Val(row.getLong(index));
        }
        if (type instanceof BooleanType) {
            return value.setBoolVal(row.getBoolean(index));
        }
        if (type instanceof StructType struct) {
            Row source = row.getStruct(index);
            List<TDeltaLakeSysTableField> fields = new ArrayList<>(struct.length());
            for (int i = 0; i < struct.length(); i++) {
                fields.add(detailValue(source, i));
            }
            return value.setStructVal(fields);
        }
        if (type instanceof MapType) {
            MapValue map = row.getMap(index);
            ColumnVector keys = map.getKeys();
            ColumnVector values = map.getValues();
            List<TDeltaLakeSysTableMapEntry> entries = new ArrayList<>(map.getSize());
            for (int i = 0; i < map.getSize(); i++) {
                TDeltaLakeSysTableField item = new TDeltaLakeSysTableField();
                if (!values.isNullAt(i)) {
                    item.setStringVal(values.getString(i));
                }
                entries.add(new TDeltaLakeSysTableMapEntry().setKey(keys.getString(i)).setValue(item));
            }
            return value.setMapVal(entries);
        }
        throw new DorisConnectorException("Unsupported Delta history field type: " + type);
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
