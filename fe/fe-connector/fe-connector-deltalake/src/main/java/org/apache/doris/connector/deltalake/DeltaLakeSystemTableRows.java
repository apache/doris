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

import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TDeltaLakeSysTableBatch;
import org.apache.doris.thrift.TDeltaLakeSysTableColumn;
import org.apache.doris.thrift.TDeltaLakeSysTableField;
import org.apache.doris.thrift.TDeltaLakeSysTableMapEntry;
import org.apache.doris.thrift.TDeltaLakeSysTableRow;

import com.google.common.base.Utf8;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

final class DeltaLakeSystemTableRows {
    private DeltaLakeSystemTableRows() {
    }

    static TDeltaLakeSysTableField text(String value) {
        TDeltaLakeSysTableField cell = new TDeltaLakeSysTableField();
        return value == null ? cell : cell.setStringVal(value);
    }

    static TDeltaLakeSysTableField number(long value) {
        return new TDeltaLakeSysTableField().setInt64Val(value);
    }

    static TDeltaLakeSysTableField strings(Collection<String> values) {
        return new TDeltaLakeSysTableField().setArrayVal(values.stream().map(DeltaLakeSystemTableRows::text).toList());
    }

    static TDeltaLakeSysTableField map(Map<String, String> values) {
        List<TDeltaLakeSysTableMapEntry> entries = new ArrayList<>(values.size());
        values.forEach((key, value) -> entries.add(new TDeltaLakeSysTableMapEntry().setKey(key).setValue(text(value))));
        return new TDeltaLakeSysTableField().setMapVal(entries);
    }

    static boolean needs(List<TDeltaLakeSysTableColumn> columns, String name) {
        return columns.stream().anyMatch(column -> column.getName().equals(name));
    }

    static TDeltaLakeSysTableRow row(List<TDeltaLakeSysTableColumn> columns,
            Function<String, TDeltaLakeSysTableField> value) {
        List<TDeltaLakeSysTableField> cells = new ArrayList<>(columns.size());
        for (TDeltaLakeSysTableColumn column : columns) {
            cells.add(value.apply(column.getName()));
        }
        return new TDeltaLakeSysTableRow().setCells(cells);
    }

    static List<ConnectorScanRange> pack(String name, List<TDeltaLakeSysTableColumn> columns,
            Iterator<TDeltaLakeSysTableRow> rows, ConnectorScanContext context) {
        List<ConnectorScanRange> result = new ArrayList<>();
        new Batches(name, columns, rows, context).forEachRemaining(result::add);
        return result;
    }

    /** Binary Thrift field/list overhead and UTF-8 payloads. An oversized single row gets its own split. */
    private static long size(TDeltaLakeSysTableField cell) {
        if (cell.isSetStringVal()) {
            return 8L + Utf8.encodedLength(cell.getStringVal());
        }
        if (cell.isSetMapVal()) {
            long bytes = 9;
            for (TDeltaLakeSysTableMapEntry entry : cell.getMapVal()) {
                bytes += 11L + Utf8.encodedLength(entry.getKey()) + size(entry.getValue());
            }
            return bytes;
        }
        List<TDeltaLakeSysTableField> children = cell.isSetArrayVal() ? cell.getArrayVal() : cell.getStructVal();
        if (children != null) {
            long bytes = 9;
            for (TDeltaLakeSysTableField child : children) {
                bytes += size(child);
            }
            return bytes;
        }
        return 12;
    }

    static final class Batches implements Iterator<ConnectorScanRange> {
        static final int MAX_ROWS = 1000;
        static final int MAX_BYTES = 512 * 1024;
        private final String name;
        private final List<TDeltaLakeSysTableColumn> columns;
        private final Iterator<TDeltaLakeSysTableRow> rows;
        private final ConnectorScanContext context;
        private final long headerBytes;
        private long splitId;
        private TDeltaLakeSysTableRow pending;

        Batches(String name, List<TDeltaLakeSysTableColumn> columns, Iterator<TDeltaLakeSysTableRow> rows,
                ConnectorScanContext context) {
            this.name = name;
            this.columns = columns;
            this.rows = rows;
            this.context = context;
            headerBytes = 17L + columns.stream().mapToLong(col -> 15L + Utf8.encodedLength(col.getName())).sum();
        }

        @Override
        public boolean hasNext() {
            context.checkCancelled();
            return pending != null || rows.hasNext();
        }

        @Override
        public ConnectorScanRange next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            List<TDeltaLakeSysTableRow> batch = new ArrayList<>();
            long bytes = headerBytes;
            while (batch.size() < MAX_ROWS && (batch.isEmpty() || bytes < MAX_BYTES) && hasNext()) {
                TDeltaLakeSysTableRow row = pending == null ? rows.next() : pending;
                pending = null;
                long rowBytes = 9;
                for (TDeltaLakeSysTableField cell : row.getCells()) {
                    rowBytes += size(cell);
                }
                if (!batch.isEmpty() && bytes + rowBytes > MAX_BYTES) {
                    pending = row;
                    break;
                }
                bytes += rowBytes;
                batch.add(row);
            }
            return DeltaLakeScanRange.systemTable(name, splitId++,
                    new TDeltaLakeSysTableBatch().setColumns(columns).setRows(batch));
        }
    }
}
