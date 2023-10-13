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

package org.apache.doris.common.jni.vec;


import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType.Type;

/**
 * Store a batch of data as vector table.
 */
public class VectorTable {
    private final VectorColumn[] columns;
    private final ColumnType[] columnTypes;
    private final String[] fields;
    private final ScanPredicate[] predicates;
    private final VectorColumn meta;
    private int numRows;

    private final boolean isRestoreTable;

    public VectorTable(ColumnType[] types, String[] fields, ScanPredicate[] predicates, int capacity) {
        this.columnTypes = types;
        this.fields = fields;
        this.columns = new VectorColumn[types.length];
        this.predicates = predicates;
        int metaSize = 1; // number of rows
        for (int i = 0; i < types.length; i++) {
            columns[i] = new VectorColumn(types[i], capacity);
            metaSize += types[i].metaSize();
        }
        this.meta = new VectorColumn(new ColumnType("#meta", Type.BIGINT), metaSize);
        this.numRows = 0;
        this.isRestoreTable = false;
    }

    public VectorTable(ColumnType[] types, String[] fields, long metaAddress) {
        long address = metaAddress;
        this.columnTypes = types;
        this.fields = fields;
        this.columns = new VectorColumn[types.length];
        this.predicates = new ScanPredicate[0];

        this.numRows = (int) OffHeap.getLong(null, address);
        address += 8;
        int metaSize = 1; // stores the number of rows + other columns meta data
        for (int i = 0; i < types.length; i++) {
            columns[i] = new VectorColumn(types[i], numRows, address);
            metaSize += types[i].metaSize();
            address += types[i].metaSize() * 8L;
        }
        this.meta = new VectorColumn(metaAddress, metaSize, new ColumnType("#meta", Type.BIGINT));
        this.isRestoreTable = true;
    }

    public void appendNativeData(int fieldId, NativeColumnValue o) {
        assert (!isRestoreTable);
        columns[fieldId].appendNativeValue(o);
    }

    public void appendData(int fieldId, ColumnValue o) {
        assert (!isRestoreTable);
        columns[fieldId].appendValue(o);
    }

    public VectorColumn[] getColumns() {
        return columns;
    }

    public VectorColumn getColumn(int fieldId) {
        return columns[fieldId];
    }

    public ColumnType[] getColumnTypes() {
        return columnTypes;
    }

    public String[] getFields() {
        return fields;
    }

    public void releaseColumn(int fieldId) {
        assert (!isRestoreTable);
        columns[fieldId].close();
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public int getNumRows() {
        return this.numRows;
    }

    public long getMetaAddress() {
        if (!isRestoreTable) {
            meta.reset();
            meta.appendLong(numRows);
            for (VectorColumn c : columns) {
                c.updateMeta(meta);
            }
        }
        return meta.dataAddress();
    }

    public void reset() {
        assert (!isRestoreTable);
        for (VectorColumn column : columns) {
            column.reset();
        }
        meta.reset();
    }

    public void close() {
        assert (!isRestoreTable);
        for (int i = 0; i < columns.length; i++) {
            releaseColumn(i);
        }
        meta.close();
    }

    // for test only.
    public String dump(int rowLimit) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowLimit && i < numRows; i++) {
            for (int j = 0; j < columns.length; j++) {
                if (j != 0) {
                    sb.append(", ");
                }
                columns[j].dump(sb, i);
            }
            sb.append('\n');
        }
        return sb.toString();
    }
}
