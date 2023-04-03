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

package org.apache.doris.jni.vec;

import org.apache.doris.jni.vec.ColumnType.Type;

/**
 * Store a batch of data as vector table.
 */
public class VectorTable {
    private final VectorColumn[] columns;
    private final String[] fields;
    private final ScanPredicate[] predicates;
    private final VectorColumn meta;
    private int numRows;

    public VectorTable(ColumnType[] types, String[] fields, ScanPredicate[] predicates, int capacity) {
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
    }

    public void appendData(int fieldId, ColumnValue o) {
        columns[fieldId].appendValue(o);
    }

    public void releaseColumn(int fieldId) {
        columns[fieldId].close();
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public int getNumRows() {
        return this.numRows;
    }

    public long getMetaAddress() {
        meta.reset();
        meta.appendLong(numRows);
        for (VectorColumn c : columns) {
            c.updateMeta(meta);
        }
        return meta.dataAddress();
    }

    public void reset() {
        for (VectorColumn column : columns) {
            column.reset();
        }
        meta.reset();
    }

    public void close() {
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
