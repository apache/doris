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

package org.apache.doris.jni;

import org.apache.doris.jni.vec.ColumnType;
import org.apache.doris.jni.vec.ColumnValue;
import org.apache.doris.jni.vec.ScanPredicate;
import org.apache.doris.jni.vec.VectorTable;

import java.io.IOException;

public abstract class JniScanner {
    protected VectorTable vectorTable;
    protected String[] fields;
    protected ColumnType[] types;
    protected ScanPredicate[] predicates;
    protected int batchSize;

    // Initialize JniScanner
    public abstract void open() throws IOException;

    // Close JniScanner and release resources
    public abstract void close() throws IOException;

    // Scan data and save as vector table
    protected abstract int getNext() throws IOException;

    protected void initTableInfo(ColumnType[] requiredTypes, String[] requiredFields, ScanPredicate[] predicates,
            int batchSize) {
        this.types = requiredTypes;
        this.fields = requiredFields;
        this.predicates = predicates;
        this.batchSize = batchSize;
    }

    protected void appendData(int index, ColumnValue value) {
        vectorTable.appendData(index, value);
    }

    protected int getBatchSize() {
        return batchSize;
    }

    public VectorTable getTable() {
        return vectorTable;
    }

    public long getNextBatchMeta() throws IOException {
        if (vectorTable == null) {
            vectorTable = new VectorTable(types, fields, predicates, batchSize);
        }
        int numRows;
        try {
            numRows = getNext();
        } catch (IOException e) {
            releaseTable();
            throw e;
        }
        if (numRows == 0) {
            return 0;
        }
        return getMetaAddress(numRows);
    }

    private long getMetaAddress(int numRows) {
        vectorTable.setNumRows(numRows);
        return vectorTable.getMetaAddress();
    }

    public void resetTable() {
        vectorTable.reset();
    }

    protected void releaseColumn(int fieldId) {
        vectorTable.releaseColumn(fieldId);
    }

    public void releaseTable() {
        if (vectorTable != null) {
            vectorTable.close();
        }
        vectorTable = null;
    }
}
