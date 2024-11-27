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

package org.apache.doris.common.jni;


import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.common.jni.vec.NativeColumnValue;
import org.apache.doris.common.jni.vec.ScanPredicate;
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.common.jni.vec.VectorTable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public abstract class JniScanner {
    protected VectorTable vectorTable;
    protected String[] fields;
    protected ColumnType[] types;
    @Deprecated
    // This predicate is from BE, but no used.
    // TODO: actually, we can generate the predicate for JNI scanner in FE's planner,
    // then serialize it to BE, and BE pass it to JNI scanner directly.
    // NO need to use this intermediate expression, because each JNI scanner has its
    // own predicate expression format.
    // For example, Paimon use "PaimonScannerUtils.decodeStringToObject(paimonPredicate)"
    // to deserialize the predicate string to PaimonPredicate object.
    protected ScanPredicate[] predicates;
    protected int batchSize;

    // Initialize JniScanner
    public abstract void open() throws IOException;

    // Close JniScanner and release resources
    public abstract void close() throws IOException;

    // Scan data and save as vector table
    protected abstract int getNext() throws IOException;

    // parse table schema
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    protected void initTableInfo(ColumnType[] requiredTypes, String[] requiredFields, int batchSize) {
        this.types = requiredTypes;
        this.fields = requiredFields;
        this.batchSize = batchSize;
    }

    protected void appendNativeData(int index, NativeColumnValue value) {
        vectorTable.appendNativeData(index, value);
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

    public String getTableSchema() throws IOException {
        TableSchema tableSchema = parseTableSchema();
        return tableSchema.getTableSchema();
    }

    public long getNextBatchMeta() throws IOException {
        if (vectorTable == null) {
            vectorTable = VectorTable.createWritableTable(types, fields, batchSize);
        }
        int numRows;
        try {
            numRows = getNext();
        } catch (IOException e) {
            releaseTable();
            throw e;
        }
        if (numRows == 0) {
            releaseTable();
            return 0;
        }
        return getMetaAddress(numRows);
    }

    /**
     * Get performance metrics. The key should be pattern like "metricType:metricName".
     * Support three metric types: timer, counter and bytes.
     * The c++ side will attach metricName into profile automatically.
     */
    public Map<String, String> getStatistics() {
        return Collections.emptyMap();
    }

    private long getMetaAddress(int numRows) {
        assert (numRows == vectorTable.getNumRows());
        return vectorTable.getMetaAddress();
    }

    public void resetTable() {
        if (vectorTable != null) {
            vectorTable.reset();
        }
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
