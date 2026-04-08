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
import org.apache.doris.common.jni.vec.VectorTable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * JniWriter is the base class for JNI-based writers, symmetric to JniScanner.
 * Constructor signature: (int batchSize, Map<String, String> params) matches JniScanner
 * to reuse the same class loading mechanism (Jni::Util::get_jni_scanner_class).
 *
 * Lifecycle: open() -> write() [repeated] -> close()
 */
public abstract class JniWriter {
    protected int batchSize;
    protected Map<String, String> params;
    protected ColumnType[] columnTypes;
    protected String[] fields;
    protected long writeTime = 0;
    protected long readTableTime = 0;

    public JniWriter(int batchSize, Map<String, String> params) {
        this.batchSize = batchSize;
        this.params = params;
    }

    public abstract void open() throws IOException;

    /**
     * JNI entry point: receives C++ Block metadata, creates a ReadableTable,
     * then delegates to writeInternal.
     */
    public void write(Map<String, String> inputParams) throws IOException {
        // Parse and cache schema on first call
        if (columnTypes == null) {
            String requiredFields = inputParams.get("required_fields");
            String columnsTypes = inputParams.get("columns_types");
            if (requiredFields != null && !requiredFields.isEmpty()) {
                fields = requiredFields.split(",");
                String[] typeStrs = columnsTypes.split("#");
                columnTypes = new ColumnType[typeStrs.length];
                for (int i = 0; i < typeStrs.length; i++) {
                    columnTypes[i] = ColumnType.parseType(fields[i], typeStrs[i]);
                }
            } else {
                fields = new String[0];
                columnTypes = new ColumnType[0];
            }
        }

        long startRead = System.nanoTime();
        VectorTable inputTable = VectorTable.createReadableTable(inputParams);
        readTableTime += System.nanoTime() - startRead;

        long startWrite = System.nanoTime();
        writeInternal(inputTable);
        writeTime += System.nanoTime() - startWrite;
    }

    protected abstract void writeInternal(VectorTable inputTable) throws IOException;

    public abstract void close() throws IOException;

    /**
     * Performance metrics. Key format: "metricType:metricName"
     * Supported types: timer, counter, bytes (same as JniScanner).
     */
    public Map<String, String> getStatistics() {
        return Collections.emptyMap();
    }

    public long getWriteTime() {
        return writeTime;
    }

    public long getReadTableTime() {
        return readTableTime;
    }
}
