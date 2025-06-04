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

public abstract class JniWriter {
    protected VectorTable vectorTable;
    protected String[] fields;
    protected ColumnType[] types;

    // Initialize JniWriter
    public abstract void open() throws IOException;

    // Close JniWriter and release resources
    public abstract void close() throws IOException;

    // Write data to the writer
    public abstract void write(Map<String, String> params) throws IOException;

    // Finish writing data
    public abstract void finish() throws Exception;

    public Map<String, String> getStatistics() {
        return Collections.emptyMap();
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
