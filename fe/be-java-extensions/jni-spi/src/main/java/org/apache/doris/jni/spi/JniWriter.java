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

package org.apache.doris.jni.spi;

import org.apache.doris.jni.spi.vec.VectorTable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Base class of every JNI writer, symmetric to {@link JniScanner}: BE resolves method ids on this
 * class and the entry points are {@code final} so that each call runs with the plugin's own
 * classloader installed as the thread context classloader. The reasoning is spelled out on
 * {@link JniScanner}; it applies unchanged here.
 *
 * <p>Lifecycle: {@code open()} -> {@code write()} repeatedly -> {@code close()}.
 */
public abstract class JniWriter {
    protected int batchSize;
    protected Map<String, String> params;
    protected long writeTime = 0;
    protected long readTableTime = 0;

    protected JniWriter(int batchSize, Map<String, String> params) {
        this.batchSize = batchSize;
        this.params = params;
    }

    // ------------------------------------------------------------------
    // Hooks implemented by plugins.
    // ------------------------------------------------------------------

    protected abstract void openInternal() throws IOException;

    protected abstract void writeInternal(VectorTable inputTable) throws IOException;

    protected abstract void closeInternal() throws IOException;

    /** Same key syntax as {@link JniScanner#collectStatistics()}. */
    protected Map<String, String> collectStatistics() {
        return Collections.emptyMap();
    }

    // ------------------------------------------------------------------
    // JNI entry points. Signatures are the BE ABI - see PROTOCOL.md.
    // ------------------------------------------------------------------

    public final void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            openInternal();
        }
    }

    /**
     * Receives one C++ block: {@code inputParams} carries the address of its meta array, which is
     * turned into a readable {@link VectorTable} before handing it to the plugin. The block
     * describes its own columns, so the schema keys BE also sends - {@code required_fields} and
     * {@code columns_types}, see PROTOCOL.md section 3 - are left in the map for a writer that
     * wants them rather than parsed here: no writer read the parsed form, and parsing it here
     * indexed the names by the types' position, which is only the same list by luck.
     */
    public final void write(Map<String, String> inputParams) throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            long startRead = System.nanoTime();
            VectorTable inputTable = VectorTable.createReadableTable(inputParams);
            readTableTime += System.nanoTime() - startRead;

            long startWrite = System.nanoTime();
            writeInternal(inputTable);
            writeTime += System.nanoTime() - startWrite;
        }
    }

    public final void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            closeInternal();
        }
    }

    public final Map<String, String> getStatistics() {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return collectStatistics();
        }
    }

    public final long getWriteTime() {
        return writeTime;
    }

    public final long getReadTableTime() {
        return readTableTime;
    }
}
