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

import org.apache.doris.jni.spi.vec.ColumnType;
import org.apache.doris.jni.spi.vec.ColumnValue;
import org.apache.doris.jni.spi.vec.NativeColumnValue;
import org.apache.doris.jni.spi.vec.VectorTable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Base class of every JNI scanner. Its public methods are the wire format between BE C++ and a
 * plugin: BE resolves their method ids on <em>this</em> class, never on the concrete plugin class,
 * so the ABI stays fixed while plugins evolve. See {@code PROTOCOL.md} in this module.
 *
 * <h2>Why the JNI entry points are final</h2>
 *
 * <p>Every entry point is {@code final} and installs the plugin's own classloader as the thread
 * context classloader for the duration of the call. This is not a convenience — it is required for
 * correctness. BE calls in on threads it created itself and attached to the JVM; such a thread's
 * context classloader is the system classloader, which under the plugin isolation model contains
 * only {@code conf/} and the SPI jars. Any library inside a plugin that resolves classes or
 * {@code ServiceLoader} providers through the context classloader (hadoop, iceberg, paimon and
 * trino all do) would otherwise fail with a class-not-found that points at the wrong place.
 *
 * <p>Trino solves the same problem with per-interface {@code ClassLoaderSafe} decorators. Building
 * it into the base class instead means plugin authors cannot forget it, at the cost of the entry
 * points not being overridable — which is what {@code final} states explicitly.
 *
 * <p>Subclasses implement the {@code *Internal} hooks and {@link #getNext()}.
 */
public abstract class JniScanner {
    protected VectorTable vectorTable;
    protected String[] fields;
    protected ColumnType[] types;
    protected int batchSize;
    protected long appendDataTime = 0;
    protected long createVectorTableTime = 0;

    // ------------------------------------------------------------------
    // Hooks implemented by plugins.
    // ------------------------------------------------------------------

    /** Acquire remote resources and prepare for scanning. */
    protected abstract void openInternal() throws IOException;

    /** Release everything acquired by {@link #openInternal()}. Must tolerate being called twice. */
    protected abstract void closeInternal() throws IOException;

    /**
     * Fill the current batch and return the number of rows appended.
     * Returning 0 means end of stream.
     */
    protected abstract int getNext() throws IOException;

    /**
     * Table schema of a table-valued function source (for example an avro file), serialized the way
     * BE expects it. Plugins normally build the string with the {@code TableSchema} helper in the
     * plugin toolkit rather than by hand; the SPI keeps it a {@code String} so that no JSON library
     * has to live on the SPI boundary.
     */
    protected String parseTableSchema() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Performance metrics of this scanner. Keys are {@code "metricType:metricName"}; BE attaches
     * metricName to the query profile automatically. PROTOCOL.md lists the metric types, and a key
     * BE does not recognise is logged and dropped rather than failing the query.
     */
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

    public final void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            closeInternal();
        }
    }

    /**
     * Scan one batch and return the address of its meta array, or 0 for end of stream.
     * On end of stream the table is released before returning, so BE must not read the address.
     */
    public final long getNextBatchMeta() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            if (vectorTable == null) {
                long l = System.nanoTime();
                vectorTable = VectorTable.createWritableTable(types, fields, batchSize);
                createVectorTableTime += System.nanoTime() - l;
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
    }

    public final String getTableSchema() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return parseTableSchema();
        }
    }

    public final Map<String, String> getStatistics() {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return collectStatistics();
        }
    }

    public final void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public final void releaseColumn(int fieldId) {
        vectorTable.releaseColumn(fieldId);
    }

    public final void releaseTable() {
        if (vectorTable != null) {
            vectorTable.close();
        }
        vectorTable = null;
    }

    public final long getAppendDataTime() {
        return appendDataTime;
    }

    public final long getCreateVectorTableTime() {
        return createVectorTableTime;
    }

    // ------------------------------------------------------------------
    // Helpers for subclasses. Pure Java state, no classloader concerns.
    // ------------------------------------------------------------------

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

    public void resetTable() {
        if (vectorTable != null) {
            vectorTable.reset();
        }
    }

    private long getMetaAddress(int numRows) {
        assert (numRows == vectorTable.getNumRows());
        return vectorTable.getMetaAddress();
    }
}
