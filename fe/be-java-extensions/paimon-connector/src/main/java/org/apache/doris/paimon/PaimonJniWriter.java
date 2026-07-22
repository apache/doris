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

package org.apache.doris.paimon;

import org.apache.doris.common.classloader.ThreadClassLoaderContext;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticatorCache;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * JNI entry point for Paimon write operations.
 *
 * <p>Called from C++ ({@code JniPaimonWriter}) via JNI. One instance per BE pipeline
 * fragment (one per {@code VPaimonTableWriter}). Data path:
 *
 * <pre>
 *   C++ Block → Arrow IPC Stream → JNI direct ByteBuffer
 *   → PaimonJniWriter.write(directBuffer)
 *   → ArrowStreamReader → VectorSchemaRoot
 *   → PaimonArrowConverter (column-major typed extraction)
 *   → PaimonWriteSchema.tableRow() (canonical table-schema order)
 *   → BatchTableWrite.write(row) (SDK-owned routing and buffering)
 * </pre>
 *
 * <p>Commit path:
 *
 * <pre>
 *   VPaimonTableWriter::close() → JNI → PaimonJniWriter.prepareCommit()
 *   → BatchTableWrite.prepareCommit()
 *   → PaimonCommitCodec.encode() → DPCM-framed byte[][]
 *   → C++ collects TPaimonCommitMessage[] → RPC to FE → PaimonTransaction
 * </pre>
 */
public class PaimonJniWriter {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonJniWriter.class);

    private final BufferAllocator allocator;
    private final ClassLoader classLoader;
    private final PaimonCommitCodec commitCodec = new PaimonCommitCodec();

    private PreExecutionAuthenticator preExecutionAuthenticator;
    private PaimonArrowConverter arrowConverter;

    private PaimonWriteSchema writeSchema;
    private FileStoreTable table;
    private BatchTableWrite writer;
    private TableWriteImpl<?> tableWrite;
    private IOManager ioManager;
    private long commitIdentifier;
    private String commitUser;
    private List<CommitMessage> preparedCommitMessages = Collections.emptyList();

    public PaimonJniWriter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.classLoader = this.getClass().getClassLoader();
    }

    // ────────────────────────────────────────────────────────────
    // JNI entry points (called from C++)
    // ────────────────────────────────────────────────────────────

    /**
     * Initialize the writer. Called once per BE pipeline fragment via JNI.
     *
     * <p>This method:
     * <ol>
     *   <li>Deserializes the target Paimon {@link FileStoreTable} selected by FE.</li>
     *   <li>Creates a {@link PaimonWriteSchema} which normalizes Doris input
     *       columns to the table-schema row layout.</li>
     *   <li>Opens one Paimon SDK writer session.</li>
     * </ol>
     *
     * @param serializedTable serialized Paimon table selected by FE
     * @param hadoopConfig   filesystem and authentication configuration
     * @param columnNames    output column names in the order produced by BE
     * @param transactionId  Doris external transaction identifier
     * @param commitUser     Paimon commit user shared with the FE committer
     * @param overwrite      whether this is an overwrite write
     * @param timeZone       normalized Doris session timezone used for Paimon LTZ values
     * @param spillDirectories Doris storage-root scoped directories for Paimon write-buffer spill
     */
    public void open(String serializedTable, Map<String, String> hadoopConfig,
                     String[] columnNames, long transactionId, String commitUser,
                     boolean overwrite, String timeZone, String spillDirectories) throws Exception {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopConfig);
            this.arrowConverter = new PaimonArrowConverter(ZoneId.of(timeZone));
            preExecutionAuthenticator.execute(() -> {
                try {
                    FileStoreTable table = PaimonUtils.deserialize(serializedTable);
                    if (table.bucketMode() == BucketMode.HASH_DYNAMIC) {
                        throw new UnsupportedOperationException(
                                "Paimon dynamic-bucket tables are not supported for writes");
                    }
                    LOG.info("PaimonJniWriter opening: table={}, columns={}",
                            table.fullName(), columnNames != null ? columnNames.length : 0);
                    this.commitIdentifier = transactionId;
                    this.table = table;
                    this.commitUser = commitUser;

                    this.writeSchema = PaimonWriteSchema.create(table.rowType(), columnNames);
                    if (writeSchema.isPartial() && !table.primaryKeys().isEmpty()) {
                        throw new UnsupportedOperationException(
                                "Paimon primary-key write requires all table columns");
                    }
                    openFileStoreWriter(table, commitUser, overwrite, spillDirectories);
                    return null;
                } catch (Throwable t) {
                    throw new RuntimeException("PaimonJniWriter open failed", t);
                }
            });
        }
    }

    /**
     * Write a batch of rows from an Arrow IPC Stream buffer.
     *
     * <p>Called from C++ {@code JniPaimonWriter::_write_projected_block()}
     * once per Block. The buffer is a zero-copy direct view of the native
     * Arrow IPC Stream bytes. Rows are deserialized, normalized to table-schema
     * order, and handed to Paimon's high-level {@code write(row)} API. The SDK
     * owns partition/bucket routing, buffering, spill, and file rolling.
     *
     * @param directBuffer direct view of the native Arrow IPC Stream bytes (no copy)
     */
    public void write(ByteBuffer directBuffer) throws Exception {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            preExecutionAuthenticator.execute(() -> {
                try {
                    try (ArrowStreamReader reader = new ArrowStreamReader(
                            new DirectBufInputStream(directBuffer), allocator)) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        while (reader.loadNextBatch()) {
                            writeBatch(root);
                        }
                    }
                    return null;
                } catch (Throwable t) {
                    throw new RuntimeException("PaimonJniWriter write failed: bytes="
                            + directBuffer.capacity(), t);
                }
            });
        }
    }

    /**
     * Prepare commit: flush all in-memory data, close files, and serialize commit
     * messages for the FE coordinator.
     *
     * <p>Flushes and collects Paimon {@link CommitMessage}s, then encodes them via
     * {@link PaimonCommitCodec} into DPCM-framed byte chunks that are forwarded to
     * FE through the BE.
     *
     * @return byte[][]  each element is a DPCM-framed serialized CommitMessage chunk
     */
    public byte[][] prepareCommit() throws Exception {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            return preExecutionAuthenticator.execute(() -> {
                try {
                    List<CommitMessage> messages = prepareCommitMessages();
                    if (messages.isEmpty()) {
                        LOG.info("PaimonJniWriter prepareCommit: empty");
                        return new byte[0][];
                    }
                    LOG.info("PaimonJniWriter prepareCommit: {} messages", messages.size());
                    return commitCodec.encode(messages);
                } catch (Throwable t) {
                    throw new RuntimeException("PaimonJniWriter prepareCommit failed", t);
                }
            });
        }
    }

    /**
     * Abort: discard all written data files and close the SDK writer.
     * Called from C++ when write or prepareCommit fails.
     */
    public void abort() throws Exception {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            try {
                if (preExecutionAuthenticator != null) {
                    preExecutionAuthenticator.execute(() -> {
                        abortWriter();
                        return null;
                    });
                } else {
                    abortWriter();
                }
            } catch (Exception e) {
                LOG.error("PaimonJniWriter abort failed", e);
                throw e;
            }
        }
    }

    /**
     * Close: release all resources.
     */
    public void close() throws Exception {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            try {
                if (preExecutionAuthenticator != null) {
                    preExecutionAuthenticator.execute(() -> {
                        closeResources();
                        return null;
                    });
                } else {
                    closeResources();
                }
            } catch (Exception e) {
                LOG.warn("PaimonJniWriter close error", e);
                throw e;
            }
        }
    }

    // ────────────────────────────────────────────────────────────
    // Initialization helpers
    // ────────────────────────────────────────────────────────────

    private void openFileStoreWriter(FileStoreTable table, String commitUser, boolean overwrite,
            String spillDirectories) throws Exception {
        CoreOptions coreOptions = CoreOptions.fromMap(table.options());
        tableWrite = table.newWrite(commitUser);
        if (overwrite) {
            tableWrite.withIgnorePreviousFiles(true);
        }
        writer = tableWrite;
        openSpillResources(coreOptions, spillDirectories);
    }

    private void openSpillResources(CoreOptions coreOptions, String spillDirectories) throws Exception {
        if (!coreOptions.writeBufferSpillable()) {
            return;
        }
        String[] splitDirectories = IOManagerImpl.splitPaths(spillDirectories);
        for (String directory : splitDirectories) {
            Files.createDirectories(Paths.get(directory));
        }
        ioManager = IOManager.create(splitDirectories);
        HeapMemorySegmentPool memorySegmentPool = new HeapMemorySegmentPool(
                coreOptions.writeBufferSize(), coreOptions.pageSize());
        writer.withIOManager(ioManager).withMemoryPool(memorySegmentPool);
        LOG.info("Paimon writer spill enabled: dirs={}", spillDirectories);
    }

    // ────────────────────────────────────────────────────────────
    // Data writing
    // ────────────────────────────────────────────────────────────

    private void writeBatch(VectorSchemaRoot root) throws Exception {
        int rowCount = root.getRowCount();
        if (rowCount == 0) {
            return;
        }
        // Extract values in Doris input order, then normalize every row to the
        // full table schema before calling the SDK's high-level write API.
        Object[][] columnValues = arrowConverter.convert(root, writeSchema.targetTypes());
        BatchTableWrite currentWriter = requireWriter();
        for (int r = 0; r < rowCount; r++) {
            currentWriter.write(writeSchema.tableRow(columnValues, r));
        }
    }

    // ────────────────────────────────────────────────────────────
    // Resource management
    // ────────────────────────────────────────────────────────────

    private void closeResources() throws Exception {
        try {
            closeWriter();
        } finally {
            writeSchema = null;
            arrowConverter = null;
            allocator.close();
        }
    }

    private List<CommitMessage> prepareCommitMessages() throws Exception {
        BatchTableWrite currentWriter = requireWriter();
        List<CommitMessage> messages = tableWrite != null && commitIdentifier > 0
                ? tableWrite.prepareCommit(true, commitIdentifier)
                : currentWriter.prepareCommit();
        preparedCommitMessages = new ArrayList<>(messages);
        return messages;
    }

    private BatchTableWrite requireWriter() {
        if (writer == null) {
            throw new IllegalStateException("Paimon writer is not open");
        }
        return writer;
    }

    private void closeWriter() throws Exception {
        try {
            if (writer != null) {
                writer.close();
            }
        } finally {
            writer = null;
            tableWrite = null;
            table = null;
            commitUser = null;
            preparedCommitMessages = Collections.emptyList();
            try {
                if (ioManager != null) {
                    ioManager.close();
                }
            } finally {
                ioManager = null;
            }
        }
    }

    private void abortWriter() throws Exception {
        try {
            List<CommitMessage> messages = preparedCommitMessages;
            if (messages.isEmpty() && writer != null) {
                messages = prepareCommitMessages();
            }
            if (!messages.isEmpty()) {
                InnerTableCommit committer = table.newCommit(commitUser);
                try {
                    committer.abort(messages);
                } finally {
                    committer.close();
                }
            }
        } finally {
            closeWriter();
        }
    }

    // ────────────────────────────────────────────────────────────
    // Utilities
    // ────────────────────────────────────────────────────────────

    /** InputStream over a direct ByteBuffer (no copy). */
    private static class DirectBufInputStream extends InputStream {
        private final ByteBuffer buf;

        DirectBufInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public int read() {
            if (buf.hasRemaining()) {
                return buf.get() & 0xFF;
            }
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (!buf.hasRemaining()) {
                return -1;
            }
            int n = Math.min(len, buf.remaining());
            buf.get(b, off, n);
            return n;
        }
    }
}
