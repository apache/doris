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

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.index.SimpleHashBucketAssigner;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * JNI entry point for Paimon write operations.
 *
 * <p>Called from C++ ({@code JniPaimonWriter}) via JNI. One instance per BE pipeline
 * fragment (one per {@code PaimonTableWriter}). Data path:
 *
 * <pre>
 *   C++ Block → Arrow RecordBatch → Arrow C Data Interface
 *   → PaimonJniWriter.writeArrow(arrayAddress, schemaAddress)
 *   → zero-copy VectorSchemaRoot view
 *   → PaimonArrowBatchAdapter (Arrow-backed Paimon columnar row)
 *   → PaimonWriteSchema.tableRow() (canonical table-schema order)
 *   → Paimon SDK bucket assignment and table write
 * </pre>
 *
 * <p>Commit path:
 *
 * <pre>
 *   PaimonTableWriter::close() → JNI → PaimonJniWriter.prepareCommit()
 *   → TableWriteImpl.prepareCommit()
 *   → PaimonCommitCodec.encode() → DPCM-framed byte[][]
 *   → C++ collects TPaimonCommitMessage[] → RPC to FE → PaimonTransaction
 * </pre>
 */
public class PaimonJniWriter {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonJniWriter.class);
    private static final int APPEND_ONLY_WRITER_MIN_PAGES = 1;
    private static final int MERGE_TREE_WRITER_MIN_PAGES = 3;

    private final ClassLoader classLoader;
    private final PaimonCommitCodec commitCodec = new PaimonCommitCodec();

    private BufferAllocator allocator;
    private PreExecutionAuthenticator preExecutionAuthenticator;
    private PaimonArrowBatchAdapter arrowAdapter;

    private PaimonWriteSchema writeSchema;
    private FileStoreTable table;
    private TableWriteImpl<?> writer;
    private IOManager ioManager;
    private long commitIdentifier;
    private String commitUser;
    private BucketMode bucketMode;
    private BucketAssigner hashBucketAssigner;
    private PartitionKeyExtractor<InternalRow> dynamicBucketExtractor;
    private GlobalIndexAssigner globalIndexAssigner;
    private boolean fullCompactionChangelog;
    private final Set<PartitionBucket> fullCompactionBuckets = new HashSet<>();
    private List<CommitMessage> preparedCommitMessages = Collections.emptyList();
    private boolean sdkCloseFailed;

    public PaimonJniWriter() {
        // Imported C Data vectors reference Doris-owned buffers; this allocator owns only Arrow's
        // Java-side views and metadata. Physical buffers remain charged to the C++ Arrow memory
        // pool until the synchronous writeArrow call returns.
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
     * @param changelogWrite whether the first input column contains a row change operation
     * @param timeZone       normalized Doris session timezone used for Paimon LTZ values
     * @param spillDirectories Doris storage-root scoped directories for Paimon write-buffer spill
     * @param nativePageMemoryLimitBytes maximum Doris-managed Paimon page memory
     * @param nativeMemoryManager opaque BE manager used to allocate tracked native pages
     */
    public void open(String serializedTable, Map<String, String> hadoopConfig,
                     String[] columnNames, long transactionId, String commitUser,
                     boolean overwrite, boolean changelogWrite, String timeZone, String spillDirectories,
                     long nativePageMemoryLimitBytes, long nativeMemoryManager) throws Exception {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            if (nativePageMemoryLimitBytes <= 0) {
                throw new IllegalArgumentException(
                        "PaimonJniWriter requires a positive native page memory limit");
            }
            if (nativeMemoryManager == 0) {
                throw new IllegalArgumentException(
                        "PaimonJniWriter requires a native memory manager");
            }
            this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopConfig);
            preExecutionAuthenticator.execute(() -> {
                try {
                    FileStoreTable table = PaimonUtils.deserialize(serializedTable);
                    LOG.info("PaimonJniWriter opening: table={}, columns={}",
                            table.fullName(), columnNames != null ? columnNames.length : 0);
                    this.commitIdentifier = transactionId;
                    this.table = table;
                    this.commitUser = commitUser;
                    this.bucketMode = table.bucketMode();

                    CoreOptions coreOptions = CoreOptions.fromMap(table.options());
                    this.writeSchema = PaimonWriteSchema.create(
                            table.rowType(), columnNames, changelogWrite);
                    this.arrowAdapter = new PaimonArrowBatchAdapter(
                            writeSchema.inputType(), ZoneId.of(timeZone), allocator);
                    validateWriteColumnsForMergeEngine(
                            columnNames.length - (changelogWrite ? 1 : 0), coreOptions);
                    this.fullCompactionChangelog =
                            !coreOptions.writeOnly()
                                    && coreOptions.changelogProducer()
                                    == CoreOptions.ChangelogProducer.FULL_COMPACTION;
                    openFileStoreWriter(
                            table,
                            commitUser,
                            overwrite,
                            spillDirectories,
                            coreOptions,
                            nativePageMemoryLimitBytes,
                            nativeMemoryManager);
                    return null;
                } catch (Throwable t) {
                    try {
                        closeResources();
                    } catch (Throwable closeFailure) {
                        t.addSuppressed(closeFailure);
                    }
                    throw new RuntimeException("PaimonJniWriter open failed", t);
                }
            });
        }
    }

    /** Return the exact Arrow schema derived from the pinned Paimon input type. */
    public byte[] getArrowSchema() {
        if (arrowAdapter == null) {
            throw new IllegalStateException("PaimonJniWriter is not open");
        }
        return arrowAdapter.serializedArrowSchema();
    }

    /**
     * Import and synchronously consume one C++ Arrow RecordBatch through the C Data Interface.
     *
     * <p>The native structs are valid only for this call. Import transfers the ArrowArray release
     * callbacks into Java. Closing the imported root releases the exported C++ buffers after Paimon
     * has consumed every row; the native caller releases only callbacks left by a partial import.
     */
    public void writeArrow(long arrayAddress, long schemaAddress) throws Exception {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            preExecutionAuthenticator.execute(() -> {
                try (ArrowArray array = ArrowArray.wrap(arrayAddress);
                        ArrowSchema schema = ArrowSchema.wrap(schemaAddress);
                        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
                        VectorSchemaRoot root = Data.importVectorSchemaRoot(
                                allocator, array, schema, dictionaries)) {
                    writeBatch(root);
                    return null;
                } catch (Throwable t) {
                    throw new RuntimeException("PaimonJniWriter C Data write failed", t);
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
     * Stop SDK writers before the coordinator can accept commit messages, while retaining the
     * table and prepared messages needed to abort a rejected report.
     */
    public void prepareCloseForCommit() throws Exception {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            if (preExecutionAuthenticator != null) {
                preExecutionAuthenticator.execute(() -> {
                    closePreparedResources();
                    return null;
                });
            } else {
                closePreparedResources();
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
            String spillDirectories, CoreOptions coreOptions, long nativePageMemoryLimitBytes,
            long nativeMemoryManager) throws Exception {
        writer = table.newWrite(commitUser);
        if (overwrite) {
            writer.withIgnorePreviousFiles(true);
        }
        openMemoryResources(table, coreOptions, spillDirectories, nativePageMemoryLimitBytes,
                nativeMemoryManager);
        openDynamicBucketAssigner(table, commitUser, overwrite, coreOptions);
    }

    private void validateWriteColumnsForMergeEngine(int writeColumnCount, CoreOptions coreOptions) {
        if (writeColumnCount == table.rowType().getFieldCount() || table.primaryKeys().isEmpty()) {
            return;
        }

        CoreOptions.MergeEngine mergeEngine = coreOptions.mergeEngine();
        if (mergeEngine != CoreOptions.MergeEngine.PARTIAL_UPDATE) {
            throw new UnsupportedOperationException(
                    "Paimon primary-key partial-column write requires "
                            + "merge-engine=partial-update, but table uses merge-engine="
                            + mergeEngine);
        }
    }

    private void openMemoryResources(
            FileStoreTable table,
            CoreOptions coreOptions,
            String spillDirectories,
            long nativePageMemoryLimitBytes,
            long nativeMemoryManager) throws Exception {
        int pageSize = coreOptions.pageSize();
        long writeBufferSize = coreOptions.writeBufferSize();
        // Paimon creates merge-tree bucket writers lazily on the first write. Their
        // SortBufferWriteBuffer requires three pages at construction time, so reject a permanent
        // per-writer capacity shortage during open instead of failing nondeterministically when a
        // particular bucket first receives a row. Paimon's MemoryPoolFactory shares these pages
        // among bucket owners; the requirement is three pages per Doris writer, not per bucket.
        long effectivePoolLimit = validateAndGetMemoryPoolLimit(writeBufferSize,
                nativePageMemoryLimitBytes, pageSize, !table.primaryKeys().isEmpty());
        DorisMemorySegmentPool memorySegmentPool =
                new DorisMemorySegmentPool(effectivePoolLimit, pageSize, nativeMemoryManager);
        MemoryPoolFactory memoryPoolFactory = new MemoryPoolFactory(memorySegmentPool);
        writer.withMemoryPoolFactory(memoryPoolFactory);
        LOG.info("Paimon writer uses Doris-managed memory pool: limit={} bytes, pageSize={}",
                memoryPoolFactory.totalBufferSize(), pageSize);

        if (!coreOptions.writeBufferSpillable()) {
            return;
        }

        String[] splitDirectories = IOManagerImpl.splitPaths(spillDirectories);
        for (String directory : splitDirectories) {
            Files.createDirectories(Paths.get(directory));
        }
        ioManager = IOManager.create(splitDirectories);
        writer.withIOManager(ioManager);
        LOG.info("Paimon writer spill enabled: dirs={}", spillDirectories);
    }

    static long validateAndGetMemoryPoolLimit(long writeBufferSize,
            long nativePageMemoryLimitBytes, int pageSize, boolean mergeTreeWriter) {
        long effectivePoolLimit = Math.min(writeBufferSize, nativePageMemoryLimitBytes);
        int requiredPages = mergeTreeWriter
                ? MERGE_TREE_WRITER_MIN_PAGES
                : APPEND_ONLY_WRITER_MIN_PAGES;
        long availablePages = effectivePoolLimit / pageSize;
        if (availablePages < requiredPages) {
            String writerType = mergeTreeWriter ? "merge-tree" : "append-only";
            throw new IllegalArgumentException("Paimon " + writerType
                    + " writer requires at least " + requiredPages
                    + " memory pages, but the effective pool contains " + availablePages
                    + " pages: effectivePoolLimit=" + effectivePoolLimit
                    + ", pageSize=" + pageSize
                    + ", writeBufferSize=" + writeBufferSize
                    + ", nativePageMemoryLimitBytes=" + nativePageMemoryLimitBytes
                    + ". Increase the query memory limit, reduce sink parallelism, or adjust "
                    + "paimon_jni_writer_memory_pool_limit_bytes, write-buffer-size, or page-size");
        }
        return effectivePoolLimit;
    }

    private void openDynamicBucketAssigner(FileStoreTable table, String commitUser,
            boolean overwrite, CoreOptions coreOptions) throws Exception {
        switch (bucketMode) {
            case HASH_DYNAMIC:
                openHashDynamicBucketAssigner(table, commitUser, overwrite, coreOptions);
                break;
            case KEY_DYNAMIC:
                openKeyDynamicBucketAssigner(table);
                break;
            default:
                // Fixed, unaware and postpone modes route through TableWrite.write(row).
                break;
        }
    }

    private void openHashDynamicBucketAssigner(FileStoreTable table, String commitUser,
            boolean overwrite, CoreOptions coreOptions) {
        dynamicBucketExtractor = new RowPartitionKeyExtractor(table.schema());
        if (overwrite) {
            hashBucketAssigner =
                    new SimpleHashBucketAssigner(
                            1,
                            0,
                            coreOptions.dynamicBucketTargetRowNum(),
                            coreOptions.dynamicBucketMaxBuckets());
            return;
        }

        hashBucketAssigner =
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        table.store().newIndexFileHandler(),
                        1,
                        1,
                        0,
                        coreOptions.dynamicBucketTargetRowNum(),
                        coreOptions.dynamicBucketMaxBuckets());
    }

    private void openKeyDynamicBucketAssigner(FileStoreTable table) throws Exception {
        globalIndexAssigner = new GlobalIndexAssigner(table);
        globalIndexAssigner.open(1, 0, this::writeAssignedRow);
        new IndexBootstrap(table).bootstrap(
                1, 0, this::bootstrapGlobalIndexKey);
        globalIndexAssigner.finishBootstrap();
    }

    // ────────────────────────────────────────────────────────────
    // Data writing
    // ────────────────────────────────────────────────────────────

    private void writeBatch(VectorSchemaRoot root) throws Exception {
        int rowCount = root.getRowCount();
        if (rowCount == 0) {
            return;
        }
        // The adapter exposes imported Arrow vectors directly as a Paimon columnar row. Only the
        // table-layout row is materialized; there is no decoded Arrow copy or Object[][] batch.
        PaimonArrowBatchAdapter.Rows rows = arrowAdapter.rows(root);
        for (int r = 0; r < rowCount; r++) {
            InternalRow row = writeSchema.tableRow(rows.row(r));
            switch (bucketMode) {
                case HASH_DYNAMIC:
                    writeHashDynamicRow(row);
                    break;
                case KEY_DYNAMIC:
                    globalIndexAssigner.processInput(row);
                    break;
                default:
                    writeRow(row);
                    break;
            }
        }
    }

    private void writeHashDynamicRow(InternalRow row) throws Exception {
        int bucket =
                hashBucketAssigner.assign(
                        dynamicBucketExtractor.partition(row),
                        dynamicBucketExtractor.trimmedPrimaryKey(row).hashCode());
        writeRow(row, bucket);
    }

    private void writeAssignedRow(InternalRow row, Integer bucket) {
        try {
            writeRow(row, bucket);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write Paimon key-dynamic bucket row", e);
        }
    }

    private void bootstrapGlobalIndexKey(InternalRow row) {
        try {
            globalIndexAssigner.bootstrapKey(row);
        } catch (Exception e) {
            throw new RuntimeException("Failed to bootstrap Paimon key-dynamic index", e);
        }
    }

    private void writeRow(InternalRow row) throws Exception {
        if (!fullCompactionChangelog) {
            writer.write(row);
            return;
        }

        trackFullCompactionBucket(writer.writeAndReturn(row));
    }

    private void writeRow(InternalRow row, int bucket) throws Exception {
        if (!fullCompactionChangelog) {
            writer.write(row, bucket);
            return;
        }

        trackFullCompactionBucket(writer.writeAndReturn(row, bucket));
    }

    private void trackFullCompactionBucket(SinkRecord sinkRecord) {
        if (sinkRecord == null) {
            return;
        }
        fullCompactionBuckets.add(
                new PartitionBucket(
                        sinkRecord.partition().copy(), sinkRecord.bucket()));
    }

    // ────────────────────────────────────────────────────────────
    // Resource management
    // ────────────────────────────────────────────────────────────

    private void closeResources() throws Exception {
        try {
            closeWriter();
        } finally {
            writeSchema = null;
            arrowAdapter = null;
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
        }
    }

    private void closePreparedResources() throws Exception {
        if (sdkCloseFailed) {
            throw new IllegalStateException(
                    "A previous Paimon SDK close failed; native memory cannot be released safely");
        }
        Exception failure = closeResource(writer, null);
        failure = closeResource(globalIndexAssigner, failure);
        failure = closeResource(ioManager, failure);
        writer = null;
        hashBucketAssigner = null;
        dynamicBucketExtractor = null;
        globalIndexAssigner = null;
        ioManager = null;
        fullCompactionBuckets.clear();
        if (allocator != null) {
            failure = closeResource(allocator, failure);
            allocator = null;
        }
        if (failure != null) {
            sdkCloseFailed = true;
            throw failure;
        }
    }

    private List<CommitMessage> prepareCommitMessages() throws Exception {
        if (writer == null) {
            throw new IllegalStateException("Paimon writer is not open");
        }
        prepareDynamicBucketCommit();
        submitFullCompaction();
        List<CommitMessage> messages = commitIdentifier > 0
                ? writer.prepareCommit(true, commitIdentifier)
                : writer.prepareCommit();
        preparedCommitMessages = new ArrayList<>(messages);
        return messages;
    }

    private void prepareDynamicBucketCommit() throws Exception {
        if (hashBucketAssigner != null) {
            hashBucketAssigner.prepareCommit(commitIdentifier);
        }
    }

    private void submitFullCompaction() throws Exception {
        if (!fullCompactionChangelog || fullCompactionBuckets.isEmpty()) {
            return;
        }
        LOG.info("PaimonJniWriter submitting full compaction for {} buckets",
                fullCompactionBuckets.size());
        Iterator<PartitionBucket> iterator = fullCompactionBuckets.iterator();
        while (iterator.hasNext()) {
            PartitionBucket partitionBucket = iterator.next();
            writer.compact(partitionBucket.partition, partitionBucket.bucket, true);
            iterator.remove();
        }
    }

    private void closeWriter() throws Exception {
        if (sdkCloseFailed) {
            throw new IllegalStateException(
                    "A previous Paimon SDK close failed; native memory cannot be released safely");
        }
        Exception failure = closeResource(writer, null);
        failure = closeResource(globalIndexAssigner, failure);
        failure = closeResource(ioManager, failure);
        clearWriterState();
        if (failure != null) {
            sdkCloseFailed = true;
            throw failure;
        }
    }

    private static Exception closeResource(AutoCloseable resource, Exception previousFailure) {
        if (resource == null) {
            return previousFailure;
        }
        try {
            resource.close();
        } catch (Exception closeFailure) {
            if (previousFailure == null) {
                return closeFailure;
            }
            previousFailure.addSuppressed(closeFailure);
        }
        return previousFailure;
    }

    private void clearWriterState() {
        writer = null;
        table = null;
        commitIdentifier = 0;
        commitUser = null;
        bucketMode = null;
        hashBucketAssigner = null;
        dynamicBucketExtractor = null;
        globalIndexAssigner = null;
        ioManager = null;
        fullCompactionChangelog = false;
        fullCompactionBuckets.clear();
        preparedCommitMessages = Collections.emptyList();
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

    static native ByteBuffer allocatePaimonMemoryPage(long nativeMemoryManager, int bytes);

    private static class PartitionBucket {
        private final BinaryRow partition;
        private final int bucket;

        private PartitionBucket(BinaryRow partition, int bucket) {
            this.partition = partition;
            this.bucket = bucket;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof PartitionBucket)) {
                return false;
            }
            PartitionBucket that = (PartitionBucket) other;
            return bucket == that.bucket && partition.equals(that.partition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket);
        }
    }

}
