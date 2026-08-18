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
import org.apache.doris.kerberos.PreExecutionAuthenticator;
import org.apache.doris.kerberos.PreExecutionAuthenticatorCache;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.index.SimpleHashBucketAssigner;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
 *   C++ Block → Arrow IPC Stream → JNI direct ByteBuffer
 *   → PaimonJniWriter.write(directBuffer)
 *   → ArrowStreamReader → VectorSchemaRoot
 *   → PaimonArrowConverter (row-at-a-time typed extraction)
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

    private final ClassLoader classLoader;
    private final PaimonCommitCodec commitCodec = new PaimonCommitCodec();

    private BufferAllocator allocator;
    private PreExecutionAuthenticator preExecutionAuthenticator;
    private PaimonArrowConverter arrowConverter;

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
    /** Whether every row of this write is a delete (row-level DELETE, either shape). */
    private boolean rowKindDelete;
    /** True when this delete targets an append-only table, i.e. it is recorded as deleted POSITIONS. */
    private boolean deletionVectorDelete;
    /** Accumulates deleted positions; non-null only for {@link #deletionVectorDelete}. */
    private PaimonDeletionVectorCollector deletionVectorCollector;
    /** Re-derives a deleted row's partition from its own partition columns; ditto. */
    private RowPartitionKeyExtractor deletionPartitionExtractor;
    /** True for an operation-tagged UPDATE/MERGE stream (primary-key tables only). */
    private boolean operationTaggedMerge;
    private final Set<PartitionBucket> fullCompactionBuckets = new HashSet<>();
    private List<CommitMessage> preparedCommitMessages = Collections.emptyList();
    private boolean sdkCloseFailed;

    public PaimonJniWriter() {
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
     * @param rowKindDelete  whether every row of this write is a DELETE (row-level DELETE on a
     *                       primary-key table): each row is stamped {@link RowKind#DELETE} so the
     *                       merge engine cancels it against the key instead of appending it
     * @param operationTaggedMerge whether the rows form an operation-tagged UPDATE/MERGE stream: each
     *                       row's leading tag column selects its {@link RowKind} (2/5 = DELETE, the
     *                       rest = keyed upsert). Primary-key tables only.
     * @param timeZone       normalized Doris session timezone used for Paimon LTZ values
     * @param spillDirectories Doris storage-root scoped directories for Paimon write-buffer spill
     * @param memoryPoolLimitBytes maximum Doris-managed Paimon write-buffer memory
     * @param nativeMemoryManager opaque BE manager used to allocate tracked native pages
     */
    public void open(String serializedTable, Map<String, String> hadoopConfig,
                     String[] columnNames, long transactionId, String commitUser,
                     boolean overwrite, boolean rowKindDelete, boolean operationTaggedMerge,
                     String timeZone, String spillDirectories,
                     long memoryPoolLimitBytes, long nativeMemoryManager) throws Exception {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            if (memoryPoolLimitBytes <= 0) {
                throw new IllegalArgumentException(
                        "PaimonJniWriter requires a positive memory pool limit");
            }
            if (nativeMemoryManager == 0) {
                throw new IllegalArgumentException(
                        "PaimonJniWriter requires a native memory manager");
            }
            this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopConfig);
            this.arrowConverter = new PaimonArrowConverter(ZoneId.of(timeZone));
            preExecutionAuthenticator.execute(() -> {
                try {
                    FileStoreTable table = PaimonUtils.deserialize(serializedTable);
                    LOG.info("PaimonJniWriter opening: table={}, columns={}",
                            table.fullName(), columnNames != null ? columnNames.length : 0);
                    this.commitIdentifier = transactionId;
                    this.table = table;
                    this.commitUser = commitUser;
                    this.rowKindDelete = rowKindDelete;
                    // A delete takes one of two shapes, decided by the TABLE, not by the statement:
                    //   - primary-key table : a keyed RowKind.DELETE record the merge engine cancels;
                    //   - append-only table : the row's POSITION recorded in a deletion vector.
                    // FE's validateRowLevelDmlMode already rejected the third shape (append-only with no
                    // deletion vectors), so reaching here with one means plan and writer disagree — fail
                    // loudly rather than append rows that were meant to be removed.
                    this.deletionVectorDelete = rowKindDelete && table.primaryKeys().isEmpty();
                    if (this.deletionVectorDelete) {
                        if (!new CoreOptions(table.options()).deletionVectorsEnabled()) {
                            throw new IllegalArgumentException(
                                    "PaimonJniWriter got a DELETE for the append-only table "
                                            + table.fullName() + ", which has no deletion vectors enabled; "
                                            + "there is nowhere to record the deletion");
                        }
                        if (table.bucketMode() != BucketMode.BUCKET_UNAWARE) {
                            // A bucketed-append delete must group positions by the file's REAL bucket,
                            // which the locator does not carry yet. Reject rather than mis-file the
                            // deletion vector under bucket 0 and corrupt reads of the other buckets.
                            throw new IllegalArgumentException(
                                    "PaimonJniWriter supports append-only DELETE only for unaware-bucket "
                                            + "tables; " + table.fullName() + " pins a fixed bucket count");
                        }
                        this.deletionVectorCollector = new PaimonDeletionVectorCollector(table);
                        this.deletionPartitionExtractor = new RowPartitionKeyExtractor(table.schema());
                    }
                    this.operationTaggedMerge = operationTaggedMerge;
                    if (operationTaggedMerge && table.primaryKeys().isEmpty()) {
                        // An append-only UPDATE/MERGE needs deletion-vector marks PLUS appended
                        // replacement rows in one write; only the keyed-upsert shape is implemented.
                        throw new IllegalArgumentException(
                                "PaimonJniWriter supports operation-tagged UPDATE/MERGE only for "
                                        + "primary-key tables; " + table.fullName() + " has no primary key");
                    }
                    this.bucketMode = table.bucketMode();

                    CoreOptions coreOptions = CoreOptions.fromMap(table.options());
                    MemoryBudget memoryBudget = createMemoryBudget(
                            memoryPoolLimitBytes, coreOptions.pageSize(), bucketMode);
                    this.allocator = new RootAllocator(memoryBudget.arrowMemoryBytes);
                    LOG.info(
                            "Paimon writer memory budget: total={}, arrow={}, writer={}, "
                                    + "globalIndexLookup={}, globalIndexWriteBuffer={}",
                            memoryPoolLimitBytes,
                            memoryBudget.arrowMemoryBytes,
                            memoryBudget.writerMemoryBytes,
                            memoryBudget.globalIndexLookupMemoryBytes,
                            memoryBudget.globalIndexWriteBufferBytes);
                    this.writeSchema = PaimonWriteSchema.create(table.rowType(), columnNames);
                    // The partial-column check compares TABLE columns only: the synthetic leaders a
                    // row-level DML plan appends (operation tag, row locator) are consumed by this
                    // writer and never reach the paimon row, so they must not count as a "partial"
                    // write against a deduplicate merge engine.
                    int tableColumnCount = columnNames.length
                            - (writeSchema.hasRowId() ? 1 : 0)
                            - (writeSchema.hasOperation() ? 1 : 0);
                    validateWriteColumnsForMergeEngine(tableColumnCount, coreOptions);
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
                            memoryBudget,
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

    /**
     * Write a batch of rows from an Arrow IPC Stream buffer.
     *
     * <p>Called from C++ {@code JniPaimonWriter::_write_projected_block()}
     * once per Block. The buffer is a zero-copy direct view of the native
     * Arrow IPC Stream bytes. Rows are deserialized, normalized to table-schema
     * order, and handed to Paimon's writer and bucket assigner APIs. The SDK
     * owns partition/bucket semantics, buffering, spill, and file rolling.
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
                    throw new RuntimeException(
                            "PaimonJniWriter write failed: bytes=" + directBuffer.capacity(), t);
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

    /**
     * Aborts prepared files after the writer itself has closed and FE explicitly rejected the
     * final ownership report.
     */
    public static void abortPreparedCommit(String serializedTable,
            Map<String, String> hadoopConfig, String commitUser, byte[][] payloads) throws Exception {
        ClassLoader classLoader = PaimonJniWriter.class.getClassLoader();
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            PreExecutionAuthenticator authenticator =
                    PreExecutionAuthenticatorCache.getAuthenticator(hadoopConfig);
            authenticator.execute(() -> {
                FileStoreTable table = PaimonUtils.deserialize(serializedTable);
                List<CommitMessage> messages = new PaimonCommitCodec().decode(payloads);
                if (messages.isEmpty()) {
                    return null;
                }
                InnerTableCommit committer = table.newCommit(commitUser);
                try {
                    committer.abort(messages);
                    return null;
                } finally {
                    committer.close();
                }
            });
        }
    }

    // ────────────────────────────────────────────────────────────
    // Initialization helpers
    // ────────────────────────────────────────────────────────────

    private void openFileStoreWriter(FileStoreTable table, String commitUser, boolean overwrite,
            String spillDirectories, CoreOptions coreOptions, MemoryBudget memoryBudget,
            long nativeMemoryManager) throws Exception {
        writer = table.newWrite(commitUser);
        if (overwrite) {
            writer.withIgnorePreviousFiles(true);
        }
        openMemoryResources(
                coreOptions, spillDirectories, memoryBudget.writerMemoryBytes, nativeMemoryManager);
        openDynamicBucketAssigner(table, commitUser, overwrite, coreOptions, memoryBudget);
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

    static MemoryBudget createMemoryBudget(
            long totalBytes, int pageSize, BucketMode bucketMode) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("Paimon page size must be positive");
        }
        boolean keyDynamic = bucketMode == BucketMode.KEY_DYNAMIC;
        long minimumBytes = Math.multiplyExact((long) pageSize, keyDynamic ? 5L : 2L);
        if (totalBytes < minimumBytes) {
            throw new IllegalArgumentException(
                    "Paimon writer memory budget is too small for " + bucketMode
                            + ": totalBytes=" + totalBytes + ", minimumBytes=" + minimumBytes);
        }

        if (!keyDynamic) {
            long writerBytes = alignDownToPage(totalBytes / 2, pageSize);
            return new MemoryBudget(totalBytes - writerBytes, writerBytes, 0, 0);
        }

        long componentBytes = totalBytes / 4;
        long writerBytes = alignDownToPage(componentBytes, pageSize);
        long indexWriteBufferBytes = Math.max(
                Math.multiplyExact((long) pageSize, 2L),
                alignDownToPage(componentBytes, pageSize));
        long indexLookupBytes = componentBytes;
        long arrowBytes = totalBytes - writerBytes - indexLookupBytes - indexWriteBufferBytes;
        if (arrowBytes <= 0) {
            throw new IllegalArgumentException(
                    "Paimon key-dynamic writer has no Arrow memory after budget allocation");
        }
        return new MemoryBudget(
                arrowBytes, writerBytes, indexLookupBytes, indexWriteBufferBytes);
    }

    private static long alignDownToPage(long bytes, int pageSize) {
        return bytes - bytes % pageSize;
    }

    static FileStoreTable tableWithGlobalIndexMemoryLimits(
            FileStoreTable table, MemoryBudget memoryBudget) {
        Options options = Options.fromMap(new HashMap<>(table.options()));
        options.set(
                CoreOptions.LOOKUP_CACHE_MAX_MEMORY_SIZE,
                new MemorySize(memoryBudget.globalIndexLookupMemoryBytes));
        options.set(
                CoreOptions.WRITE_BUFFER_SIZE,
                new MemorySize(memoryBudget.globalIndexWriteBufferBytes));
        return table.copy(options.toMap());
    }

    private void openMemoryResources(
            CoreOptions coreOptions,
            String spillDirectories,
            long memoryPoolLimitBytes,
            long nativeMemoryManager) throws Exception {
        int pageSize = coreOptions.pageSize();
        long effectivePoolLimit = Math.min(coreOptions.writeBufferSize(), memoryPoolLimitBytes);
        DorisMemorySegmentPool memorySegmentPool =
                new DorisMemorySegmentPool(effectivePoolLimit, pageSize, nativeMemoryManager);
        MemoryPoolFactory memoryPoolFactory = new MemoryPoolFactory(memorySegmentPool);
        writer.withMemoryPoolFactory(memoryPoolFactory);
        LOG.info("Paimon writer uses Doris-managed memory pool: limit={} bytes, pageSize={}",
                memoryPoolFactory.totalBufferSize(), pageSize);

        boolean writerSpillEnabled = coreOptions.writeBufferSpillable();
        if (!writerSpillEnabled && bucketMode != BucketMode.KEY_DYNAMIC) {
            return;
        }

        String[] splitDirectories = IOManagerImpl.splitPaths(spillDirectories);
        for (String directory : splitDirectories) {
            Files.createDirectories(Paths.get(directory));
        }
        ioManager = IOManager.create(splitDirectories);
        if (writerSpillEnabled) {
            writer.withIOManager(ioManager);
        }
        LOG.info("Paimon writer IOManager enabled: dirs={}, writerSpillEnabled={}",
                spillDirectories, writerSpillEnabled);
    }

    private void openDynamicBucketAssigner(FileStoreTable table, String commitUser,
            boolean overwrite, CoreOptions coreOptions, MemoryBudget memoryBudget) throws Exception {
        switch (bucketMode) {
            case HASH_DYNAMIC:
                openHashDynamicBucketAssigner(table, commitUser, overwrite, coreOptions);
                break;
            case KEY_DYNAMIC:
                openKeyDynamicBucketAssigner(table, overwrite, memoryBudget);
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

    private void openKeyDynamicBucketAssigner(
            FileStoreTable table, boolean overwrite, MemoryBudget memoryBudget) throws Exception {
        if (ioManager == null) {
            throw new IllegalStateException("Paimon key-dynamic writer requires an IOManager");
        }
        FileStoreTable indexTable = tableWithGlobalIndexMemoryLimits(table, memoryBudget);
        globalIndexAssigner = new GlobalIndexAssigner(indexTable);
        globalIndexAssigner.open(
                memoryBudget.globalIndexLookupMemoryBytes,
                null,
                ioManager,
                1,
                0,
                this::writeAssignedRow);
        if (!overwrite) {
            new IndexBootstrap(indexTable).bootstrap(
                    1, 0, this::bootstrapGlobalIndexKey);
        }
        globalIndexAssigner.endBoostrap(false);
    }

    // ────────────────────────────────────────────────────────────
    // Data writing
    // ────────────────────────────────────────────────────────────

    private void writeBatch(VectorSchemaRoot root) throws Exception {
        int rowCount = root.getRowCount();
        if (rowCount == 0) {
            return;
        }
        // Convert and write one row at a time. Keeping only one row of boxed values
        // avoids retaining a second, Object[][] representation of the full Arrow batch.
        PaimonArrowConverter.RowReader rows =
                arrowConverter.rows(root, writeSchema.targetTypes());
        for (int r = 0; r < rowCount; r++) {
            Object[] values = rows.values(r);
            if (deletionVectorDelete) {
                // An append-only delete never reaches the SDK writer: there is no key to cancel, so the
                // row's POSITION is recorded in a deletion vector instead. The position rides in the
                // synthetic row-id column the connector declared for this table.
                collectDeletedPosition(values);
                continue;
            }
            GenericRow row = writeSchema.tableRow(values);
            if (rowKindDelete) {
                // A primary-key delete is a DELETE-kind record carrying the key: the merge engine cancels
                // it against the existing row. Stamped here, after the row is built and BEFORE bucket
                // assignment, because the bucket is derived from the key, which a DELETE row still carries.
                row.setRowKind(RowKind.DELETE);
            } else if (operationTaggedMerge) {
                // An UPDATE/MERGE stream tags every row with its merge operation. The delete-shaped tags
                // (DELETE=2, UPDATE_DELETE=5) become keyed DELETE records; everything else (INSERT=1,
                // UPDATE=3, UPDATE_INSERT=4) is a keyed upsert, which on a primary-key table IS the
                // update. Same-key delete+insert pairs in one batch keep their arrival order, which the
                // LSM sequence number preserves.
                byte op = writeSchema.operationValue(values);
                if (op == 2 || op == 5) {
                    row.setRowKind(RowKind.DELETE);
                }
            }
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

    /**
     * Records one append-only deletion from the synthetic row-id column.
     *
     * <p>The BE Paimon reader materializes {@code __DORIS_PAIMON_ROWID_COL__} as a
     * {@code STRUCT<file_path STRING, row_position BIGINT>} per scanned row; the deletion vector keys
     * on the data file's NAME plus that ordinal. The partition is re-derived from the row's own
     * partition columns — the same extractor the write path uses — so a partitioned delete lands in
     * the right deletion-vector index.
     */
    private void collectDeletedPosition(Object[] values) throws Exception {
        GenericRow rowId = writeSchema.rowIdValue(values);
        if (rowId == null || rowId.isNullAt(0) || rowId.isNullAt(1)) {
            // The plan promised a locator for every scanned row; a missing one means the scan and this
            // writer disagree about the projection. Failing beats silently not deleting the row.
            throw new IllegalStateException(
                    "PaimonJniWriter got an append-only DELETE row without a row locator; the scan did "
                            + "not materialize " + PaimonWriteSchema.ROWID_COL);
        }
        String filePath = rowId.getString(0).toString();
        long rowPosition = rowId.getLong(1);
        // The deletion vector keys on the file NAME; the reader emits the full path.
        String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
        GenericRow dataRow = writeSchema.tableRow(values);
        BinaryRow partition = deletionPartitionExtractor.partition(dataRow);
        deletionVectorCollector.add(partition, 0, fileName, rowPosition);
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
            arrowConverter = null;
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
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
        if (deletionVectorCollector != null && !deletionVectorCollector.isEmpty()) {
            // An append-only delete writes no data rows; its whole output is the deletion-vector index
            // files the collector persists here, merged with existing vectors as of the latest snapshot.
            messages = new ArrayList<>(messages);
            messages.addAll(deletionVectorCollector.persist());
        }
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

    static final class MemoryBudget {
        final long arrowMemoryBytes;
        final long writerMemoryBytes;
        final long globalIndexLookupMemoryBytes;
        final long globalIndexWriteBufferBytes;

        private MemoryBudget(
                long arrowMemoryBytes,
                long writerMemoryBytes,
                long globalIndexLookupMemoryBytes,
                long globalIndexWriteBufferBytes) {
            this.arrowMemoryBytes = arrowMemoryBytes;
            this.writerMemoryBytes = writerMemoryBytes;
            this.globalIndexLookupMemoryBytes = globalIndexLookupMemoryBytes;
            this.globalIndexWriteBufferBytes = globalIndexWriteBufferBytes;
        }
    }

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
