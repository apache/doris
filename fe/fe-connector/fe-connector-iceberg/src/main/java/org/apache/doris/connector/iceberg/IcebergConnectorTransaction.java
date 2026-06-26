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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import com.google.common.base.Preconditions;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Iceberg connector transaction (ports the legacy
 * {@code org.apache.doris.datasource.iceberg.IcebergTransaction} write lifecycle to the connector SPI).
 *
 * <p>Holds a single SDK {@link Transaction} / {@link Table} for one SQL statement and the accumulated
 * commit fragments ({@link TIcebergCommitData}, fed back from BE via {@link #addCommitData}). The SDK
 * transaction is opened — through the {@link IcebergCatalogOps} seam wrapped in the auth context — by
 * {@link #beginWrite}; {@link #commit()} builds the SDK operation for the {@link WriteOperation} from the
 * accumulated fragments, stages it onto the transaction, then flushes with {@code commitTransaction()}.</p>
 *
 * <p><b>Op selection (P6.3-T04).</b> Unlike the legacy class (which split {@code finishInsert} from
 * {@code commit}), the unified {@link ConnectorTransaction} SPI exposes only {@link #commit()} — so the
 * manifest build happens there (mirroring maxcompute): INSERT &rarr; AppendFiles; OVERWRITE dynamic &rarr;
 * ReplacePartitions; OVERWRITE empty/unpartitioned &rarr; OverwriteFiles (clears the table); OVERWRITE static
 * &rarr; OverwriteFiles.overwriteByRowFilter; DELETE &rarr; RowDelta (deletes); UPDATE/MERGE &rarr; RowDelta
 * (rows + deletes). The {@code IcebergWriterHelper} equivalents (DataFile / DeleteFile / Metrics /
 * PartitionData) live in {@link IcebergWriterHelper}.</p>
 *
 * <p><b>Conflict detection (P6.3-T05).</b> The DELETE/MERGE {@link RowDelta} commit is guarded by the
 * optimistic conflict-detection validation suite (validateFromSnapshot from the begin-time
 * {@link #baseSnapshotId} / conflictDetectionFilter / serializable validateNoConflictingDataFiles /
 * validateDeletedFiles / validateNoConflictingDeleteFiles / validateDataFilesExist) and, on a V3 table, the
 * deletion-vector "rewrite previous delete files" {@code removeDeletes}. The conflict-detection filter is the
 * O5-2 write constraint ({@link #applyWriteConstraint}, a neutral {@link ConnectorPredicate} converted lazily
 * at commit) ANDed with a commit-time identity-partition filter derived from the commit fragments.</p>
 *
 * <p><b>Gate-closed / dormant.</b> Iceberg is not in {@code SPI_READY_TYPES} until the P6.6 cutover, so
 * nothing routes plugin-driven iceberg writes through this class yet ({@link #beginWrite} is wired by T06's
 * {@code planWrite}). The txn-id is the engine-allocated Doris global id, so the generic
 * {@code PluginDrivenTransactionManager} registers it in both the per-manager map and
 * {@code GlobalExternalTransactionInfoMgr} — no per-connector registration code is needed, mirroring
 * maxcompute.</p>
 */
public class IcebergConnectorTransaction implements ConnectorTransaction {

    private static final Logger LOG = LogManager.getLogger(IcebergConnectorTransaction.class);
    private static final String DELETE_ISOLATION_LEVEL = "delete_isolation_level";
    private static final String DELETE_ISOLATION_LEVEL_DEFAULT = "serializable";

    private final long transactionId;
    private final IcebergCatalogOps catalogOps;
    private final ConnectorContext context;
    private final List<TIcebergCommitData> commitDataList = new ArrayList<>();

    // The single SDK transaction / table, opened lazily by beginWrite (the write plan binds the target
    // table only when the sink is planned). volatile: addCommitData / commit may run on different threads.
    private volatile Transaction transaction;
    private volatile Table table;

    // Op context captured at begin time, consumed by commit() (the volatile transaction write at the end of
    // beginWrite publishes these plain writes to the commit thread).
    private WriteOperation writeOperation = WriteOperation.INSERT;
    private boolean staticPartitionOverwrite;
    private Map<String, String> staticPartitionValues = Collections.emptyMap();
    private String branchName;
    // The current snapshot pinned at begin time for a DELETE/MERGE (null for INSERT/OVERWRITE). Consumed by
    // the commit validation suite (validateFromSnapshot).
    private Long baseSnapshotId;
    // Session zone for human-readable TIMESTAMP partition value parsing (DV-T04-f).
    private ZoneId zone = ZoneOffset.UTC;

    // ── REWRITE (rewrite_data_files, P6.4-T06; dormant until the P6.6 cutover) ──
    // The original data files to remove, fed by updateRewriteFiles (the rewrite execution half hands it the
    // planner's RewriteDataGroup.getDataFiles() — FileScanTask.file()). The new compacted files arrive on the
    // shared commitDataList channel (like INSERT) and are materialized into filesToAdd at commit time.
    private final List<DataFile> filesToDelete = new ArrayList<>();
    private final List<DataFile> filesToAdd = new ArrayList<>();
    // OCC anchor: the current snapshot captured at begin time, passed verbatim to
    // RewriteFiles.validateFromSnapshot (-1 when the table has no snapshot). REWRITE uses this, NOT
    // baseSnapshotId (which drives the RowDelta path). Ported from legacy IcebergTransaction.startingSnapshotId.
    private long startingSnapshotId = -1L;

    // O5-2: the engine-extracted target-only write constraint (neutral form), stashed by applyWriteConstraint
    // at plan time and converted to an iceberg Expression lazily at commit (the table schema is only known
    // after beginWrite has loaded the table). volatile: applyWriteConstraint and commit may run on different
    // threads.
    private volatile ConnectorPredicate writeConstraint;

    public IcebergConnectorTransaction(long transactionId, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        this.transactionId = transactionId;
        this.catalogOps = catalogOps;
        this.context = context;
    }

    /**
     * Opens the single SDK transaction for {@code db.tableName} and applies the op-specific begin guards,
     * loading the table through the {@link IcebergCatalogOps} seam wrapped in the FE-injected auth context
     * (Kerberos UGI), mirroring {@code IcebergConnectorMetadata.loadTable} and legacy
     * {@code IcebergTransaction.begin{Insert,Delete,Merge}}.
     *
     * <p>Guards: an INSERT/OVERWRITE that targets a branch validates it exists and is a branch (not a tag); a
     * DELETE/MERGE requires format-version &ge; 2 (position deletes) and captures {@link #baseSnapshotId}.
     * Both {@code loadTable} and {@code newTransaction()} run inside {@code executeAuthenticated}:
     * {@code BaseTable.newTransaction()} issues an unconditional {@code TableOperations.refresh()} (a remote
     * metastore call), so it must carry the same auth context as the load (UT-invisible offline; verified at
     * P6.6 docker on a Kerberized HMS).</p>
     */
    public void beginWrite(ConnectorSession session, String db, String tableName, IcebergWriteContext ctx) {
        this.writeOperation = ctx.getWriteOperation();
        this.staticPartitionOverwrite = ctx.isStaticPartitionOverwrite();
        this.staticPartitionValues = ctx.getStaticPartitionValues();
        this.zone = IcebergTimeUtils.resolveSessionZone(session);
        try {
            context.executeAuthenticated(() -> {
                Table loaded = catalogOps.loadTable(db, tableName);
                this.table = loaded;
                applyBeginGuards(ctx, tableName);
                this.transaction = loaded.newTransaction();
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to begin write for iceberg table " + tableName + ": " + e.getMessage(), e);
        }
    }

    private void applyBeginGuards(IcebergWriteContext ctx, String tableName) {
        WriteOperation op = ctx.getWriteOperation();
        if (op == WriteOperation.REWRITE) {
            // rewrite_data_files works directly on the main table (legacy IcebergTransaction.beginRewrite:175):
            // never targets a branch, never pins baseSnapshotId; instead it captures the current snapshot as the
            // OCC anchor for the commit-time RewriteFiles.validateFromSnapshot (-1 when the table has none).
            this.branchName = null;
            this.baseSnapshotId = null;
            Long current = getSnapshotIdIfPresent(table);
            this.startingSnapshotId = current != null ? current : -1L;
            return;
        }
        if (op == WriteOperation.DELETE || op == WriteOperation.UPDATE || op == WriteOperation.MERGE) {
            // RowDelta path: the merge/delete write never targets a branch (legacy beginMerge forces null).
            this.branchName = null;
            // [SHOULD-2] / Fix B: anchor baseSnapshotId at the statement's READ snapshot (the MVCC pin the
            // scan used, S_read), threaded onto the write handle and carried on the ctx. The commit-time
            // removeDeletes (option D) re-derives from baseSnapshotId, and BE unions the scan-time (S_read)
            // old deletes into the new DV — anchoring both at S_read keeps supply and remove on one snapshot
            // (no resurrection under a concurrent commit in the read->begin-write window). A -1 readSnapshotId
            // (no pin: a caller without the threaded handle) falls back to the begin-time current snapshot.
            long pinnedReadSnapshot = ctx.getReadSnapshotId();
            // Keep both ternary arms boxed (Long): getSnapshotIdIfPresent returns null for an empty table
            // (no snapshot), and a primitive arm would force-unbox that null into an NPE.
            this.baseSnapshotId = pinnedReadSnapshot >= 0
                    ? Long.valueOf(pinnedReadSnapshot) : getSnapshotIdIfPresent(table);
            if (table instanceof HasTableOperations) {
                int formatVersion = ((HasTableOperations) table).operations().current().formatVersion();
                if (formatVersion < 2) {
                    throw new IllegalArgumentException("Iceberg table " + tableName
                            + " must have format version 2 or higher for position deletes");
                }
            }
        } else {
            // INSERT / OVERWRITE (append path).
            this.baseSnapshotId = null;
            if (ctx.getBranchName().isPresent()) {
                this.branchName = ctx.getBranchName().get();
                SnapshotRef branchRef = table.refs().get(branchName);
                if (branchRef == null) {
                    throw new IllegalArgumentException(branchName + " is not founded in " + tableName);
                } else if (!branchRef.isBranch()) {
                    throw new IllegalArgumentException(branchName
                            + " is a tag, not a branch. Tags cannot be targets for producing snapshots");
                }
            } else {
                this.branchName = null;
            }
        }
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void addCommitData(byte[] commitFragment) {
        TIcebergCommitData data = new TIcebergCommitData();
        try {
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(data, commitFragment);
        } catch (TException e) {
            throw new DorisConnectorException("failed to deserialize Iceberg commit data", e);
        }
        synchronized (this) {
            commitDataList.add(data);
        }
    }

    /**
     * O5-2: stashes the engine-extracted target-only write constraint (neutral form). It is converted to an
     * iceberg {@link Expression} lazily at commit time ({@link #buildWriteConstraintExpression}) — the table
     * schema needed by {@link IcebergPredicateConverter} is only available after {@link #beginWrite}, and the
     * engine calls this at plan time, before begin. Mirrors legacy
     * {@code IcebergTransaction.setConflictDetectionFilter}, except the connector receives the neutral
     * predicate (not an already-converted iceberg expression).
     */
    @Override
    public void applyWriteConstraint(ConnectorPredicate targetOnlyFilter) {
        this.writeConstraint = targetOnlyFilter;
    }

    /**
     * REWRITE: registers original data files to remove (the rewrite execution half feeds it one bin-packed
     * group at a time — {@code RewriteDataGroup.getDataFiles()}). Accumulates across calls. Ported from legacy
     * {@code IcebergTransaction.updateRewriteFiles}; package-visible because the rewrite coordinator lives in
     * the connector (fe-core cannot traffic in iceberg {@code DataFile}). Dormant until the P6.6 cutover.
     */
    void updateRewriteFiles(List<DataFile> originalFiles) {
        synchronized (filesToDelete) {
            filesToDelete.addAll(originalFiles);
        }
    }

    /**
     * Affected-row count for the statement, ported verbatim from legacy
     * {@code IcebergTransaction.getUpdateCnt}: prefer {@code affected_rows} over {@code row_count}, split
     * data-file rows from delete-file rows (position deletes / deletion vectors), and return the data
     * rows when present, else the delete rows. For UPDATE/MERGE the data rows already equal the affected
     * rows; the internal position deletes must not be double-counted.
     */
    @Override
    public long getUpdateCnt() {
        long dataRows = 0;
        long deleteRows = 0;
        for (TIcebergCommitData commitData : commitDataList) {
            long affectedRows = commitData.isSetAffectedRows()
                    ? commitData.getAffectedRows()
                    : commitData.getRowCount();
            if (commitData.isSetFileContent()
                    && (commitData.getFileContent() == TFileContent.POSITION_DELETES
                    || commitData.getFileContent() == TFileContent.DELETION_VECTOR)) {
                deleteRows += affectedRows;
            } else {
                dataRows += affectedRows;
            }
        }
        return dataRows > 0 ? dataRows : deleteRows;
    }

    @Override
    public String profileLabel() {
        return "ICEBERG";
    }

    @Override
    public void commit() {
        if (transaction == null) {
            throw new DorisConnectorException("no active iceberg transaction to commit");
        }
        try {
            // Build the SDK operation (manifest scan hits the remote metastore) and flush it, both under the
            // FE-injected auth context (Kerberos UGI), mirroring legacy finish*/commitTransaction.
            context.executeAuthenticated(() -> {
                buildPendingOperation();
                transaction.commitTransaction();
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to commit iceberg transaction: " + e.getMessage(), e);
        }
    }

    /** Dispatches the accumulated commit fragments onto the SDK operation for {@link #writeOperation}. */
    private void buildPendingOperation() {
        switch (writeOperation) {
            case INSERT:
                commitAppendTxn(buildDataWriteResults());
                break;
            case OVERWRITE:
                if (staticPartitionOverwrite) {
                    commitStaticPartitionOverwrite(buildDataWriteResults());
                } else {
                    commitReplaceTxn(buildDataWriteResults());
                }
                break;
            case DELETE:
                updateManifestAfterDelete();
                break;
            case UPDATE:
            case MERGE:
                updateManifestAfterMerge();
                break;
            case REWRITE:
                commitRewriteTxn();
                break;
            default:
                throw new DorisConnectorException("Unsupported iceberg write operation: " + writeOperation);
        }
    }

    private List<WriteResult> buildDataWriteResults() {
        if (commitDataList.isEmpty()) {
            return Collections.emptyList();
        }
        WriteResult writeResult = IcebergWriterHelper.convertToWriterResult(transaction.table(), commitDataList, zone);
        List<WriteResult> results = new ArrayList<>(1);
        results.add(writeResult);
        return results;
    }

    /**
     * REWRITE ({@code rewrite_data_files}): atomically replace the original data files ({@link #filesToDelete},
     * fed by {@link #updateRewriteFiles}) with the newly written compacted files via the SDK
     * {@link RewriteFiles} op, guarded by {@code validateFromSnapshot(startingSnapshotId)} (OCC). The new files
     * arrive on the shared {@link #commitDataList} channel and are materialized here. Ported from legacy
     * {@code IcebergTransaction.finishRewrite} &rarr; {@code updateManifestAfterRewrite}, folded into
     * {@code commit()} because the unified {@link ConnectorTransaction} exposes only {@code commit()} (mirroring
     * how INSERT folded {@code finishInsert} into {@code commitAppendTxn}). When there is nothing to delete and
     * nothing to add, the op is skipped — the empty SDK transaction still flushes (legacy :248-251).
     *
     * <p>Unlike legacy {@code updateManifestAfterRewrite:258}, the {@code scanManifestsWith(threadPool)}
     * manifest-scan parallelism is dropped, matching the connector's append path ({@link #commitAppendTxn}
     * also drops it) — a perf-only divergence registered in the deviations log (DV-T06r-scanpool).</p>
     */
    private void commitRewriteTxn() {
        convertCommitDataToFilesToAdd();
        if (filesToDelete.isEmpty() && filesToAdd.isEmpty()) {
            return;
        }
        RewriteFiles rewriteFiles = transaction.newRewrite();
        rewriteFiles = rewriteFiles.validateFromSnapshot(startingSnapshotId);
        for (DataFile dataFile : filesToDelete) {
            rewriteFiles.deleteFile(dataFile);
        }
        for (DataFile dataFile : filesToAdd) {
            rewriteFiles.addFile(dataFile);
        }
        rewriteFiles.commit();
    }

    /**
     * Materializes the BE-reported commit fragments into {@link #filesToAdd} through the same {@link WriteResult}
     * helper INSERT uses (legacy {@code IcebergTransaction.convertCommitDataListToDataFilesToAdd}). No-op when no
     * fragments were reported (an empty rewrite, or a group whose write produced nothing).
     */
    private void convertCommitDataToFilesToAdd() {
        if (commitDataList.isEmpty()) {
            return;
        }
        WriteResult writeResult =
                IcebergWriterHelper.convertToWriterResult(transaction.table(), commitDataList, zone);
        synchronized (filesToAdd) {
            filesToAdd.addAll(Arrays.asList(writeResult.dataFiles()));
        }
    }

    private void commitAppendTxn(List<WriteResult> pendingResults) {
        AppendFiles appendFiles = transaction.newAppend();
        if (branchName != null) {
            appendFiles = appendFiles.toBranch(branchName);
        }
        for (WriteResult result : pendingResults) {
            Preconditions.checkState(result.referencedDataFiles().length == 0,
                    "Should have no referenced data files for append.");
            Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
        }
        appendFiles.commit();
    }

    private void commitReplaceTxn(List<WriteResult> pendingResults) {
        if (pendingResults.isEmpty()) {
            // such as : insert overwrite table `dst_tb` select * from `empty_tb`
            // 1. if dst_tb is a partitioned table, it returns directly.
            // 2. if dst_tb is an unpartitioned table, the `dst_tb` table is emptied.
            if (!transaction.table().spec().isPartitioned()) {
                OverwriteFiles overwriteFiles = transaction.newOverwrite();
                if (branchName != null) {
                    overwriteFiles = overwriteFiles.toBranch(branchName);
                }
                try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
                    OverwriteFiles finalOverwriteFiles = overwriteFiles;
                    fileScanTasks.forEach(f -> finalOverwriteFiles.deleteFile(f.file()));
                } catch (IOException e) {
                    throw new DorisConnectorException("Failed to scan files for overwrite: " + e.getMessage(), e);
                }
                overwriteFiles.commit();
            }
            return;
        }

        ReplacePartitions appendPartitionOp = transaction.newReplacePartitions();
        if (branchName != null) {
            appendPartitionOp = appendPartitionOp.toBranch(branchName);
        }
        for (WriteResult result : pendingResults) {
            Preconditions.checkState(result.referencedDataFiles().length == 0,
                    "Should have no referenced data files.");
            Arrays.stream(result.dataFiles()).forEach(appendPartitionOp::addFile);
        }
        appendPartitionOp.commit();
    }

    /**
     * INSERT OVERWRITE ... PARTITION(col=val, ...): overwrite only the matching partitions via
     * {@code OverwriteFiles.overwriteByRowFilter}.
     */
    private void commitStaticPartitionOverwrite(List<WriteResult> pendingResults) {
        Table icebergTable = transaction.table();
        PartitionSpec spec = icebergTable.spec();
        Schema schema = icebergTable.schema();

        Expression partitionFilter = buildPartitionFilter(staticPartitionValues, spec, schema);

        OverwriteFiles overwriteFiles = transaction.newOverwrite();
        if (branchName != null) {
            overwriteFiles = overwriteFiles.toBranch(branchName);
        }
        overwriteFiles = overwriteFiles.overwriteByRowFilter(partitionFilter);

        for (WriteResult result : pendingResults) {
            Preconditions.checkState(result.referencedDataFiles().length == 0,
                    "Should have no referenced data files for static partition overwrite.");
            Arrays.stream(result.dataFiles()).forEach(overwriteFiles::addFile);
        }
        overwriteFiles.commit();
    }

    /**
     * Build an iceberg {@link Expression} from the static partition key-value pairs. Identity partitions
     * require the SOURCE column name (not the partition field name) in the expression.
     */
    private Expression buildPartitionFilter(Map<String, String> staticPartitions, PartitionSpec spec,
            Schema schema) {
        if (staticPartitions == null || staticPartitions.isEmpty()) {
            return Expressions.alwaysTrue();
        }

        List<Expression> predicates = new ArrayList<>();
        for (PartitionField field : spec.fields()) {
            String partitionColName = field.name();
            if (staticPartitions.containsKey(partitionColName)) {
                String partitionValueStr = staticPartitions.get(partitionColName);
                Types.NestedField sourceField = schema.findField(field.sourceId());
                if (sourceField == null) {
                    throw new DorisConnectorException(String.format(
                            "Source field not found for partition field: %s", partitionColName));
                }
                Object partitionValue = IcebergPartitionUtils.parsePartitionValueFromString(
                        partitionValueStr, sourceField.type(), zone);
                String sourceColName = sourceField.name();
                Expression eqExpr = partitionValue == null
                        ? Expressions.isNull(sourceColName)
                        : Expressions.equal(sourceColName, partitionValue);
                predicates.add(eqExpr);
            }
        }

        if (predicates.isEmpty()) {
            return Expressions.alwaysTrue();
        }
        Expression result = predicates.get(0);
        for (int i = 1; i < predicates.size(); i++) {
            result = Expressions.and(result, predicates.get(i));
        }
        return result;
    }

    /**
     * DELETE: commit position-delete files via {@link RowDelta}, guarded by the optimistic conflict-detection
     * validation suite ({@link #applyRowDeltaValidations}) and — on a V3 table — the deletion-vector
     * "rewrite previous delete files" {@code removeDeletes}. Ported from legacy
     * {@code IcebergTransaction.updateManifestAfterDelete}.
     */
    private void updateManifestAfterDelete() {
        FileFormat fileFormat = IcebergWriterHelper.getFileFormat(transaction.table());
        if (commitDataList.isEmpty()) {
            return;
        }
        List<DeleteFile> deleteFiles = convertCommitDataToDeleteFiles(fileFormat, commitDataList);
        List<DeleteFile> rewrittenDeleteFiles = shouldRewritePreviousDeleteFiles()
                ? collectRewrittenDeleteFiles(commitDataList)
                : Collections.emptyList();
        if (deleteFiles.isEmpty()) {
            return;
        }
        RowDelta rowDelta = transaction.newRowDelta();
        applyRowDeltaValidations(rowDelta, transaction.table(), commitDataList,
                collectReferencedDataFiles(commitDataList));
        for (DeleteFile deleteFile : deleteFiles) {
            rowDelta.addDeletes(deleteFile);
        }
        for (DeleteFile deleteFile : rewrittenDeleteFiles) {
            rowDelta.removeDeletes(deleteFile);
        }
        rowDelta.commit();
    }

    /**
     * UPDATE/MERGE: commit data + position-delete files via a single {@link RowDelta}, guarded by the
     * conflict-detection validation suite and V3 deletion-vector {@code removeDeletes}. Ported from legacy
     * {@code IcebergTransaction.updateManifestAfterMerge}.
     */
    private void updateManifestAfterMerge() {
        if (commitDataList.isEmpty()) {
            return;
        }
        FileFormat fileFormat = IcebergWriterHelper.getFileFormat(transaction.table());

        List<TIcebergCommitData> dataCommitData = new ArrayList<>();
        List<TIcebergCommitData> deleteCommitData = new ArrayList<>();
        for (TIcebergCommitData commitData : commitDataList) {
            if (commitData.isSetFileContent()
                    && (commitData.getFileContent() == TFileContent.POSITION_DELETES
                    || commitData.getFileContent() == TFileContent.DELETION_VECTOR)) {
                deleteCommitData.add(commitData);
            } else {
                dataCommitData.add(commitData);
            }
        }

        List<DataFile> dataFiles = new ArrayList<>();
        if (!dataCommitData.isEmpty()) {
            WriteResult writeResult = IcebergWriterHelper.convertToWriterResult(
                    transaction.table(), dataCommitData, zone);
            dataFiles.addAll(Arrays.asList(writeResult.dataFiles()));
        }

        List<DeleteFile> deleteFiles = convertCommitDataToDeleteFiles(fileFormat, deleteCommitData);
        List<DeleteFile> rewrittenDeleteFiles = shouldRewritePreviousDeleteFiles()
                ? collectRewrittenDeleteFiles(deleteCommitData)
                : Collections.emptyList();
        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            return;
        }

        RowDelta rowDelta = transaction.newRowDelta();
        // Conflict filter spans the whole statement (commitDataList); referenced data files come from the
        // delete fragments only (legacy IcebergTransaction.updateManifestAfterMerge:490-491).
        applyRowDeltaValidations(rowDelta, transaction.table(), commitDataList,
                collectReferencedDataFiles(deleteCommitData));
        for (DataFile dataFile : dataFiles) {
            rowDelta.addRows(dataFile);
        }
        for (DeleteFile deleteFile : deleteFiles) {
            rowDelta.addDeletes(deleteFile);
        }
        for (DeleteFile deleteFile : rewrittenDeleteFiles) {
            rowDelta.removeDeletes(deleteFile);
        }
        rowDelta.commit();
    }

    /**
     * Group the delete commit fragments by their partition spec id (delete files may belong to an older spec
     * after partition evolution) and convert each group with its own {@link PartitionSpec}. A fragment with no
     * spec id is only valid for an unpartitioned table.
     */
    private List<DeleteFile> convertCommitDataToDeleteFiles(FileFormat fileFormat,
            List<TIcebergCommitData> commitData) {
        if (commitData.isEmpty()) {
            return Collections.emptyList();
        }

        PartitionSpec currentSpec = transaction.table().spec();
        Map<Integer, PartitionSpec> specsById = transaction.table().specs();
        Map<Integer, List<TIcebergCommitData>> commitDataBySpecId = new HashMap<>();
        List<TIcebergCommitData> missingSpecId = new ArrayList<>();

        for (TIcebergCommitData data : commitData) {
            if (data.isSetPartitionSpecId()) {
                commitDataBySpecId.computeIfAbsent(data.getPartitionSpecId(), k -> new ArrayList<>()).add(data);
            } else {
                missingSpecId.add(data);
            }
        }

        if (!missingSpecId.isEmpty()) {
            Preconditions.checkState(!currentSpec.isPartitioned(),
                    "Missing partition spec id for delete files in partitioned table %s",
                    transaction.table().name());
            commitDataBySpecId.computeIfAbsent(currentSpec.specId(), k -> new ArrayList<>()).addAll(missingSpecId);
        }

        List<DeleteFile> deleteFiles = new ArrayList<>();
        for (Map.Entry<Integer, List<TIcebergCommitData>> entry : commitDataBySpecId.entrySet()) {
            int specId = entry.getKey();
            PartitionSpec spec = specsById.get(specId);
            Preconditions.checkState(spec != null,
                    "Unknown partition spec id %s for delete files in table %s",
                    specId, transaction.table().name());
            deleteFiles.addAll(IcebergWriterHelper.convertToDeleteFiles(fileFormat, spec, entry.getValue(), zone));
        }
        return deleteFiles;
    }

    private Long getSnapshotIdIfPresent(Table icebergTable) {
        if (icebergTable == null || icebergTable.currentSnapshot() == null) {
            return null;
        }
        return icebergTable.currentSnapshot().snapshotId();
    }

    // ─────────────────── commit-time conflict-detection validation suite (legacy :655-784) ───────────────────

    /**
     * Applies the optimistic conflict-detection validation suite onto the {@link RowDelta} before it commits,
     * ported verbatim from legacy {@code IcebergTransaction.applyRowDeltaValidations}: pin the base snapshot,
     * set the conflict-detection filter (O5-2 write constraint AND identity-partition filter), and — at the
     * serializable isolation level — validate against conflicting data/delete files and referenced data files.
     */
    private void applyRowDeltaValidations(RowDelta rowDelta, Table icebergTable,
            List<TIcebergCommitData> commitData, List<String> referencedDataFiles) {
        applyBaseSnapshotValidation(rowDelta);
        applyConflictDetectionFilter(rowDelta, icebergTable, commitData);
        if (isSerializableIsolationLevel(icebergTable)) {
            rowDelta.validateNoConflictingDataFiles();
        }
        rowDelta.validateDeletedFiles();
        rowDelta.validateNoConflictingDeleteFiles();
        if (!referencedDataFiles.isEmpty()) {
            rowDelta.validateDataFilesExist(referencedDataFiles);
        }
    }

    private void applyBaseSnapshotValidation(RowDelta rowDelta) {
        if (baseSnapshotId != null) {
            rowDelta.validateFromSnapshot(baseSnapshotId);
        }
    }

    private void applyConflictDetectionFilter(RowDelta rowDelta, Table icebergTable,
            List<TIcebergCommitData> commitData) {
        Optional<Expression> queryFilter = buildWriteConstraintExpression(icebergTable);
        Optional<Expression> partitionFilter = buildConflictDetectionFilter(icebergTable, commitData);
        Optional<Expression> combined = combineConflictDetectionFilters(queryFilter, partitionFilter);
        combined.ifPresent(rowDelta::conflictDetectionFilter);
    }

    /**
     * O5-2: converts the stashed neutral {@link ConnectorPredicate} into an iceberg {@link Expression} using
     * the connector's {@link IcebergPredicateConverter} (P6.2-T02). Done lazily here because the table schema
     * is only available after {@link #beginWrite}. The converter flattens the top-level AND and drops any
     * unconvertible conjunct — which only ever <i>widens</i> the conflict-detection filter (more conservative,
     * never missing a real conflict), see design DV-T05-c.
     */
    Optional<Expression> buildWriteConstraintExpression(Table icebergTable) {
        if (writeConstraint == null || writeConstraint.getExpression() == null || icebergTable == null) {
            return Optional.empty();
        }
        ConnectorExpression expr = writeConstraint.getExpression();
        // conflictMode=true: build the iceberg expression for write-time conflict detection (O5-2), whose
        // matrix is a conservative port of legacy convertPredicateToIcebergExpression (IS NULL / BETWEEN /
        // same-column OR / NOT(IS NULL); drops NE / cross-column OR) — different from scan pushdown.
        List<Expression> converted =
                new IcebergPredicateConverter(icebergTable.schema(), zone, true).convert(expr);
        if (converted.isEmpty()) {
            return Optional.empty();
        }
        Expression combined = converted.get(0);
        for (int i = 1; i < converted.size(); i++) {
            combined = Expressions.and(combined, converted.get(i));
        }
        return Optional.of(combined);
    }

    private Optional<Expression> combineConflictDetectionFilters(Optional<Expression> queryFilter,
            Optional<Expression> partitionFilter) {
        if (queryFilter.isPresent() && partitionFilter.isPresent()) {
            return Optional.of(Expressions.and(queryFilter.get(), partitionFilter.get()));
        }
        return queryFilter.isPresent() ? queryFilter : partitionFilter;
    }

    /**
     * Builds the commit-time identity-partition filter from the partition values carried by the commit
     * fragments: an OR over each fragment's per-partition AND of {@code col = value} (or {@code isNull}).
     * Only when every partition transform is identity and every fragment matches the current spec; otherwise
     * empty (no narrowing). Ported from legacy {@code IcebergTransaction.buildConflictDetectionFilter}.
     */
    private Optional<Expression> buildConflictDetectionFilter(Table icebergTable,
            List<TIcebergCommitData> commitData) {
        if (icebergTable == null || commitData == null || commitData.isEmpty()) {
            return Optional.empty();
        }

        PartitionSpec spec = icebergTable.spec();
        if (!spec.isPartitioned()) {
            return Optional.empty();
        }
        if (!areAllIdentityPartitions(spec)) {
            return Optional.empty();
        }

        Schema schema = icebergTable.schema();
        int currentSpecId = spec.specId();

        Expression combined = null;
        for (TIcebergCommitData data : commitData) {
            if (data.isSetPartitionSpecId() && data.getPartitionSpecId() != currentSpecId) {
                return Optional.empty();
            }
            if (!data.isSetPartitionSpecId() && spec.isPartitioned()) {
                return Optional.empty();
            }

            List<String> partitionValues = extractPartitionValues(data);
            if (partitionValues.isEmpty() || partitionValues.size() != spec.fields().size()) {
                return Optional.empty();
            }

            Expression partitionExpr = buildIdentityPartitionExpression(spec, schema, partitionValues);
            if (partitionExpr == null) {
                return Optional.empty();
            }
            combined = combined == null ? partitionExpr : Expressions.or(combined, partitionExpr);
        }
        return combined == null ? Optional.empty() : Optional.of(combined);
    }

    private boolean areAllIdentityPartitions(PartitionSpec spec) {
        for (PartitionField field : spec.fields()) {
            if (!field.transform().isIdentity()) {
                return false;
            }
        }
        return true;
    }

    private Expression buildIdentityPartitionExpression(PartitionSpec spec, Schema schema,
            List<String> partitionValues) {
        Expression expression = null;
        List<PartitionField> fields = spec.fields();
        for (int i = 0; i < fields.size(); i++) {
            PartitionField field = fields.get(i);
            Types.NestedField sourceField = schema.findField(field.sourceId());
            if (sourceField == null) {
                return null;
            }
            String valueStr = partitionValues.get(i);
            if ("null".equals(valueStr)) {
                valueStr = null;
            }
            Object value = IcebergPartitionUtils.parsePartitionValueFromString(valueStr, sourceField.type(), zone);
            Expression predicate = value == null
                    ? Expressions.isNull(sourceField.name())
                    : Expressions.equal(sourceField.name(), value);
            expression = expression == null ? predicate : Expressions.and(expression, predicate);
        }
        return expression;
    }

    private List<String> extractPartitionValues(TIcebergCommitData commitData) {
        if (commitData == null) {
            return Collections.emptyList();
        }
        if (commitData.getPartitionValues() != null && !commitData.getPartitionValues().isEmpty()) {
            return commitData.getPartitionValues();
        }
        if (commitData.getPartitionDataJson() != null && !commitData.getPartitionDataJson().isEmpty()) {
            return IcebergPartitionUtils.parsePartitionValuesFromJson(commitData.getPartitionDataJson());
        }
        return Collections.emptyList();
    }

    boolean isSerializableIsolationLevel(Table icebergTable) {
        if (icebergTable == null) {
            return true;
        }
        String level = icebergTable.properties()
                .getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT);
        return "serializable".equalsIgnoreCase(level);
    }

    // ─────────────────── V3 deletion-vector "rewrite previous delete files" (legacy :786-851) ───────────────────

    boolean shouldRewritePreviousDeleteFiles() {
        return table != null && formatVersion(table) >= 3;
    }

    /**
     * Reads the real table format version, mirroring {@code IcebergConnectorMetadata.getFormatVersion} /
     * legacy {@code IcebergUtils.getFormatVersion}: from a {@link BaseTable}'s current metadata when
     * available, else from the {@code format-version} table property, defaulting to 2.
     */
    private static int formatVersion(Table table) {
        int formatVersion = 2;
        if (table instanceof BaseTable) {
            formatVersion = ((BaseTable) table).operations().current().formatVersion();
        } else if (table != null && table.properties() != null) {
            String version = table.properties().get(TableProperties.FORMAT_VERSION);
            if (version != null) {
                try {
                    formatVersion = Integer.parseInt(version);
                } catch (NumberFormatException ignored) {
                    // keep the default
                }
            }
        }
        return formatVersion;
    }

    /**
     * Collects the old file-scoped delete files to {@code removeDeletes} from the V3 deletion-vector RowDelta,
     * re-derived at commit time (Trino-style, mirroring {@code DefaultDeletionVectorWriter
     * .getExistingDeletesByMetadataOnly}): a metadata-only read of the base snapshot's delete manifests, keyed by
     * the data-file paths this commit touched ({@link TIcebergCommitData#getReferencedDataFilePath}). The old
     * file-scoped deletes (legacy file-scoped position deletes + V3 deletion vectors) referencing those data
     * files are exactly the ones a V3 commit must remove so each data file keeps at most one deletion file.
     * Deduped by {@link #buildDeleteFileDedupKey}.
     *
     * <p>Unlike legacy {@code IcebergTransaction.collectRewrittenDeleteFiles} (which looked the old files up in a
     * scan-time map fed from the read plan), this reads them from the write-time {@link #baseSnapshotId} the
     * RowDelta validates against, so the removed deletes are snapshot-consistent with the commit by construction
     * (no read-vs-write snapshot skew). Post-flip deviation DV-S2-rederive (dormant: iceberg is not yet a
     * plugin-driven type, so this commit path runs only after the C5 flip).</p>
     */
    List<DeleteFile> collectRewrittenDeleteFiles(List<TIcebergCommitData> deleteCommitData) {
        if (deleteCommitData == null || deleteCommitData.isEmpty() || baseSnapshotId == null || table == null) {
            return Collections.emptyList();
        }
        Set<String> touchedDataFilePaths = new HashSet<>();
        for (TIcebergCommitData commitData : deleteCommitData) {
            if (commitData.isSetReferencedDataFilePath()
                    && commitData.getReferencedDataFilePath() != null
                    && !commitData.getReferencedDataFilePath().isEmpty()) {
                touchedDataFilePaths.add(commitData.getReferencedDataFilePath());
            }
        }
        if (touchedDataFilePaths.isEmpty()) {
            return Collections.emptyList();
        }
        return readExistingFileScopedDeletes(table, baseSnapshotId, touchedDataFilePaths);
    }

    /**
     * Reads the base snapshot's delete manifests (metadata-only — no data-file reads) and returns the file-scoped
     * position deletes / deletion vectors whose referenced data file is among {@code touchedDataFilePaths},
     * deduped by {@link #buildDeleteFileDedupKey}. Mirrors Trino
     * {@code DefaultDeletionVectorWriter.getExistingDeletesByMetadataOnly} (POSITION_DELETES content, file-scoped
     * only — partition-scoped deletes are never removed).
     *
     * <p>Intentional divergence from Trino: when a data file (on a v2&rarr;v3 upgraded table) carries BOTH a
     * legacy file-scoped position delete AND a deletion vector, this returns BOTH (Trino suppresses the legacy
     * file once a DV exists). Doris's BE unions the old positions from both kinds into the new DV
     * ({@code viceberg_delete_sink} load_rewritable_delete_rows), so both old files are fully superseded and both
     * must be removed — leaving the legacy file would orphan a stale delete.
     *
     * <p>Each {@link DeleteFile} is defensively copied so the returned list stays valid after the reader closes.
     */
    private List<DeleteFile> readExistingFileScopedDeletes(
            Table baseTable, long snapshotId, Set<String> touchedDataFilePaths) {
        Snapshot snapshot = baseTable.snapshot(snapshotId);
        if (snapshot == null) {
            return Collections.emptyList();
        }
        FileIO io = baseTable.io();
        Map<Integer, PartitionSpec> specsById = baseTable.specs();
        Map<String, DeleteFile> dedup = new LinkedHashMap<>();
        for (ManifestFile manifest : snapshot.deleteManifests(io)) {
            try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest, io, specsById)) {
                for (DeleteFile deleteFile : reader) {
                    if (deleteFile.content() != FileContent.POSITION_DELETES
                            || !ContentFileUtil.isFileScoped(deleteFile)) {
                        continue;
                    }
                    String referenced = deleteFile.referencedDataFile();
                    if (referenced == null || !touchedDataFilePaths.contains(referenced)) {
                        continue;
                    }
                    dedup.putIfAbsent(buildDeleteFileDedupKey(deleteFile), deleteFile.copy());
                }
            } catch (IOException e) {
                throw new DorisConnectorException(
                        "Failed to read iceberg delete manifest " + manifest.path() + ": " + e.getMessage(), e);
            }
        }
        return new ArrayList<>(dedup.values());
    }

    private String buildDeleteFileDedupKey(DeleteFile deleteFile) {
        if (deleteFile.format() == FileFormat.PUFFIN) {
            return deleteFile.path() + "#" + deleteFile.contentOffset() + "#"
                    + deleteFile.contentSizeInBytes();
        }
        return deleteFile.path().toString();
    }

    /**
     * Collects the referenced data-file paths for {@code validateDataFilesExist} from the delete/DV fragments
     * ({@code referenced_data_files} + {@code referenced_data_file_path}). Ported from legacy
     * {@code IcebergTransaction.collectReferencedDataFiles}.
     */
    List<String> collectReferencedDataFiles(List<TIcebergCommitData> commitData) {
        if (commitData == null || commitData.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> referencedDataFiles = new ArrayList<>();
        for (TIcebergCommitData data : commitData) {
            if (data.isSetFileContent()
                    && data.getFileContent() != TFileContent.POSITION_DELETES
                    && data.getFileContent() != TFileContent.DELETION_VECTOR) {
                continue;
            }
            if (data.isSetReferencedDataFiles()) {
                for (String dataFile : data.getReferencedDataFiles()) {
                    if (dataFile != null && !dataFile.isEmpty()) {
                        referencedDataFiles.add(dataFile);
                    }
                }
            }
            if (data.isSetReferencedDataFilePath()
                    && data.getReferencedDataFilePath() != null
                    && !data.getReferencedDataFilePath().isEmpty()) {
                referencedDataFiles.add(data.getReferencedDataFilePath());
            }
        }
        return referencedDataFiles;
    }

    @Override
    public void rollback() {
        // Insert-mode: nothing to undo on the FE side — an uncommitted SDK transaction simply discards
        // its pending manifests (legacy IcebergTransaction.rollback no-ops the insert path). The rewrite
        // path's file-list cleanup is a P6.4 procedure concern, out of scope here.
        LOG.info("Iceberg transaction {} rollback called; uncommitted manifests will be discarded.",
                transactionId);
    }

    @Override
    public void close() {
        // No resources to release: the SDK transaction holds no connections of its own.
    }

    /** Package-visible accessors for the unit tests (and the T05 validation suite). */
    Transaction getTransaction() {
        return transaction;
    }

    Table getTable() {
        return table;
    }

    List<TIcebergCommitData> getCommitDataList() {
        return commitDataList;
    }

    /** The snapshot pinned at begin time for a DELETE/MERGE (null for INSERT/OVERWRITE); consumed by T05. */
    Long getBaseSnapshotId() {
        return baseSnapshotId;
    }

    // ─────────────────── REWRITE accessors (P6.4-T06; consumed by the rewrite coordinator + tests) ───────────────────

    /** REWRITE OCC anchor captured at begin time (-1 when the table had no snapshot); the value passed to
     *  {@code RewriteFiles.validateFromSnapshot} at commit. */
    long getStartingSnapshotId() {
        return startingSnapshotId;
    }

    /** Number of original data files to remove (legacy {@code getFilesToDeleteCount}); available after
     *  {@link #updateRewriteFiles}. Feeds the {@code rewritten_data_files_count} result column. */
    int getFilesToDeleteCount() {
        synchronized (filesToDelete) {
            return filesToDelete.size();
        }
    }

    /** Number of new compacted data files added — populated DURING {@code commit()}
     *  ({@link #convertCommitDataToFilesToAdd}), so read it only after commit (legacy {@code getFilesToAddCount},
     *  which the executor reads after {@code finishRewrite}). Feeds the {@code added_data_files_count} column. */
    int getFilesToAddCount() {
        synchronized (filesToAdd) {
            return filesToAdd.size();
        }
    }

    /** Total byte size of the original data files to remove (legacy {@code getFilesToDeleteSize}). */
    long getFilesToDeleteSize() {
        synchronized (filesToDelete) {
            return filesToDelete.stream().mapToLong(DataFile::fileSizeInBytes).sum();
        }
    }

    /** Total byte size of the new compacted data files added (legacy {@code getFilesToAddSize}); post-commit. */
    long getFilesToAddSize() {
        synchronized (filesToAdd) {
            return filesToAdd.stream().mapToLong(DataFile::fileSizeInBytes).sum();
        }
    }
}
