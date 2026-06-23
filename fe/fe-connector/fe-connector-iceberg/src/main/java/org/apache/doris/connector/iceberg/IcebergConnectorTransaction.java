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
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import com.google.common.base.Preconditions;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
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
import java.util.List;
import java.util.Map;

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
 * <p><b>P6.3-T05 follow-up.</b> The DELETE/MERGE {@link RowDelta} built here intentionally carries no
 * conflict-detection validation suite (validateFromSnapshot / conflictDetectionFilter /
 * validateNoConflictingDataFiles / ...) and no V3 deletion-vector {@code removeDeletes}. {@link #baseSnapshotId}
 * is captured at begin time for T05 to consume; the validation suite + {@code applyWriteConstraint} (O5-2)
 * land in T05.</p>
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
    // the T05 validation suite (validateFromSnapshot); captured here.
    private Long baseSnapshotId;
    // Session zone for human-readable TIMESTAMP partition value parsing (DV-T04-f).
    private ZoneId zone = ZoneOffset.UTC;

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
        if (op == WriteOperation.DELETE || op == WriteOperation.UPDATE || op == WriteOperation.MERGE) {
            // RowDelta path: the merge/delete write never targets a branch (legacy beginMerge forces null).
            this.branchName = null;
            this.baseSnapshotId = getSnapshotIdIfPresent(table);
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

    /** DELETE: commit position-delete files via {@link RowDelta}. The T05 validation suite is not applied. */
    private void updateManifestAfterDelete() {
        FileFormat fileFormat = IcebergWriterHelper.getFileFormat(transaction.table());
        if (commitDataList.isEmpty()) {
            return;
        }
        List<DeleteFile> deleteFiles = convertCommitDataToDeleteFiles(fileFormat, commitDataList);
        if (deleteFiles.isEmpty()) {
            return;
        }
        RowDelta rowDelta = transaction.newRowDelta();
        // T05: applyRowDeltaValidations(rowDelta, ...) + V3 DV removeDeletes(...) are inserted here.
        for (DeleteFile deleteFile : deleteFiles) {
            rowDelta.addDeletes(deleteFile);
        }
        rowDelta.commit();
    }

    /** UPDATE/MERGE: commit data + position-delete files via {@link RowDelta}. T05 validation suite excluded. */
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
        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            return;
        }

        RowDelta rowDelta = transaction.newRowDelta();
        // T05: applyRowDeltaValidations(rowDelta, ...) + V3 DV removeDeletes(...) are inserted here.
        for (DataFile dataFile : dataFiles) {
            rowDelta.addRows(dataFile);
        }
        for (DeleteFile deleteFile : deleteFiles) {
            rowDelta.addDeletes(deleteFile);
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
}
