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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java
// and modified by Doris

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.iceberg.helper.IcebergWriterHelper;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;
import org.apache.doris.thrift.TUpdateMode;
import org.apache.doris.transaction.Transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
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
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergTransaction implements Transaction {

    private static final Logger LOG = LogManager.getLogger(IcebergTransaction.class);
    private static final String DELETE_ISOLATION_LEVEL = "delete_isolation_level";
    private static final String DELETE_ISOLATION_LEVEL_DEFAULT = "serializable";

    private final IcebergMetadataOps ops;
    private Table table;

    private org.apache.iceberg.Transaction transaction;
    private IcebergCommitCoordinator.Guard commitGuard;
    private final List<TIcebergCommitData> commitDataList = Lists.newArrayList();
    private boolean acceptingCommitData = true;
    private Optional<Expression> conflictDetectionFilter = Optional.empty();

    private IcebergInsertCommandContext insertCtx;
    private String branchName;
    private Long baseSnapshotId;
    private Map<String, List<DeleteFile>> rewrittenDeleteFilesByReferencedDataFile = Collections.emptyMap();

    // Rewrite operation support
    long startingSnapshotId = -1L; // Track the starting snapshot ID for rewrite operations
    private final List<DataFile> filesToDelete = Lists.newArrayList();
    private final List<DataFile> filesToAdd = Lists.newArrayList();
    private boolean isRewriteMode = false;

    public IcebergTransaction(IcebergMetadataOps ops) {
        this.ops = ops;
    }

    public void updateIcebergCommitData(List<TIcebergCommitData> commitDataList) {
        synchronized (this) {
            // A report may retain this transaction after the manager removes it; the closing fence
            // must therefore be checked under the same lock that attaches report ownership.
            Preconditions.checkState(acceptingCommitData,
                    "Iceberg transaction is no longer accepting commit data");
            this.commitDataList.addAll(commitDataList);
        }
    }

    public synchronized void stopAcceptingCommitData() {
        acceptingCommitData = false;
    }

    public void setConflictDetectionFilter(Expression filter) {
        conflictDetectionFilter = Optional.ofNullable(filter);
    }

    public void clearConflictDetectionFilter() {
        conflictDetectionFilter = Optional.empty();
    }

    public void setRewrittenDeleteFilesByReferencedDataFile(
            Map<String, List<DeleteFile>> rewrittenDeleteFilesByReferencedDataFile) {
        this.rewrittenDeleteFilesByReferencedDataFile =
                rewrittenDeleteFilesByReferencedDataFile == null
                        ? Collections.emptyMap()
                        : rewrittenDeleteFilesByReferencedDataFile;
    }

    public List<TIcebergCommitData> getCommitDataList() {
        return commitDataList;
    }

    public void updateRewriteFiles(List<DataFile> filesToDelete) {
        synchronized (this) {
            this.filesToDelete.addAll(filesToDelete);
        }
    }

    /** Begin an insert against the metadata generation retained during sink binding. */
    public void beginInsert(ExternalTable dorisTable, Table targetTable,
            Optional<InsertCommandContext> ctx) throws UserException {
        ctx.ifPresent(c -> this.insertCtx = (IcebergInsertCommandContext) c);
        acquireCommitFence(targetTable);
        try {
            ops.getExecutionAuthenticator().execute(() -> {
                // Planning, BE serialization, and commit must share one Iceberg metadata
                // generation even if the catalog refreshes between those phases.
                this.table = targetTable;
                // check branch
                if (insertCtx != null && insertCtx.getBranchName().isPresent()) {
                    this.branchName = insertCtx.getBranchName().get();
                    SnapshotRef branchRef = table.refs().get(branchName);
                    if (branchRef == null) {
                        throw new RuntimeException(branchName + " is not founded in " + dorisTable.getName());
                    } else if (!branchRef.isBranch()) {
                        throw new RuntimeException(
                                branchName
                                        + " is a tag, not a branch. Tags cannot be targets for producing snapshots");
                    }
                }
                if (insertCtx != null && insertCtx.isOverwrite()) {
                    // OVERWRITE must validate against the exact target branch generation retained at binding.
                    Snapshot baseSnapshot = branchName == null
                            ? table.currentSnapshot() : table.snapshot(branchName);
                    this.baseSnapshotId = baseSnapshot == null ? null : baseSnapshot.snapshotId();
                } else {
                    this.baseSnapshotId = null;
                }
                this.transaction = createTransactionTable(dorisTable, table).newTransaction();
                this.rewrittenDeleteFilesByReferencedDataFile = Collections.emptyMap();
            });
        } catch (Exception e) {
            releaseCommitFence();
            throw new UserException("Failed to begin insert for iceberg table " + dorisTable.getName()
                    + " because: " + e.getMessage(), e);
        }

    }

    /** Begin a rewrite against the same retained table used by every rewrite task. */
    public void beginRewrite(ExternalTable dorisTable, Table targetTable) throws UserException {
        // For rewrite operations, we work directly on the main table
        this.branchName = null;
        this.isRewriteMode = true;
        acquireCommitFence(targetTable);

        try {
            ops.getExecutionAuthenticator().execute(() -> {
                // A rewrite group must not silently switch to refreshed metadata mid-transaction.
                this.table = targetTable;
                this.baseSnapshotId = null;

                // Capture the starting snapshot ID for validation during rewrite commit
                Long snapshotId = getSnapshotIdIfPresent(table);
                this.startingSnapshotId = snapshotId != null ? snapshotId : -1L;

                // For rewrite operations, we work directly on the main table
                // No branch information needed
                this.transaction = createTransactionTable(dorisTable, table).newTransaction();
                LOG.info("Started rewrite transaction for table: {} (main table)",
                        dorisTable.getName());
                return null;
            });
        } catch (Exception e) {
            releaseCommitFence();
            throw new UserException("Failed to begin rewrite for iceberg table " + dorisTable.getName()
                    + " because: " + e.getMessage(), e);
        }
    }

    /**
     * Finish rewrite operation by committing all file changes using RewriteFiles
     * API
     */
    public void finishRewrite() {
        // TODO: refactor IcebergTransaction to make code cleaner
        convertCommitDataListToDataFilesToAdd();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Finishing rewrite with {} files to delete and {} files to add",
                    filesToDelete.size(), filesToAdd.size());
        }

        try {
            ops.getExecutionAuthenticator().execute(() -> {
                updateManifestAfterRewrite();
                return null;
            });
        } catch (Exception e) {
            LOG.error("Failed to finish rewrite transaction", e);
            throw new RuntimeException(e);
        }
    }

    private void convertCommitDataListToDataFilesToAdd() {
        if (commitDataList.isEmpty()) {
            LOG.debug("No commit data to convert for rewrite operation");
            return;
        }

        // Convert commit data to DataFile objects using the same logic as insert
        WriteResult writeResult = IcebergWriterHelper.convertToWriterResult(transaction.table(), commitDataList);

        // Add the generated DataFiles to filesToAdd list
        synchronized (filesToAdd) {
            for (DataFile dataFile : writeResult.dataFiles()) {
                filesToAdd.add(dataFile);
            }
        }

        LOG.info("Converted {} commit data entries to {} DataFiles for rewrite operation",
                commitDataList.size(), writeResult.dataFiles().length);
    }

    private void updateManifestAfterRewrite() {
        if (filesToDelete.isEmpty() && filesToAdd.isEmpty()) {
            LOG.info("No files to rewrite, skipping commit");
            return;
        }

        RewriteFiles rewriteFiles = transaction.newRewrite();

        rewriteFiles = rewriteFiles.validateFromSnapshot(startingSnapshotId);

        // For rewrite operations, we work directly on the main table
        rewriteFiles = rewriteFiles.scanManifestsWith(ops.getThreadPoolWithPreAuth());

        // Add files to delete
        for (DataFile dataFile : filesToDelete) {
            rewriteFiles.deleteFile(dataFile);
        }

        // Add files to add
        for (DataFile dataFile : filesToAdd) {
            rewriteFiles.addFile(dataFile);
        }

        // Commit the rewrite operation
        rewriteFiles.commit();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Rewrite committed with {} files deleted and {} files added",
                    filesToDelete.size(), filesToAdd.size());
        }
    }

    /**
     * Begin delete operation for Iceberg table
     */
    public void beginDelete(ExternalTable dorisTable, Table targetTable) throws UserException {
        acquireCommitFence(targetTable);
        try {
            ops.getExecutionAuthenticator().execute(() -> {
                // RowDelta's validation base must match the generation used to select row IDs;
                // reloading the live table here could silently include a concurrent commit.
                this.table = targetTable;
                this.baseSnapshotId = getSnapshotIdIfPresent(table);
                if (table instanceof org.apache.iceberg.HasTableOperations) {
                    int formatVersion = ((org.apache.iceberg.HasTableOperations) table).operations()
                            .current().formatVersion();
                    if (formatVersion < 2) {
                        throw new IllegalArgumentException("Iceberg table " + dorisTable.getName()
                                + " must have format version 2 or higher for position deletes");
                    }
                }
                this.transaction = createTransactionTable(dorisTable, table).newTransaction();
                this.rewrittenDeleteFilesByReferencedDataFile = Collections.emptyMap();
                LOG.info("Started delete transaction for table: {}", dorisTable.getName());
            });
        } catch (Exception e) {
            releaseCommitFence();
            throw new UserException("Failed to begin delete for iceberg table " + dorisTable.getName()
                    + " because: " + e.getMessage(), e);
        }
    }

    private Table createTransactionTable(ExternalTable dorisTable, Table retainedTable) {
        if (!IcebergSnapshotCacheValue.isFrozenGeneration(retainedTable)) {
            return retainedTable;
        }
        // Reads stay on the retained generation; commit refreshes may follow data-only snapshots,
        // while writer-contract changes still invalidate files produced for the retained metadata.
        return IcebergSnapshotCacheValue.createWritableTable(
                retainedTable, IcebergUtils.getIcebergTable(dorisTable));
    }

    /** Begin UPDATE/MERGE against the metadata generation retained by the merge sink. */
    public void beginMerge(ExternalTable dorisTable, Table targetTable) throws UserException {
        acquireCommitFence(targetTable);
        try {
            ops.getExecutionAuthenticator().execute(() -> {
                this.branchName = null;
                // Keep row binding, partition routing, writer schema, and commit on one generation.
                this.table = targetTable;
                this.baseSnapshotId = getSnapshotIdIfPresent(table);
                if (table instanceof org.apache.iceberg.HasTableOperations) {
                    int formatVersion = ((org.apache.iceberg.HasTableOperations) table).operations()
                            .current().formatVersion();
                    if (formatVersion < 2) {
                        throw new IllegalArgumentException("Iceberg table " + dorisTable.getName()
                                + " must have format version 2 or higher for position deletes");
                    }
                }
                this.transaction = createTransactionTable(dorisTable, table).newTransaction();
                this.rewrittenDeleteFilesByReferencedDataFile = Collections.emptyMap();
                LOG.info("Started merge transaction for table: {}", dorisTable.getName());
                return null;
            });
        } catch (Exception e) {
            releaseCommitFence();
            throw new UserException("Failed to begin merge for iceberg table " + dorisTable.getName()
                    + " because: " + e.getMessage(), e);
        }
    }

    /**
     * Finish delete operation by committing DeleteFiles using RowDelta API
     */
    public void finishDelete(NameMapping nameMapping) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("iceberg table {} delete operation finished!", nameMapping.getFullLocalName());
        }
        try {
            ops.getExecutionAuthenticator().execute(() -> {
                updateManifestAfterDelete();
            });
        } catch (Exception e) {
            LOG.warn("Failed to finish delete for iceberg table {}.", nameMapping.getFullLocalName(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Finish merge operation by committing data and delete files using RowDelta.
     */
    public void finishMerge(NameMapping nameMapping) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("iceberg table {} merge operation finished!", nameMapping.getFullLocalName());
        }
        try {
            ops.getExecutionAuthenticator().execute(() -> {
                updateManifestAfterMerge();
            });
        } catch (Exception e) {
            LOG.warn("Failed to finish merge for iceberg table {}.", nameMapping.getFullLocalName(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Update manifest after delete operation using RowDelta API
     */
    private void updateManifestAfterDelete() {
        FileFormat fileFormat = IcebergUtils.getFileFormat(transaction.table());

        if (commitDataList.isEmpty()) {
            LOG.info("No delete files to commit");
            return;
        }
        List<DeleteFile> deleteFiles = convertCommitDataToDeleteFiles(fileFormat, commitDataList);
        List<DeleteFile> rewrittenDeleteFiles = shouldRewritePreviousDeleteFiles()
                ? collectRewrittenDeleteFiles(commitDataList)
                : Collections.emptyList();

        if (deleteFiles.isEmpty()) {
            LOG.info("No delete files generated from commit data");
            return;
        }

        // Create RowDelta operation
        RowDelta rowDelta = transaction.newRowDelta();
        applyRowDeltaValidations(rowDelta, transaction.table(), commitDataList,
                collectReferencedDataFiles(commitDataList));
        rowDelta.scanManifestsWith(ops.getThreadPoolWithPreAuth());

        // Add all delete files
        for (DeleteFile deleteFile : deleteFiles) {
            rowDelta.addDeletes(deleteFile);
        }

        for (DeleteFile deleteFile : rewrittenDeleteFiles) {
            rowDelta.removeDeletes(deleteFile);
        }

        // Commit the delete operation
        rowDelta.commit();

        LOG.info("Committed {} delete files and removed {} previous delete files",
                deleteFiles.size(), rewrittenDeleteFiles.size());
    }

    private List<DeleteFile> convertCommitDataToDeleteFiles(FileFormat fileFormat,
            List<TIcebergCommitData> commitDataList) {
        if (commitDataList.isEmpty()) {
            return Collections.emptyList();
        }

        PartitionSpec currentSpec = transaction.table().spec();
        Map<Integer, PartitionSpec> specsById = transaction.table().specs();
        Map<Integer, List<TIcebergCommitData>> commitDataBySpecId = new HashMap<>();
        List<TIcebergCommitData> missingSpecId = new ArrayList<>();

        for (TIcebergCommitData commitData : commitDataList) {
            if (commitData.isSetPartitionSpecId()) {
                commitDataBySpecId.computeIfAbsent(commitData.getPartitionSpecId(), k -> new ArrayList<>())
                        .add(commitData);
            } else {
                missingSpecId.add(commitData);
            }
        }

        if (!missingSpecId.isEmpty()) {
            Preconditions.checkState(!currentSpec.isPartitioned(),
                    "Missing partition spec id for delete files in partitioned table %s",
                    transaction.table().name());
            commitDataBySpecId.computeIfAbsent(currentSpec.specId(), k -> new ArrayList<>())
                    .addAll(missingSpecId);
        }

        List<DeleteFile> deleteFiles = new ArrayList<>();
        for (Map.Entry<Integer, List<TIcebergCommitData>> entry : commitDataBySpecId.entrySet()) {
            int specId = entry.getKey();
            PartitionSpec spec = specsById.get(specId);
            Preconditions.checkState(spec != null,
                    "Unknown partition spec id %s for delete files in table %s",
                    specId, transaction.table().name());
            deleteFiles.addAll(IcebergWriterHelper.convertToDeleteFiles(fileFormat, spec, entry.getValue()));
        }

        return deleteFiles;
    }

    private void updateManifestAfterMerge() {
        if (commitDataList.isEmpty()) {
            LOG.info("No commit data for merge operation");
            return;
        }

        FileFormat fileFormat = IcebergUtils.getFileFormat(transaction.table());

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
                    transaction.table(), dataCommitData);
            dataFiles.addAll(Arrays.asList(writeResult.dataFiles()));
        }

        List<DeleteFile> deleteFiles = convertCommitDataToDeleteFiles(fileFormat, deleteCommitData);
        List<DeleteFile> rewrittenDeleteFiles = shouldRewritePreviousDeleteFiles()
                ? collectRewrittenDeleteFiles(deleteCommitData)
                : Collections.emptyList();

        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            LOG.info("No data or delete files generated from commit data");
            return;
        }

        RowDelta rowDelta = transaction.newRowDelta();
        applyRowDeltaValidations(rowDelta, transaction.table(), commitDataList,
                collectReferencedDataFiles(deleteCommitData));
        rowDelta.scanManifestsWith(ops.getThreadPoolWithPreAuth());

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
        LOG.info("Committed merge with {} data files, {} delete files and removed {} previous delete files",
                dataFiles.size(), deleteFiles.size(), rewrittenDeleteFiles.size());
    }

    public void finishInsert(NameMapping nameMapping) {
        if (LOG.isDebugEnabled()) {
            LOG.info("iceberg table {} insert table finished!", nameMapping.getFullLocalName());
        }
        try {
            ops.getExecutionAuthenticator().execute(() -> {
                //create and start the iceberg transaction
                TUpdateMode updateMode = TUpdateMode.APPEND;
                if (insertCtx != null) {
                    updateMode = insertCtx.isOverwrite()
                            ? TUpdateMode.OVERWRITE
                            : TUpdateMode.APPEND;
                }
                updateManifestAfterInsert(updateMode);
                return null;
            });
        } catch (Exception e) {
            LOG.warn("Failed to finish insert for iceberg table {}.", nameMapping.getFullLocalName(), e);
            throw new RuntimeException(e);
        }

    }

    private void updateManifestAfterInsert(TUpdateMode updateMode) {
        List<WriteResult> pendingResults;
        if (commitDataList.isEmpty()) {
            pendingResults = Collections.emptyList();
        } else {
            //convert commitDataList to writeResult
            WriteResult writeResult = IcebergWriterHelper
                    .convertToWriterResult(transaction.table(), commitDataList);
            pendingResults = Lists.newArrayList(writeResult);
        }

        if (updateMode == TUpdateMode.APPEND) {
            commitAppendTxn(pendingResults);
        } else {
            // Check if this is a static partition overwrite
            if (insertCtx != null && insertCtx.isStaticPartitionOverwrite()) {
                commitStaticPartitionOverwrite(pendingResults);
            } else {
                commitReplaceTxn(pendingResults);
            }
        }
    }

    @Override
    public void commit() throws UserException {
        stopAcceptingCommitData();
        try {
            transaction.commitTransaction();
        } finally {
            releaseCommitFence();
        }
    }

    @Override
    public void rollback() {
        stopAcceptingCommitData();
        try {
            if (isRewriteMode) {
                // Clear the collected files for rewrite mode
                synchronized (filesToDelete) {
                    filesToDelete.clear();
                }
                synchronized (filesToAdd) {
                    filesToAdd.clear();
                }
                LOG.info("Rewrite transaction rolled back");
            }
            // For insert mode, do nothing as original implementation
        } finally {
            releaseCommitFence();
        }
    }

    private void acquireCommitFence(Table targetTable) {
        Preconditions.checkState(commitGuard == null, "Iceberg transaction fence is already held");
        // Hold a shared fence before files can be selected or written. Destructive maintenance
        // takes the exclusive side, while unrelated and concurrent table writes remain parallel.
        commitGuard = IcebergCommitCoordinator.beginCommit(targetTable.location());
    }

    private void releaseCommitFence() {
        if (commitGuard != null) {
            commitGuard.close();
            commitGuard = null;
        }
    }

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
        // For UPDATE/MERGE, dataRows includes both inserted and update-inserted rows,
        // which equals the number of rows affected. Position deletes are internal
        // implementation details and should not be double-counted.
        return dataRows > 0 ? dataRows : deleteRows;
    }

    /**
     * Get the number of files that will be deleted in rewrite operation
     */
    public int getFilesToDeleteCount() {
        synchronized (filesToDelete) {
            return filesToDelete.size();
        }
    }

    /**
     * Get the number of files that will be added in rewrite operation
     */
    public int getFilesToAddCount() {
        synchronized (filesToAdd) {
            return filesToAdd.size();
        }
    }

    /**
     * Get the total size of files to be deleted in rewrite operation
     */
    public long getFilesToDeleteSize() {
        synchronized (filesToDelete) {
            return filesToDelete.stream().mapToLong(DataFile::fileSizeInBytes).sum();
        }
    }

    /**
     * Get the total size of files to be added in rewrite operation
     */
    public long getFilesToAddSize() {
        synchronized (filesToAdd) {
            return filesToAdd.stream().mapToLong(DataFile::fileSizeInBytes).sum();
        }
    }

    private void commitAppendTxn(List<WriteResult> pendingResults) {
        // commit append files.
        AppendFiles appendFiles = transaction.newAppend().scanManifestsWith(ops.getThreadPoolWithPreAuth());
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

    private Long getSnapshotIdIfPresent(Table icebergTable) {
        if (icebergTable == null || icebergTable.currentSnapshot() == null) {
            return null;
        }
        return icebergTable.currentSnapshot().snapshotId();
    }

    private void applyBaseSnapshotValidation(RowDelta rowDelta) {
        if (baseSnapshotId != null) {
            rowDelta.validateFromSnapshot(baseSnapshotId);
        }
    }

    private void applyRowDeltaValidations(RowDelta rowDelta, Table icebergTable,
            List<TIcebergCommitData> commitDataList, List<String> referencedDataFiles) {
        applyBaseSnapshotValidation(rowDelta);
        applyConflictDetectionFilter(rowDelta, icebergTable, commitDataList);
        if (isSerializableIsolationLevel(icebergTable)) {
            rowDelta.validateNoConflictingDataFiles();
        }
        rowDelta.validateDeletedFiles();
        rowDelta.validateNoConflictingDeleteFiles();
        if (!referencedDataFiles.isEmpty()) {
            rowDelta.validateDataFilesExist(referencedDataFiles);
        }
    }

    private void applyConflictDetectionFilter(RowDelta rowDelta, Table icebergTable,
            List<TIcebergCommitData> commitDataList) {
        Optional<Expression> partitionFilter = buildConflictDetectionFilter(icebergTable, commitDataList);
        Optional<Expression> combined =
                combineConflictDetectionFilters(conflictDetectionFilter, partitionFilter);
        combined.ifPresent(rowDelta::conflictDetectionFilter);
    }

    private Optional<Expression> combineConflictDetectionFilters(Optional<Expression> queryFilter,
            Optional<Expression> partitionFilter) {
        if (queryFilter.isPresent() && partitionFilter.isPresent()) {
            return Optional.of(Expressions.and(queryFilter.get(), partitionFilter.get()));
        }
        return queryFilter.isPresent() ? queryFilter : partitionFilter;
    }

    private Optional<Expression> buildConflictDetectionFilter(Table icebergTable,
            List<TIcebergCommitData> commitDataList) {
        if (icebergTable == null || commitDataList == null || commitDataList.isEmpty()) {
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
        for (TIcebergCommitData commitData : commitDataList) {
            if (commitData.isSetPartitionSpecId()
                    && commitData.getPartitionSpecId() != currentSpecId) {
                return Optional.empty();
            }
            if (!commitData.isSetPartitionSpecId() && spec.isPartitioned()) {
                return Optional.empty();
            }

            List<String> partitionValues = extractPartitionValues(commitData);
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
            String sourceColumnName = identitySourceColumnName(schema, field.sourceId());
            if (sourceColumnName == null) {
                return null;
            }
            String valueStr = partitionValues.get(i);
            if ("null".equals(valueStr)) {
                valueStr = null;
            }
            Object value = IcebergUtils.parsePartitionValueFromString(valueStr, sourceField.type());
            Expression predicate = value == null
                    ? Expressions.isNull(sourceColumnName)
                    : Expressions.equal(sourceColumnName, value);
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
            return IcebergUtils.parsePartitionValuesFromJson(commitData.getPartitionDataJson());
        }
        return Collections.emptyList();
    }

    private boolean isSerializableIsolationLevel(Table icebergTable) {
        if (icebergTable == null) {
            return true;
        }
        String level = icebergTable.properties()
                .getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT);
        return "serializable".equalsIgnoreCase(level);
    }

    private boolean shouldRewritePreviousDeleteFiles() {
        return table != null && IcebergUtils.getFormatVersion(table) >= 3;
    }

    private List<DeleteFile> collectRewrittenDeleteFiles(List<TIcebergCommitData> deleteCommitData) {
        if (deleteCommitData == null || deleteCommitData.isEmpty()
                || rewrittenDeleteFilesByReferencedDataFile.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, DeleteFile> dedup = new LinkedHashMap<>();
        for (TIcebergCommitData commitData : deleteCommitData) {
            if (!commitData.isSetReferencedDataFilePath()
                    || commitData.getReferencedDataFilePath() == null
                    || commitData.getReferencedDataFilePath().isEmpty()) {
                continue;
            }
            List<DeleteFile> oldDeleteFiles =
                    rewrittenDeleteFilesByReferencedDataFile.get(commitData.getReferencedDataFilePath());
            if (oldDeleteFiles == null) {
                continue;
            }
            for (DeleteFile deleteFile : oldDeleteFiles) {
                if (deleteFile != null && ContentFileUtil.isFileScoped(deleteFile)) {
                    dedup.putIfAbsent(buildDeleteFileDedupKey(deleteFile), deleteFile);
                }
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

    private List<String> collectReferencedDataFiles(List<TIcebergCommitData> commitDataList) {
        if (commitDataList == null || commitDataList.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> referencedDataFiles = new ArrayList<>();
        for (TIcebergCommitData commitData : commitDataList) {
            if (commitData.isSetFileContent()
                    && commitData.getFileContent() != TFileContent.POSITION_DELETES
                    && commitData.getFileContent() != TFileContent.DELETION_VECTOR) {
                continue;
            }
            if (commitData.isSetReferencedDataFiles()) {
                for (String dataFile : commitData.getReferencedDataFiles()) {
                    if (dataFile != null && !dataFile.isEmpty()) {
                        referencedDataFiles.add(dataFile);
                    }
                }
            }
            if (commitData.isSetReferencedDataFilePath()
                    && commitData.getReferencedDataFilePath() != null
                    && !commitData.getReferencedDataFilePath().isEmpty()) {
                referencedDataFiles.add(commitData.getReferencedDataFilePath());
            }
        }
        return referencedDataFiles;
    }

    private void commitReplaceTxn(List<WriteResult> pendingResults) {
        if (pendingResults.isEmpty()) {
            // such as : insert overwrite table `dst_tb` select * from `empty_tb`
            // 1. if dst_tb is a partitioned table, it will return directly.
            // 2. if dst_tb is an unpartitioned table, the `dst_tb` table will be emptied.
            if (!transaction.table().spec().isPartitioned()) {
                OverwriteFiles overwriteFiles = transaction.newOverwrite();
                if (branchName != null) {
                    overwriteFiles = overwriteFiles.toBranch(branchName);
                }
                overwriteFiles = overwriteFiles.scanManifestsWith(ops.getThreadPoolWithPreAuth());
                overwriteFiles = validateOverwrite(overwriteFiles, Expressions.alwaysTrue());
                TableScan overwriteScan = table.newScan();
                if (branchName != null) {
                    // The files removed must come from the same branch whose head anchors OCC validation.
                    overwriteScan = overwriteScan.useRef(branchName);
                }
                try (CloseableIterable<FileScanTask> fileScanTasks = overwriteScan.planFiles()) {
                    OverwriteFiles finalOverwriteFiles = overwriteFiles;
                    fileScanTasks.forEach(f -> finalOverwriteFiles.deleteFile(f.file()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                overwriteFiles.commit();
            }
            return;
        }

        // commit replace partitions
        ReplacePartitions appendPartitionOp = transaction.newReplacePartitions();
        if (branchName != null) {
            appendPartitionOp = appendPartitionOp.toBranch(branchName);
        }
        appendPartitionOp = appendPartitionOp.scanManifestsWith(ops.getThreadPoolWithPreAuth());
        // Partition replacement must not delete or revive files committed after sink binding.
        if (baseSnapshotId != null) {
            appendPartitionOp = appendPartitionOp.validateFromSnapshot(baseSnapshotId);
        }
        appendPartitionOp = appendPartitionOp.validateNoConflictingData().validateNoConflictingDeletes();
        for (WriteResult result : pendingResults) {
            Preconditions.checkState(result.referencedDataFiles().length == 0,
                    "Should have no referenced data files.");
            Arrays.stream(result.dataFiles()).forEach(appendPartitionOp::addFile);
        }
        appendPartitionOp.commit();
    }

    /**
     * Commit static partition overwrite operation
     * This method uses OverwriteFiles.overwriteByRowFilter() to overwrite only the specified partitions
     */
    private void commitStaticPartitionOverwrite(List<WriteResult> pendingResults) {
        Table icebergTable = transaction.table();
        PartitionSpec spec = icebergTable.spec();
        Schema schema = icebergTable.schema();

        // Build partition filter expression from static partition values
        Expression partitionFilter = buildPartitionFilter(
                insertCtx.getStaticPartitionValues(), spec, schema);

        // Create OverwriteFiles operation
        OverwriteFiles overwriteFiles = transaction.newOverwrite();
        if (branchName != null) {
            overwriteFiles = overwriteFiles.toBranch(branchName);
        }
        overwriteFiles = overwriteFiles.scanManifestsWith(ops.getThreadPoolWithPreAuth());

        // Set partition filter to overwrite only matching partitions
        overwriteFiles = overwriteFiles.overwriteByRowFilter(partitionFilter);
        overwriteFiles = validateOverwrite(overwriteFiles, partitionFilter);

        // Add new data files
        for (WriteResult result : pendingResults) {
            Preconditions.checkState(result.referencedDataFiles().length == 0,
                    "Should have no referenced data files for static partition overwrite.");
            Arrays.stream(result.dataFiles()).forEach(overwriteFiles::addFile);
        }

        // Commit the overwrite operation
        overwriteFiles.commit();
    }

    private OverwriteFiles validateOverwrite(OverwriteFiles overwriteFiles, Expression conflictFilter) {
        overwriteFiles = overwriteFiles.conflictDetectionFilter(conflictFilter);
        if (baseSnapshotId != null) {
            overwriteFiles = overwriteFiles.validateFromSnapshot(baseSnapshotId);
        }
        return overwriteFiles.validateNoConflictingData().validateNoConflictingDeletes();
    }

    /**
     * Build partition filter expression from static partition key-value pairs
     *
     * @param staticPartitions Map of partition column name to partition value (as String)
     * @param spec PartitionSpec of the table
     * @param schema Schema of the table
     * @return Iceberg Expression for partition filtering
     */
    Expression buildPartitionFilter(
            Map<String, String> staticPartitions,
            PartitionSpec spec,
            Schema schema) {
        if (staticPartitions == null || staticPartitions.isEmpty()) {
            return Expressions.alwaysTrue();
        }

        List<Expression> predicates = new ArrayList<>();
        HashSet<String> matchedPartitionNames = new HashSet<>();

        for (PartitionField field : spec.fields()) {
            String partitionColName = field.name();
            if (staticPartitions.containsKey(partitionColName)) {
                String partitionValueStr = staticPartitions.get(partitionColName);

                // Get source field to determine the type
                Types.NestedField sourceField = schema.findField(field.sourceId());
                if (sourceField == null) {
                    throw new RuntimeException(String.format("Source field not found for partition field: %s",
                        partitionColName));
                }
                String sourceColName = identitySourceColumnName(schema, field.sourceId());
                if (sourceColName == null) {
                    throw new RuntimeException(String.format(
                            "Source column path not found for partition field: %s", partitionColName));
                }

                // Convert partition value string to appropriate type
                Object partitionValue = IcebergUtils.parsePartitionValueFromString(
                        partitionValueStr, sourceField.type());

                // Build equality expression using the source column path (not partition field name).
                // For identity partitions, Iceberg requires the source column in expressions.
                Expression eqExpr;
                if (partitionValue == null) {
                    eqExpr = Expressions.isNull(sourceColName);
                } else {
                    eqExpr = Expressions.equal(sourceColName, partitionValue);
                }
                predicates.add(eqExpr);
                matchedPartitionNames.add(partitionColName);
            }
        }

        if (matchedPartitionNames.size() != staticPartitions.size()) {
            HashSet<String> unmatchedPartitionNames = new HashSet<>(staticPartitions.keySet());
            unmatchedPartitionNames.removeAll(matchedPartitionNames);
            // Falling back to alwaysTrue here would turn a misspelled/static stale key into a
            // whole-table overwrite, so every user-specified key must bind to the retained spec.
            throw new IllegalArgumentException(
                    "Static partition columns are not present in the Iceberg partition spec: "
                            + unmatchedPartitionNames);
        }

        // Combine all predicates with AND
        Expression result = predicates.get(0);
        for (int i = 1; i < predicates.size(); i++) {
            result = Expressions.and(result, predicates.get(i));
        }
        return result;
    }

    private String identitySourceColumnName(Schema schema, int sourceId) {
        // Nested identity predicates must bind to the complete schema path; a leaf name alone can
        // bind another field or fail open when sibling structs reuse that name.
        return schema.findColumnName(sourceId);
    }
}
