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
import org.apache.doris.thrift.TIcebergCommitData;
import org.apache.doris.thrift.TUpdateMode;
import org.apache.doris.transaction.Transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergTransaction implements Transaction {

    private static final Logger LOG = LogManager.getLogger(IcebergTransaction.class);

    private final IcebergMetadataOps ops;
    private Table table;

    private org.apache.iceberg.Transaction transaction;
    private final List<TIcebergCommitData> commitDataList = Lists.newArrayList();

    private IcebergInsertCommandContext insertCtx;
    private String branchName;

    // Rewrite operation support
    private final List<DataFile> filesToDelete = Lists.newArrayList();
    private final List<DataFile> filesToAdd = Lists.newArrayList();
    private boolean isRewriteMode = false;

    public IcebergTransaction(IcebergMetadataOps ops) {
        this.ops = ops;
    }

    public void updateIcebergCommitData(List<TIcebergCommitData> commitDataList) {
        synchronized (this) {
            this.commitDataList.addAll(commitDataList);
        }
    }

    public void updateRewriteFiles(List<DataFile> filesToDelete) {
        synchronized (this) {
            this.filesToDelete.addAll(filesToDelete);
        }
    }

    public void beginInsert(ExternalTable dorisTable, Optional<InsertCommandContext> ctx) throws UserException {
        ctx.ifPresent(c -> this.insertCtx = (IcebergInsertCommandContext) c);
        try {
            ops.getExecutionAuthenticator().execute(() -> {
                // create and start the iceberg transaction
                this.table = IcebergUtils.getIcebergTable(dorisTable);
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
                this.transaction = table.newTransaction();
            });
        } catch (Exception e) {
            throw new UserException("Failed to begin insert for iceberg table " + dorisTable.getName()
                    + "because: " + e.getMessage(), e);
        }

    }

    /**
     * Begin rewrite transaction for data file rewrite operations
     */
    public void beginRewrite(ExternalTable dorisTable) throws UserException {
        // For rewrite operations, we work directly on the main table
        this.branchName = null;
        this.isRewriteMode = true;

        try {
            ops.getExecutionAuthenticator().execute(() -> {
                // create and start the iceberg transaction
                this.table = IcebergUtils.getIcebergTable(dorisTable);

                // For rewrite operations, we work directly on the main table
                // No branch information needed
                this.transaction = table.newTransaction();
                LOG.info("Started rewrite transaction for table: {} (main table)",
                        dorisTable.getName());
                return null;
            });
        } catch (Exception e) {
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
        // commit the iceberg transaction
        transaction.commitTransaction();
    }

    @Override
    public void rollback() {
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
    }

    public long getUpdateCnt() {
        return commitDataList.stream().mapToLong(TIcebergCommitData::getRowCount).sum();
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
                try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
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

        // Add new data files
        for (WriteResult result : pendingResults) {
            Preconditions.checkState(result.referencedDataFiles().length == 0,
                    "Should have no referenced data files for static partition overwrite.");
            Arrays.stream(result.dataFiles()).forEach(overwriteFiles::addFile);
        }

        // Commit the overwrite operation
        overwriteFiles.commit();
    }

    /**
     * Build partition filter expression from static partition key-value pairs
     *
     * @param staticPartitions Map of partition column name to partition value (as String)
     * @param spec PartitionSpec of the table
     * @param schema Schema of the table
     * @return Iceberg Expression for partition filtering
     */
    private Expression buildPartitionFilter(
            Map<String, String> staticPartitions,
            PartitionSpec spec,
            Schema schema) {
        if (staticPartitions == null || staticPartitions.isEmpty()) {
            return Expressions.alwaysTrue();
        }

        List<Expression> predicates = new ArrayList<>();

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

                // Convert partition value string to appropriate type
                Object partitionValue = IcebergUtils.parsePartitionValueFromString(
                        partitionValueStr, sourceField.type());

                // Build equality expression using source field name (not partition field name)
                // For identity partitions, Iceberg requires the source column name in expressions
                String sourceColName = sourceField.name();
                Expression eqExpr;
                if (partitionValue == null) {
                    eqExpr = Expressions.isNull(sourceColName);
                } else {
                    eqExpr = Expressions.equal(sourceColName, partitionValue);
                }
                predicates.add(eqExpr);
            }
        }

        if (predicates.isEmpty()) {
            return Expressions.alwaysTrue();
        }

        // Combine all predicates with AND
        Expression result = predicates.get(0);
        for (int i = 1; i < predicates.size(); i++) {
            result = Expressions.and(result, predicates.get(i));
        }
        return result;
    }
}
