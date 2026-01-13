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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;
import org.apache.doris.transaction.TransactionType;

import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Executor for Iceberg DELETE operations.
 *
 * DELETE is implemented by generating Position Delete files
 * instead of rewriting data files.
 *
 * Flow:
 * 1. Execute query to get rows matching WHERE clause (with $row_id column)
 * 2. Collect $row_id information grouped by file
 * 3. Write Position Delete files (file_path, pos)
 * 4. Commit DeleteFiles to Iceberg table using RowDelta API
 */
public class IcebergDeleteExecutor extends BaseExternalTableInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(IcebergDeleteExecutor.class);
    private Optional<Expression> conflictDetectionFilter = Optional.empty();

    public IcebergDeleteExecutor(ConnectContext ctx, IcebergExternalTable table,
            String labelName, NereidsPlanner planner,
            boolean emptyInsert, long jobId) {
        // BaseExternalTableInsertExecutor requires Optional<InsertCommandContext>
        // For DELETE operations, we pass Optional.empty().
        super(ctx, table, labelName, planner, Optional.empty(), emptyInsert, jobId);
    }

    public void finalizeSinkForDelete(PlanFragment fragment, DataSink sink, PhysicalSink<?> physicalSink) {
        super.finalizeSink(fragment, sink, physicalSink);
    }

    public void setConflictDetectionFilter(Optional<Expression> filter) {
        conflictDetectionFilter = filter == null ? Optional.empty() : filter;
    }

    @Override
    protected void beforeExec() throws UserException {
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
        transaction.beginDelete((IcebergExternalTable) table);
        if (conflictDetectionFilter.isPresent()) {
            transaction.setConflictDetectionFilter(conflictDetectionFilter.get());
        } else {
            transaction.clearConflictDetectionFilter();
        }
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        IcebergExternalTable dorisTable = (IcebergExternalTable) table;

        // For Position Delete: collect $row_id information from query results.
        // In current implementation, the delete information is collected by BE
        // and sent to FE through the normal data flow (as part of the query result).
        //
        // TODO Phase 2: Implement specialized collection mechanism
        // 1. Parse $row_id column from query result blocks
        // 2. Group by file_path
        // 3. Generate Position Delete files with (file_path, pos) pairs
        //
        // For now, the transaction will collect commit data from BE through
        // the existing mechanism (TIcebergCommitData).

        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);

        // TODO Phase 2: Process delete files from commit data
        // List<TIcebergCommitData> commitDataList = transaction.getCommitDataList();
        // TODO Phase 2: Process commit data from BE
        // For Position Delete, we need to process $row_id data
        // This will be implemented when we add the data collection mechanism
        LOG.info("Processing Position Delete for table: {}", dorisTable.getName());

        // TODO: Extract $row_id from query result and write Position Delete files
        // Map<String, List<Long>> fileToPositions = extractRowIdData();
        // for (Map.Entry<String, List<Long>> entry : fileToPositions.entrySet()) {
        //     writePositionDeleteFile(entry.getKey(), entry.getValue());
        // }

        this.loadedRows = transaction.getUpdateCnt();

        // Finish delete and commit
        org.apache.doris.datasource.NameMapping nameMapping =
                new org.apache.doris.datasource.NameMapping(
                    dorisTable.getCatalog().getId(),
                    dorisTable.getDbName(),
                    dorisTable.getName(),
                    dorisTable.getRemoteDbName(),
                    dorisTable.getRemoteName());

        transaction.finishDelete(nameMapping);
    }

    @Override
    protected TransactionType transactionType() {
        return TransactionType.ICEBERG;
    }

    /**
     * Extract $row_id data from query results and group by file.
     *
     * This method will parse the $row_id STRUCT column which contains:
     * - file_path: STRING
     * - row_position: BIGINT
     * - partition_spec_id: INT
     * - partition_data: STRING
     *
     * Returns a map of file_path -> list of row positions to delete.
     */
    private Map<String, RowIdGroup> extractRowIdData() throws UserException {
        Map<String, RowIdGroup> result = new HashMap<>();

        // Get the commit data list which contains the delete information
        // In Doris, the BE sends back TIcebergCommitData through the transaction
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
        List<TIcebergCommitData> commitDataList = transaction.getCommitDataList();

        if (commitDataList == null || commitDataList.isEmpty()) {
            LOG.info("No commit data from BE for delete operation");
            return result;
        }

        // Process each commit data entry
        // For Position Delete, BE should have collected $row_id information
        for (TIcebergCommitData commitData : commitDataList) {
            if (commitData.getFileContent() != TFileContent.POSITION_DELETES) {
                continue;
            }

            // Extract file path and row positions from commit data
            String filePath = commitData.getFilePath();
            if (filePath == null || filePath.isEmpty()) {
                LOG.warn("Empty file path in commit data, skipping");
                continue;
            }

            // Get or create RowIdGroup for this file
            result.computeIfAbsent(filePath, k -> {
                // Extract partition information
                int partitionSpecId = 0;
                String partitionDataJson = "";

                if (commitData.partition_spec_id > 0) {
                    partitionSpecId = commitData.partition_spec_id;
                }
                if (commitData.getPartitionDataJson() != null) {
                    partitionDataJson = commitData.getPartitionDataJson();
                }

                return new RowIdGroup(k, partitionSpecId, partitionDataJson);
            });

            // Add row count (in real implementation, BE should provide individual positions)
            long rowCount = commitData.getRowCount();
            LOG.info("Extracted {} rows to delete from file: {}", rowCount, filePath);
        }

        return result;
    }

    /**
     * Write a Position Delete file for a specific data file.
     *
     * Creates a Parquet file with schema: (file_path: STRING, pos: BIGINT)
     * Each row represents a row to delete in the data file.
     */
    private void writePositionDeleteFile(String dataFilePath, List<Long> positions,
                                        RowIdGroup rowIdGroup) throws UserException {
        // Position Delete files are created by the Iceberg transaction mechanism
        // The commit data has already been collected in extractRowIdData()
        //
        // In Doris architecture:
        // 1. BE generates delete file data during query execution
        // 2. BE sends TIcebergCommitData to FE via transaction
        // 3. FE collects these in IcebergTransaction.commitDataList
        // 4. FE calls IcebergWriterHelper.convertToDeleteFiles() to create DeleteFile objects
        // 5. FE commits via RowDelta API
        //
        // This method is primarily for logging and validation

        if (positions == null || positions.isEmpty()) {
            LOG.warn("No positions to delete for file: {}", dataFilePath);
            return;
        }

        LOG.info("Position Delete file info:");
        LOG.info("  Data file: {}", dataFilePath);
        LOG.info("  Rows to delete: {}", positions.size());
        LOG.info("  Partition spec ID: {}", rowIdGroup.partitionSpecId);
        LOG.info("  Partition data: {}", rowIdGroup.partitionDataJson);

        // The actual DeleteFile creation happens in:
        // - IcebergWriterHelper.convertToDeleteFiles() (converts TIcebergCommitData -> DeleteFile)
        // - IcebergTransaction.finishDelete() (commits via RowDelta)
        //
        // Both are already implemented and called in doBeforeCommit()
    }

    /**
     * Helper class to group $row_id data by file.
     */
    private static class RowIdGroup {
        private final String filePath;
        private final List<Long> positions;
        private final int partitionSpecId;
        private final String partitionDataJson;

        public RowIdGroup(String filePath, int partitionSpecId, String partitionDataJson) {
            this.filePath = filePath;
            this.positions = new ArrayList<>();
            this.partitionSpecId = partitionSpecId;
            this.partitionDataJson = partitionDataJson;
        }

        public void addPosition(long position) {
            positions.add(position);
        }

        public String getFilePath() {
            return filePath;
        }

        public List<Long> getPositions() {
            return positions;
        }

        public int getPartitionSpecId() {
            return partitionSpecId;
        }

        public String getPartitionDataJson() {
            return partitionDataJson;
        }
    }
}
