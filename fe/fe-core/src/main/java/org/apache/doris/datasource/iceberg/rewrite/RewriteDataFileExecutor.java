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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergRewriteExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.NereidsCoordinator;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.iceberg.DataFile;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Executes INSERT-SELECT statements for Iceberg data file rewriting.
 *
 * Execution Flow:
 * 1. initialize() - Build rewrite logical plan
 * 2. executeGroup() - Execute rewrite for each file group
 * 3. Collect execution statistics and return results
 */
public class RewriteDataFileExecutor {
    private static final Logger LOG = LogManager.getLogger(RewriteDataFileExecutor.class);

    private final IcebergExternalTable dorisTable;
    private final ConnectContext connectContext;

    // Pre-prepared execution context
    private boolean initialized = false;
    private InsertIntoTableCommand logicalPlan;
    private StatementBase parsedStmt;

    public RewriteDataFileExecutor(IcebergExternalTable dorisTable,
            ConnectContext connectContext) {
        this.dorisTable = dorisTable;
        this.connectContext = connectContext;
    }

    /**
     * Initialize the executor with manually constructed logical plan
     */
    public void initialize() {
        if (initialized) {
            return;
        }
        this.logicalPlan = buildRewriteLogicalPlan();

        LogicalPlanAdapter adapter = new LogicalPlanAdapter(logicalPlan, connectContext.getStatementContext());
        adapter.setOrigStmt(new OriginStatement(logicalPlan.toString(), 0));
        this.parsedStmt = adapter;
        this.initialized = true;

        LOG.debug("Initialized executor with manually constructed logical plan");
    }

    /**
     * Execute rewrite for a single group
     */
    public RewriteResult executeGroup(RewriteDataGroup group) throws UserException {
        if (!initialized) {
            throw new UserException("Executor not initialized. Call initialize() first.");
        }

        LOG.info("Executing rewrite for group with {} tasks, total size: {} bytes",
                group.getTaskCount(), group.getTotalSize());

        StmtExecutor executor = null;
        try {
            // Step 1: Create StmtExecutor using pre-parsed statement
            executor = new StmtExecutor(connectContext, parsedStmt);
            AbstractInsertExecutor insertExecutor = logicalPlan.initPlan(connectContext, executor);
            Preconditions.checkState(insertExecutor instanceof IcebergRewriteExecutor,
                    "Expected IcebergRewriteExecutor, got: " + insertExecutor.getClass());
            IcebergRewriteExecutor rewriteExecutor = (IcebergRewriteExecutor) insertExecutor;

            // Step 2: Customize insert executor for rewrite
            customizeInsertExecutorForRewrite(rewriteExecutor, group);

            // Step 3: Update transaction with files to delete
            IcebergTransaction transaction = rewriteExecutor.getTransaction();
            List<DataFile> filesToDelete = group.getDataFiles().stream().collect(Collectors.toList());
            transaction.updateRewriteFiles(filesToDelete);

            // Step 4: Execute insert operation
            insertExecutor.executeSingleInsert(executor, System.currentTimeMillis());

            // Step 5: Collect execution statistics and rewrite information
            return collectRewriteResult(rewriteExecutor, group);

        } catch (Exception e) {
            LOG.error("Failed to execute rewrite group: ", e);
            throw new UserException("Rewrite group execution failed: " + e.getMessage());
        }
    }

    /**
     * Customize insert executor for Iceberg file rewrite
     */
    private void customizeInsertExecutorForRewrite(
            IcebergRewriteExecutor insertExecutor,
            RewriteDataGroup group) throws Exception {

        LOG.debug("Customizing insert executor for rewrite with {} tasks", group.getTaskCount());

        // Get the coordinator from the insert executor
        Coordinator coordinator = insertExecutor.getCoordinator();
        if (coordinator == null) {
            throw new UserException("No coordinator found in insert executor");
        }

        // Access coordinator context to get scan nodes
        Preconditions.checkState(coordinator instanceof NereidsCoordinator,
                "Expected NereidsCoordinator, got: " + coordinator.getClass());
        NereidsCoordinator nereidsCoordinator = (NereidsCoordinator) coordinator;
        CoordinatorContext context = nereidsCoordinator.getCoordinatorContext();
        Preconditions.checkState(context != null && context.scanNodes != null && context.scanNodes.size() == 1,
                "No scan nodes found in coordinator context");

        // Find and customize IcebergScanNode
        ScanNode scanNode = context.scanNodes.get(0);
        Preconditions.checkState(scanNode instanceof IcebergScanNode,
                "Expected IcebergScanNode, got: " + scanNode.getClass());
        ((IcebergScanNode) scanNode).resetFileScanTasks(group.getTasks());
    }

    /**
     * Collect rewrite result from insert executor
     */
    private RewriteResult collectRewriteResult(IcebergRewriteExecutor insertExecutor, RewriteDataGroup group)
            throws UserException {
        // Get execution statistics from the executor
        long processedRows = insertExecutor.getLoadedRows();

        // Get detailed file information from IcebergTransaction
        RewriteFileInfo fileInfo = insertExecutor.getRewriteFileInfo();

        // Create rewrite result with collected information
        RewriteResult result = new RewriteResult(
                fileInfo.getFilesToDeleteCount(), // rewrittenDataFilesCount (files to delete)
                fileInfo.getFilesToAddCount(), // addedDataFilesCount (files to add)
                fileInfo.getFilesToDeleteSize(), // rewrittenBytesCount (size of files to delete)
                0 // removedDeleteFilesCount (no delete files in rewrite)
        );

        LOG.info("Rewrite completed for group: {} files to delete, {} files to add, {} rows processed, "
                + "{} bytes deleted, {} bytes added",
                fileInfo.getFilesToDeleteCount(), fileInfo.getFilesToAddCount(),
                processedRows, fileInfo.getFilesToDeleteSize(), fileInfo.getFilesToAddSize());

        return result;
    }

    /**
     * Build logical plan for rewrite operation (INSERT INTO ... SELECT ...)
     */
    private InsertIntoTableCommand buildRewriteLogicalPlan() {
        // Build table name parts
        List<String> tableNameParts = ImmutableList.of(
                dorisTable.getCatalog().getName(),
                dorisTable.getDbName(),
                dorisTable.getName());

        // Create UnboundRelation for SELECT part (source table)
        UnboundRelation sourceRelation = new UnboundRelation(
                StatementScopeIdGenerator.newRelationId(),
                tableNameParts,
                ImmutableList.of(), // partitions
                false, // isTemporary
                ImmutableList.of(), // tabletIds
                ImmutableList.of(), // hints
                Optional.empty(), // orderKeys
                Optional.empty() // limit
        );

        // Create UnboundIcebergTableSink for INSERT part (target table)
        UnboundIcebergTableSink<?> tableSink = new UnboundIcebergTableSink<>(
                tableNameParts,
                ImmutableList.of(), // colNames (empty means all columns)
                ImmutableList.of(), // hints
                ImmutableList.of(), // partitions
                DMLCommandType.INSERT,
                Optional.empty(), // labelName
                Optional.empty(), // branchName
                sourceRelation);

        // Create InsertIntoTableCommand for rewrite operation
        InsertIntoTableCommand insertCommand = new InsertIntoTableCommand(
                tableSink,
                Optional.empty(), // labelName
                Optional.empty(), // insertCtx
                Optional.empty(), // cte
                true, // needNormalizePlan
                Optional.empty() // branchName
        );
        insertCommand.setRewriteOperation(true);
        return insertCommand;
    }
}
