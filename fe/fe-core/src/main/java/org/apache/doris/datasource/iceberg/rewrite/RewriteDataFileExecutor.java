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
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergRewriteExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.NereidsCoordinator;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Executes INSERT-SELECT statements for Iceberg data file rewriting.
 *
 * Execution Flow:
 * 1. initialize() - Prepare SQL template and parse INSERT INTO ... SELECT
 * statement once
 * 2. executeGroup() - Execute rewrite for each file group using pre-parsed
 * components
 * 3. Collect execution statistics and return results
 *
 * The executor generates SQL: INSERT INTO catalog.db.table SELECT * FROM
 * catalog.db.table
 * and customizes the scan to target specific files in each RewriteDataGroup.
 */
public class RewriteDataFileExecutor {
    private static final Logger LOG = LogManager.getLogger(RewriteDataFileExecutor.class);

    private final IcebergExternalTable dorisTable;
    private final ConnectContext connectContext;

    // Pre-prepared execution context
    private boolean initialized = false;
    private StatementBase parsedStmt;
    private LogicalPlan logicalPlan;

    public RewriteDataFileExecutor(IcebergExternalTable dorisTable,
            ConnectContext connectContext) {
        this.dorisTable = dorisTable;
        this.connectContext = connectContext;
    }

    /**
     * Initialize the executor with manually constructed logical plan
     */
    public void initialize() throws UserException {
        if (initialized) {
            return;
        }

        LOG.info("Initializing rewrite executor for table: {}", dorisTable.getName());

        try {
            // Manually construct logical plan for INSERT INTO ... SELECT ...
            this.logicalPlan = buildRewriteLogicalPlan();

            // Create LogicalPlanAdapter
            LogicalPlanAdapter adapter = new LogicalPlanAdapter(logicalPlan, connectContext.getStatementContext());
            adapter.setOrigStmt(new org.apache.doris.qe.OriginStatement("INSERT INTO "
                    + dorisTable.getName() + " SELECT * FROM " + dorisTable.getName(), 0));
            this.parsedStmt = adapter;

            if (!(logicalPlan instanceof UnboundIcebergTableSink)) {
                throw new UserException("Expected UnboundIcebergTableSink, got: " + logicalPlan.getClass());
            }
            this.initialized = true;

            LOG.debug("Initialized executor with manually constructed logical plan");

        } catch (Exception e) {
            LOG.error("Failed to initialize rewrite executor", e);
            throw new UserException("Failed to initialize rewrite executor: " + e.getMessage());
        }
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

            // Step 2: Execute the customized insert operation using pre-parsed components
            RewriteResult result = executeInsertWithGroup(executor, group);

            LOG.info("Completed rewrite execution for group with {} tasks", group.getTaskCount());

            return result;

        } catch (Exception e) {
            LOG.error("Failed to execute rewrite group: ", e);
            throw new UserException("Rewrite group execution failed: " + e.getMessage());
        } finally {
            // Clean up resources
            if (executor != null) {
                try {
                    executor.finalizeQuery();
                } catch (Exception e) {
                    LOG.warn("Failed to finalize query executor: ", e);
                }
            }
        }
    }

    /**
     * Execute insert operation with specific group customization
     */
    private RewriteResult executeInsertWithGroup(StmtExecutor executor, RewriteDataGroup group) throws Exception {

        // Create IcebergRewriteExecutor directly
        IcebergRewriteExecutor insertExecutor = createRewriteExecutor(executor, group);

        // Execute the insert operation
        if (!insertExecutor.isEmptyInsert()) {
            insertExecutor.beginTransaction();
            insertExecutor.executeSingleInsert(executor, System.currentTimeMillis());
        }

        // Collect execution statistics and rewrite information
        RewriteResult result = collectRewriteResult(insertExecutor, group);
        return result;
    }

    /**
     * Create IcebergRewriteExecutor for rewrite operations
     */
    private IcebergRewriteExecutor createRewriteExecutor(StmtExecutor executor, RewriteDataGroup group)
            throws UserException {
        try {
            // Create NereidsPlanner for the logical plan
            NereidsPlanner planner = new NereidsPlanner(connectContext.getStatementContext());

            // Plan the logical plan to get physical plan and fragments
            planner.plan(parsedStmt, connectContext.getSessionVariable().toThrift());

            // Create IcebergRewriteExecutor directly
            String label = String.format("rewrite_label_%x_%x", connectContext.queryId().hi,
                    connectContext.queryId().lo);
            IcebergRewriteExecutor rewriteExecutor = new IcebergRewriteExecutor(
                    connectContext,
                    dorisTable,
                    label,
                    planner,
                    Optional.empty(), // insertCtx
                    false // emptyInsert
            );

            // Customize the executor for this specific group
            customizeInsertExecutorForRewrite(rewriteExecutor, group);

            return rewriteExecutor;

        } catch (Exception e) {
            LOG.error("Failed to create rewrite executor", e);
            throw new UserException("Failed to create rewrite executor: " + e.getMessage());
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
    private RewriteResult collectRewriteResult(IcebergRewriteExecutor insertExecutor, RewriteDataGroup group) {
        try {
            // Get execution statistics from the executor
            long processedRows = insertExecutor.getLoadedRows();

            // Get detailed file information from IcebergTransaction
            RewriteFileInfo fileInfo = insertExecutor.getRewriteFileInfo();

            // Create rewrite result with collected information
            RewriteResult result = new RewriteResult(
                    fileInfo.getFilesToDeleteCount(),    // rewrittenDataFilesCount (files to delete)
                    fileInfo.getFilesToAddCount(),       // addedDataFilesCount (files to add)
                    fileInfo.getFilesToDeleteSize(),     // rewrittenBytesCount (size of files to delete)
                    0                                   // removedDeleteFilesCount (no delete files in rewrite)
            );

            LOG.info("Rewrite completed for group: {} files to delete, {} files to add, {} rows processed, "
                    + "{} bytes deleted, {} bytes added",
                    fileInfo.getFilesToDeleteCount(), fileInfo.getFilesToAddCount(),
                    processedRows, fileInfo.getFilesToDeleteSize(), fileInfo.getFilesToAddSize());

            return result;

        } catch (Exception e) {
            LOG.warn("Failed to collect rewrite result, using fallback: ", e);
            // Fallback to basic result
            return new RewriteResult(group.getTaskCount(), group.getTaskCount(),
                    group.getTotalSize(), 0);
        }
    }

    /**
     * Build logical plan for rewrite operation (INSERT INTO ... SELECT ...)
     */
    private LogicalPlan buildRewriteLogicalPlan() throws UserException {
        try {
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

            // Return the table sink as the logical plan (not a command)
            return tableSink;

        } catch (Exception e) {
            LOG.error("Failed to build rewrite logical plan", e);
            throw new UserException("Failed to build rewrite logical plan: " + e.getMessage());
        }
    }
}
