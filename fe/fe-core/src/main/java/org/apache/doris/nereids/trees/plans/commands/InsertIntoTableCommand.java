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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.ProfileManager.ProfileType;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.txn.Transaction;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * insert into select command implementation
 * insert into select command support the grammer: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 *  InsertIntoTableCommand(Query())
 *  ExplainCommand(Query())
 */
public class InsertIntoTableCommand extends Command implements ForwardWithSync, Explainable {

    public static final Logger LOG = LogManager.getLogger(InsertIntoTableCommand.class);

    private final LogicalPlan logicalQuery;
    private final Optional<String> labelName;
    private NereidsPlanner planner;
    private boolean isTxnBegin = false;

    /**
     * constructor
     */
    public InsertIntoTableCommand(LogicalPlan logicalQuery, Optional<String> labelName) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery,
                "logicalQuery cannot be null in InsertIntoTableCommand");
        this.labelName = labelName;
    }

    public NereidsPlanner getPlanner() {
        return planner;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }
        if (ctx.isTxnModel()) {
            // in original planner, if is in txn model, insert into select command and tableRef >= 1 will be refused.
            // we can just run select a one-row-relation like select 1, 2, 3
            // in StmtExecutor#executeForTxn, select 1, 2, 3 means valueList is null, so the if-clause from line 1556
            // to 1580 will be skipped and effect rows will be always 0
            // in nereids, we just forbid it.
            throw new AnalysisException("insert into table command is not supported in txn model");
        }

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        executor.checkBlockRules();
        if (ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }
        String label = this.labelName.orElse(String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo));

        Optional<TreeNode> plan = ((Set<TreeNode>) planner.getPhysicalPlan()
                .collect(node -> node instanceof PhysicalOlapTableSink)).stream().findAny();
        Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
        PhysicalOlapTableSink<?> physicalOlapTableSink = ((PhysicalOlapTableSink) plan.get());

        OlapTableSink sink = ((OlapTableSink) planner.getFragments().get(0).getSink());

        Preconditions.checkArgument(!isTxnBegin, "an insert command cannot create more than one txn");
        Transaction txn = new Transaction(ctx,
                physicalOlapTableSink.getDatabase(),
                physicalOlapTableSink.getTargetTable(), label, planner);
        isTxnBegin = true;
        boolean isStrictMode = (ctx.getSessionVariable().getEnableInsertStrict()
                && physicalOlapTableSink.isPartialUpdate()
                && physicalOlapTableSink.isFromNativeInsertStmt());
        sink.init(ctx.queryId(), txn.getTxnId(),
                physicalOlapTableSink.getDatabase().getId(),
                ctx.getExecTimeout(),
                ctx.getSessionVariable().getSendBatchParallelism(),
                false,
                isStrictMode);

        sink.complete(new Analyzer(Env.getCurrentEnv(), ctx));
        TransactionState state = Env.getCurrentGlobalTransactionMgr().getTransactionState(
                physicalOlapTableSink.getDatabase().getId(),
                txn.getTxnId());
        if (state == null) {
            throw new DdlException("txn does not exist: " + txn.getTxnId());
        }
        state.addTableIndexes(physicalOlapTableSink.getTargetTable());
        if (physicalOlapTableSink.isFromNativeInsertStmt() && physicalOlapTableSink.isPartialUpdate()) {
            state.setSchemaForPartialUpdate(physicalOlapTableSink.getTargetTable());
        }

        executor.setProfileType(ProfileType.LOAD);

        LOG.info("Nereids start to execute the insert command, query id: {}, txn id: {}",
                ctx.queryId(), txn.getTxnId());

        txn.executeInsertIntoTableCommand(executor);
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            try {
                String errMsg = Strings.emptyToNull(ctx.getState().getErrorMessage());
                Env.getCurrentGlobalTransactionMgr().abortTransaction(
                        physicalOlapTableSink.getDatabase().getId(), txn.getTxnId(),
                        (errMsg == null ? "unknown reason" : errMsg));
            } catch (Exception abortTxnException) {
                LOG.warn("errors when abort txn. {}", ctx.getQueryIdentifier(), abortTxnException);
            }
        }
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return this.logicalQuery;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoCommand(this, context);
    }
}
