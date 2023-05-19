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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.txn.Transaction;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * insert into select command implementation
 * insert into select command support the grammer: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 *  InsertIntoTableCommand(Query())
 *  ExplainCommand(Query())
 */
public class InsertIntoTableCommand extends Command implements ForwardWithSync {
    public static final Logger LOG = LogManager.getLogger(InsertIntoTableCommand.class);

    private final LogicalPlan logicalQuery;
    private final String labelName;
    private NereidsPlanner planner;

    /**
     * constructor
     */
    public InsertIntoTableCommand(LogicalPlan logicalQuery, String labelName) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        Preconditions.checkArgument(logicalQuery != null, "logicalQuery cannot be null in InsertIntoTableCommand");
        this.logicalQuery = logicalQuery;
        this.labelName = labelName;
    }

    public NereidsPlanner getPlanner() {
        return planner;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (ctx.isTxnModel()) {
            // in original planner with txn model, we can execute sql like: insert into t select 1, 2, 3
            // but no data will be inserted, now we adjust forbid it.
            throw new AnalysisException("insert into table command is not supported in txn model");
        }

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());

        if (ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }
        String label = this.labelName;
        if (label == null) {
            label = String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo);
        }

        PhysicalOlapTableSink<?> physicalOlapTableSink = ((PhysicalOlapTableSink) planner.getPhysicalPlan());
        OlapTableSink sink = ((OlapTableSink) planner.getFragments().get(0).getSink());

        Transaction txn = new Transaction(ctx,
                physicalOlapTableSink.getDatabase(),
                physicalOlapTableSink.getTargetTable(), label, planner);

        sink.init(ctx.queryId(), txn.getTxnId(),
                physicalOlapTableSink.getDatabase().getId(),
                ctx.getExecTimeout(),
                ctx.getSessionVariable().getSendBatchParallelism(), false);

        sink.complete();
        TransactionState state = Env.getCurrentGlobalTransactionMgr().getTransactionState(
                physicalOlapTableSink.getDatabase().getId(),
                txn.getTxnId());
        if (state == null) {
            throw new DdlException("txn does not exist: " + txn.getTxnId());
        }
        state.addTableIndexes(physicalOlapTableSink.getTargetTable());

        txn.executeInsertIntoSelectCommand(executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoCommand(this, context);
    }
}
