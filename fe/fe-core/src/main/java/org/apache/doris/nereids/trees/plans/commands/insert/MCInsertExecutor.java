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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.maxcompute.MCTransaction;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.MaxComputeTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.TransactionType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * MCInsertExecutor for MaxCompute external table insert.
 */
public class MCInsertExecutor extends BaseExternalTableInsertExecutor {

    private static final Logger LOG = LogManager.getLogger(MCInsertExecutor.class);

    // Saved during finalizeSink() so we can inject writeSessionId before execution
    private MaxComputeTableSink mcTableSink;

    public MCInsertExecutor(ConnectContext ctx, MaxComputeExternalTable table,
                            String labelName, NereidsPlanner planner,
                            Optional<InsertCommandContext> insertCtx,
                            boolean emptyInsert, long jobId) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert, jobId);
    }

    @Override
    protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        LOG.info("MC_DIAG stage=FE_MC_FINALIZE_SINK_ENTER queryId={} label={} table={}",
                DebugUtil.printId(ctx.queryId()), labelName, table.getName());
        // Let parent call bindDataSink() to build the Thrift sink
        super.finalizeSink(fragment, sink, physicalSink);
        // Save reference so beforeExec() can inject writeSessionId later
        mcTableSink = (MaxComputeTableSink) sink;
        LOG.info("MC_DIAG stage=FE_MC_FINALIZE_SINK_EXIT queryId={} label={} table={} hasSink={}",
                DebugUtil.printId(ctx.queryId()), labelName, table.getName(), mcTableSink != null);
    }

    @Override
    protected void beforeExec() throws UserException {
        long startMs = System.currentTimeMillis();
        LOG.info("MC_DIAG stage=FE_MC_BEFORE_EXEC_ENTER queryId={} label={} table={} txnId={}",
                DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId);
        // 1. Create Storage API write session as part of the transaction
        MCTransaction transaction = (MCTransaction) transactionManager.getTransaction(txnId);
        long beginInsertStartMs = System.currentTimeMillis();
        LOG.info("MC_DIAG stage=FE_MC_BEGIN_INSERT_BEFORE queryId={} label={} table={} txnId={}",
                DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId);
        transaction.beginInsert((MaxComputeExternalTable) table, insertCtx);
        LOG.info("MC_DIAG stage=FE_MC_BEGIN_INSERT_AFTER queryId={} label={} table={} txnId={} sessionId={}"
                        + " costMs={}",
                DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId,
                transaction.getWriteSessionId(), System.currentTimeMillis() - beginInsertStartMs);

        // 2. Inject write context into the Thrift sink before fragments are sent to BE
        if (mcTableSink != null) {
            LOG.info("MC_DIAG stage=FE_MC_SET_WRITE_CONTEXT_BEFORE queryId={} label={} table={} txnId={}"
                            + " sessionId={}",
                    DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId,
                    transaction.getWriteSessionId());
            mcTableSink.setWriteContext(txnId, transaction.getWriteSessionId());
            LOG.info("MC_DIAG stage=FE_MC_SET_WRITE_CONTEXT_AFTER queryId={} label={} table={} txnId={}"
                            + " sessionId={}",
                    DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId,
                    transaction.getWriteSessionId());
        } else {
            LOG.info("MC_DIAG stage=FE_MC_SET_WRITE_CONTEXT_SKIP queryId={} label={} table={} txnId={}",
                    DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId);
        }
        LOG.info("MC_DIAG stage=FE_MC_BEFORE_EXEC_EXIT queryId={} label={} table={} txnId={} sessionId={}"
                        + " costMs={}",
                DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId,
                transaction.getWriteSessionId(), System.currentTimeMillis() - startMs);
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        long startMs = System.currentTimeMillis();
        LOG.info("MC_DIAG stage=FE_MC_DO_BEFORE_COMMIT_ENTER queryId={} label={} table={} txnId={}",
                DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId);
        MCTransaction transaction = (MCTransaction) transactionManager.getTransaction(txnId);
        loadedRows = transaction.getUpdateCnt();
        LOG.info("MC_DIAG stage=FE_MC_FINISH_INSERT_BEFORE queryId={} label={} table={} txnId={} loadedRows={}"
                        + " sessionId={}",
                DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId, loadedRows,
                transaction.getWriteSessionId());
        transaction.finishInsert();
        LOG.info("MC_DIAG stage=FE_MC_FINISH_INSERT_AFTER queryId={} label={} table={} txnId={} loadedRows={}"
                        + " sessionId={} costMs={}",
                DebugUtil.printId(ctx.queryId()), labelName, table.getName(), txnId, loadedRows,
                transaction.getWriteSessionId(), System.currentTimeMillis() - startMs);
    }

    @Override
    protected TransactionType transactionType() {
        return TransactionType.MAXCOMPUTE;
    }
}
