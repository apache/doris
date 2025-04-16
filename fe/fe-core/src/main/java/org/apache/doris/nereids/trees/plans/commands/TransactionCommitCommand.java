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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * TransactionCommitCommand
 */
public class TransactionCommitCommand extends TransactionCommand {
    private static final Logger LOG = LogManager.getLogger(TransactionCommitCommand.class);

    public TransactionCommitCommand() {
        super(PlanType.TRANSACTION_COMMIT_COMMAND);
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        handleTransactionCommit(ctx);
    }

    private void handleTransactionCommit(ConnectContext ctx) throws AnalysisException {
        if (ctx.getConnectType() == ConnectContext.ConnectType.MYSQL) {
            // Every time set no send flag and clean all data in buffer
            ctx.getMysqlChannel().reset();
        }
        ctx.getState().setOk(0, 0, "");

        if (ctx.getTxnEntry() != null && ctx.getTxnEntry().getRowsInTransaction() == 0
                && !ctx.getTxnEntry().isTransactionBegan()) {
            ctx.setTxnEntry(null);
        } else {
            if (!ctx.isTxnModel()) {
                LOG.info("No transaction to commit");
                return;
            }
            try {
                TransactionEntry txnEntry = ctx.getTxnEntry();
                TransactionStatus txnStatus = txnEntry.commitTransaction();
                StringBuilder sb = new StringBuilder();
                sb.append("{'label':'").append(txnEntry.getLabel()).append("', 'status':'")
                    .append(txnStatus.name()).append("', 'txnId':'")
                    .append(txnEntry.getTransactionId()).append("'").append("}");
                ctx.getState().setOk(0, 0, sb.toString());
            } catch (Exception e) {
                LOG.warn("Txn commit failed", e);
                throw new AnalysisException(e.getMessage());
            } finally {
                ctx.setTxnEntry(null);
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitTransactionCommitCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.TRANSACTION;
    }
}
