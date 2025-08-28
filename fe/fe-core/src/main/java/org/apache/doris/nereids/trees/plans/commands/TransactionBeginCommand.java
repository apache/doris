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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * TransactionBeginCommand
 */
public class TransactionBeginCommand extends TransactionCommand {
    private static final Logger LOG = LogManager.getLogger(TransactionBeginCommand.class);
    private String label;

    public TransactionBeginCommand() {
        super(PlanType.TRANSACTION_BEGIN_COMMAND);
        this.label = "";
    }

    public TransactionBeginCommand(String label) {
        super(PlanType.TRANSACTION_BEGIN_COMMAND);
        this.label = label;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        handleTransactionBegin(ctx);
    }

    private void validate(ConnectContext ctx) {
        if (label == null || label.isEmpty()) {
            label = "txn_insert_" + DebugUtil.printId(ctx.queryId());
        }
        if (ctx.getTxnEntry() == null) {
            ctx.setTxnEntry(new TransactionEntry());
        }
        ctx.getTxnEntry().setLabel(label);
    }

    private void handleTransactionBegin(ConnectContext ctx) {
        if (ctx.getConnectType() == ConnectContext.ConnectType.MYSQL) {
            // Every time set no send flag and clean all data in buffer
            ctx.getMysqlChannel().reset();
        }
        ctx.getState().setOk(0, 0, "");

        if (ctx.isTxnModel()) {
            LOG.info("A transaction has already begin");
            return;
        }
        if (ctx.getTxnEntry() == null) {
            ctx.setTxnEntry(new TransactionEntry());
        }
        ctx.getTxnEntry()
                .setTxnConf(new TTxnParams().setNeedTxn(true).setThriftRpcTimeoutMs(5000).setTxnId(-1).setDb("")
                .setTbl("").setMaxFilterRatio(ctx.getSessionVariable().getEnableInsertStrict() ? 0
                    : ctx.getSessionVariable().getInsertMaxFilterRatio()));
        ctx.getTxnEntry().setFirstTxnInsert(true);
        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(ctx.getTxnEntry().getLabel()).append("', 'status':'")
            .append(TransactionStatus.PREPARE.name());
        sb.append("', 'txnId':'").append("'").append("}");
        ctx.getState().setOk(0, 0, sb.toString());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitTransactionBeginCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.TRANSACTION;
    }
}
