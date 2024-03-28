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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.transaction.TransactionEntry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Insert executor for olap table with transaction model
 */
public class OlapTxnInsertExecutor extends OlapInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(OlapTxnInsertExecutor.class);

    public OlapTxnInsertExecutor(ConnectContext ctx, Table table,
            String labelName, NereidsPlanner planner, Optional<InsertCommandContext> insertCtx) {
        super(ctx, table, labelName, planner, insertCtx);
    }

    public long getTxnId() {
        return txnId;
    }

    @Override
    public void beginTransaction() {
        try {
            TransactionEntry txnEntry = ctx.getTxnEntry();
            // check the same label with begin
            if (this.labelName != null && !this.labelName.equals(txnEntry.getLabel())) {
                throw new AnalysisException("Transaction insert expect label " + txnEntry.getLabel()
                        + ", but got " + this.labelName);
            }
            this.txnId = txnEntry.beginTransaction(database, table);
            this.labelName = txnEntry.getLabel();
        } catch (Exception e) {
            throw new AnalysisException("begin transaction failed. " + e.getMessage(), e);
        }
    }

    @Override
    protected void beforeExec() {
        String queryId = DebugUtil.printId(ctx.queryId());
        LOG.info("start insert [{}] with query id {} and txn id {}, txn_model=true", labelName, queryId, txnId);
    }

    @Override
    protected void onComplete() {
        TransactionEntry txnEntry = ctx.getTxnEntry();
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            if (txnId != INVALID_TXN_ID) {
                txnEntry.removeTable((Table) table);
            }
        } else {
            txnEntry.addTabletCommitInfos(txnId, (Table) table, coordinator.getCommitInfos());
        }
    }

    @Override
    protected void onFail(Throwable t) {
        errMsg = t.getMessage() == null ? "unknown reason" : t.getMessage();
        String queryId = DebugUtil.printId(ctx.queryId());
        // if any throwable being thrown during insert operation, first we should abort this txn
        LOG.warn("insert [{}] with query id {} failed, url={}", labelName, queryId, coordinator.getTrackingUrl(), t);
        if (txnId != INVALID_TXN_ID) {
            ctx.getTxnEntry().removeTable((Table) table);
            Env.getCurrentGlobalTransactionMgr().removeSubTransaction(table.getDatabase().getId(), txnId);
        }
    }
}
