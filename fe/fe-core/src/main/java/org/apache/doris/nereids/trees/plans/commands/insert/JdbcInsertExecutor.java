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

import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TransactionType;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Insert executor for jdbc table
 */
public class JdbcInsertExecutor extends BaseExternalTableInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(JdbcInsertExecutor.class);

    /**
     * constructor
     */
    public JdbcInsertExecutor(ConnectContext ctx, JdbcExternalTable table,
            String labelName, NereidsPlanner planner,
            Optional<InsertCommandContext> insertCtx,
            boolean emptyInsert) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert);
    }

    @Override
    public void beginTransaction() {
        // do nothing
    }

    @Override
    protected void onComplete() throws UserException {
        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            LOG.warn("errors when abort txn. {}", ctx.getQueryIdentifier());
        } else {
            summaryProfile.ifPresent(profile -> profile.setTransactionBeginTime(transactionType()));
            summaryProfile.ifPresent(SummaryProfile::setTransactionEndTime);
            txnStatus = TransactionStatus.COMMITTED;
        }
    }

    @Override
    protected void onFail(Throwable t) {
        errMsg = t.getMessage() == null ? "unknown reason" : t.getMessage();
        String queryId = DebugUtil.printId(ctx.queryId());
        // if any throwable being thrown during insert operation, first we should abort this txn
        LOG.warn("insert [{}] with query id {} failed", labelName, queryId, t);
        StringBuilder sb = new StringBuilder(t.getMessage());
        if (txnId != INVALID_TXN_ID) {
            LOG.warn("insert [{}] with query id {} abort txn {} failed", labelName, queryId, txnId);
            if (!Strings.isNullOrEmpty(coordinator.getTrackingUrl())) {
                sb.append(". url: ").append(coordinator.getTrackingUrl());
            }
        }
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, t.getMessage());
    }

    @Override
    protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        // do nothing
    }

    @Override
    protected void setCollectCommitInfoFunc() {
        // do nothing
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        // do nothing
    }

    @Override
    protected TransactionType transactionType() {
        return TransactionType.JDBC;
    }

    @Override
    protected void beforeExec() {
        String queryId = DebugUtil.printId(ctx.queryId());
        LOG.info("start insert [{}] with query id {} and txn id {}", labelName, queryId, txnId);
    }
}
