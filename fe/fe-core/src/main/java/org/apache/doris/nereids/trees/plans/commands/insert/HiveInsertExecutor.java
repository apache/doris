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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSTransaction;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHiveTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.HiveTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.TransactionManager;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Insert executor for olap table
 */
public class HiveInsertExecutor extends AbstractInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(HiveInsertExecutor.class);
    private static final long INVALID_TXN_ID = -1L;
    private long txnId = INVALID_TXN_ID;
    private TransactionStatus txnStatus = TransactionStatus.ABORTED;
    private final TransactionManager transactionManager;
    private final String catalogName;

    /**
     * constructor
     */
    public HiveInsertExecutor(ConnectContext ctx, HMSExternalTable table,
                              String labelName, NereidsPlanner planner,
                              Optional<InsertCommandContext> insertCtx) {
        super(ctx, table, labelName, planner, insertCtx);
        catalogName = table.getCatalog().getName();
        transactionManager = table.getCatalog().getTransactionManager();
    }

    public long getTxnId() {
        return txnId;
    }

    @Override
    public void beginTransaction() {
        txnId = transactionManager.begin();
        HMSTransaction transaction = (HMSTransaction) transactionManager.getTransaction(txnId);
        coordinator.setHivePartitionUpdateFunc(transaction::updateHivePartitionUpdates);
    }

    @Override
    protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        HiveTableSink hiveTableSink = (HiveTableSink) sink;
        PhysicalHiveTableSink<? extends Plan> physicalHiveSink = (PhysicalHiveTableSink<? extends Plan>) physicalSink;
        try {
            hiveTableSink.bindDataSink(physicalHiveSink.getCols(), insertCtx);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    protected void beforeExec() {
        // check params
    }

    @Override
    protected void onComplete() throws UserException {
        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            LOG.warn("errors when abort txn. {}", ctx.getQueryIdentifier());
        } else {
            HMSTransaction transaction = (HMSTransaction) transactionManager.getTransaction(txnId);
            loadedRows = transaction.getUpdateCnt();
            String dbName = ((HMSExternalTable) table).getDbName();
            String tbName = table.getName();
            transaction.finishInsertTable(dbName, tbName);
            transactionManager.commit(txnId);
            txnStatus = TransactionStatus.COMMITTED;
            Env.getCurrentEnv().getCatalogMgr().refreshExternalTable(
                    dbName,
                    tbName,
                    catalogName,
                    true);
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
        transactionManager.rollback(txnId);
    }

    @Override
    protected void afterExec(StmtExecutor executor) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("'status':'")
                .append(ctx.isTxnModel() ? TransactionStatus.PREPARE.name() : txnStatus.name());
        // sb.append("', 'txnId':'").append(txnId).append("'");
        if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }
        sb.append("}");
        ctx.getState().setOk(loadedRows, 0, sb.toString());
        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        ctx.setOrUpdateInsertResult(txnId, labelName, database.getFullName(), table.getName(),
                txnStatus, loadedRows, 0);
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows((int) loadedRows);
    }
}
