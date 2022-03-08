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

package org.apache.doris.load.update;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.UpdateStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class UpdateStmtExecutor {
    private static final Logger LOG = LogManager.getLogger(UpdateStmtExecutor.class);

    private OlapTable targetTable;
    private Expr whereExpr;
    private List<Expr> setExprs;
    private long dbId;
    private TUniqueId queryId;
    private int timeoutSecond;
    private Analyzer analyzer;
    private UpdatePlanner updatePlanner;

    private String label;
    private long txnId;
    private Coordinator coordinator;
    private long effectRows;


    public long getTargetTableId() {
        return targetTable.getId();
    }

    public void execute() throws UserException {
        // 0. empty set
        // A where clause with a constant equal to false will not execute the update directly
        // Example: update xxx set v1=0 where 1=2
        if (analyzer.hasEmptyResultSet()) {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
            analyzer.getContext().getState().setOk();
            return;
        }

        // 1. begin txn
        beginTxn();

        // 2. plan
        targetTable.readLock();
        try {
            updatePlanner.plan(txnId);
        } catch (Throwable e) {
            LOG.warn("failed to plan update stmt, query id:{}", DebugUtil.printId(queryId), e);
            Catalog.getCurrentGlobalTransactionMgr().abortTransaction(dbId, txnId, e.getMessage());
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
            throw new DdlException("failed to plan update stmt, query id: " + DebugUtil.printId(queryId) + ", err: " + e.getMessage());
        } finally {
            targetTable.readUnlock();
        }

        // 3. execute plan
        try {
            executePlan();
        } catch (DdlException e) {
            LOG.warn("failed to execute update stmt, query id:{}", DebugUtil.printId(queryId), e);
            Catalog.getCurrentGlobalTransactionMgr().abortTransaction(dbId, txnId, e.getMessage());
            throw e;
        } catch (Throwable e) {
            LOG.warn("failed to execute update stmt, query id:{}", DebugUtil.printId(queryId), e);
            Catalog.getCurrentGlobalTransactionMgr().abortTransaction(dbId, txnId, e.getMessage());
            throw new DdlException("failed to execute update stmt, query id: " + DebugUtil.printId(queryId) + ", err: " + e.getMessage());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
        
        // 4. commit and publish
        commitAndPublishTxn();
    }

    private void beginTxn() throws LabelAlreadyUsedException, AnalysisException, BeginTransactionException,
            DuplicatedRequestException, QuotaExceedException, MetaNotFoundException {
        LOG.info("begin transaction for update stmt, query id:{}", DebugUtil.printId(queryId));
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);
        label = "update_" + DebugUtil.printId(queryId);
        txnId = Catalog.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, Lists.newArrayList(targetTable.getId()), label,
                        new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                        LoadJobSourceType.INSERT_STREAMING, timeoutSecond);
    }

    // TODO(ML): Abstract the logic of executing the coordinater and retrying.
    //           It makes stmt such as insert, load, update and export can be reused
    private void executePlan() throws Exception {
        LOG.info("begin execute update stmt, query id:{}", DebugUtil.printId(queryId));
        coordinator = new Coordinator(Catalog.getCurrentCatalog().getNextId(), queryId, analyzer.getDescTbl(),
                updatePlanner.getFragments(), updatePlanner.getScanNodes(), TimeUtils.DEFAULT_TIME_ZONE, false);
        coordinator.setQueryType(TQueryType.LOAD);
        coordinator.setExecVecEngine(VectorizedUtil.isVectorized());
        QeProcessorImpl.INSTANCE.registerQuery(queryId, coordinator);
        analyzer.getContext().getExecutor().setCoord(coordinator);

        // execute
        coordinator.setTimeout(timeoutSecond);
        coordinator.exec();
        if (coordinator.join(timeoutSecond)) {
            if (!coordinator.isDone()) {
                coordinator.cancel();
                ErrorReport.reportDdlException(ErrorCode.ERR_EXECUTE_TIMEOUT);
            }
            if (!coordinator.getExecStatus().ok()) {
                String errMsg = "update failed: " + coordinator.getExecStatus().getErrorMsg();
                LOG.warn(errMsg);
                throw new DdlException(errMsg);
            }
            LOG.info("finish to execute update stmt, query id:{}", DebugUtil.printId(queryId));
        } else {
            String errMsg = "coordinator could not finished before update timeout: "
                    + coordinator.getExecStatus().getErrorMsg();
            LOG.warn(errMsg);
            throw new DdlException(errMsg);
        }

        // counter
        if (coordinator.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL) != null) {
            effectRows = Long.valueOf(coordinator.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL));
            if (Long.valueOf(coordinator.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL)) != 0) {
                throw new DdlException("update failed, some rows did not take effect");
            }
        }
    }

    private void commitAndPublishTxn() throws UserException {
        GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        TransactionStatus txnStatus;
        boolean isPublished;
        try {
            LOG.info("commit and publish transaction for update stmt, query id: {}", DebugUtil.printId(queryId));
            isPublished = globalTransactionMgr.commitAndPublishTransaction(
                    Catalog.getCurrentCatalog().getDbOrMetaException(dbId),
                    Lists.newArrayList(targetTable),
                    txnId,
                    TabletCommitInfo.fromThrift(coordinator.getCommitInfos()),
                    analyzer.getContext().getSessionVariable().getInsertVisibleTimeoutMs());
        } catch (Throwable e) {
            // situation2.1: publish error, throw exception
            String errMsg = "failed to commit and publish transaction for update stmt, query id:"
                    + DebugUtil.printId(queryId);
            LOG.warn(errMsg, e);
            globalTransactionMgr.abortTransaction(dbId, txnId, e.getMessage());
            throw new DdlException(errMsg, e);
        }
        String errMsg = null;
        if (isPublished) {
            // situation2.2: publish successful
            txnStatus = TransactionStatus.VISIBLE;
            MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
        } else {
            // situation2.3: be published later
            txnStatus = TransactionStatus.COMMITTED;
            errMsg = "transaction will be published later, data will be visible later";
            LOG.warn("transaction will be published later, query id: {}", DebugUtil.printId(queryId));
        }

        // set context
        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(label).append("', 'status':'").append(txnStatus.name()).append("'");
        sb.append(", 'txnId':'").append(txnId).append("'");
        sb.append(", 'queryId':'").append(DebugUtil.printId(queryId)).append("'");
        if (errMsg != null) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }
        sb.append("}");
        analyzer.getContext().getState().setOk(effectRows, 0, sb.toString());
    }

    public static UpdateStmtExecutor fromUpdateStmt(UpdateStmt updateStmt) throws AnalysisException {
        UpdateStmtExecutor updateStmtExecutor = new UpdateStmtExecutor();
        updateStmtExecutor.targetTable = (OlapTable) updateStmt.getTargetTable();
        updateStmtExecutor.whereExpr = updateStmt.getWhereExpr();
        updateStmtExecutor.setExprs = updateStmt.getSetExprs();
        Database database = Catalog.getCurrentCatalog().getDbOrAnalysisException(updateStmt.getTableName().getDb());
        updateStmtExecutor.dbId = database.getId();
        updateStmtExecutor.analyzer = updateStmt.getAnalyzer();
        updateStmtExecutor.queryId = updateStmtExecutor.analyzer.getContext().queryId();
        updateStmtExecutor.timeoutSecond = updateStmtExecutor.analyzer.getContext()
                .getSessionVariable().getQueryTimeoutS();
        updateStmtExecutor.updatePlanner = new UpdatePlanner(updateStmtExecutor.dbId, updateStmtExecutor.targetTable,
                updateStmt.getSetExprs(), updateStmt.getSrcTupleDesc(),
                updateStmt.getAnalyzer());
        return updateStmtExecutor;
    }

}
