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

package org.apache.doris.transaction;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.InsertStreamTxnExecutor;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.MasterTxnExecutor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TTxnLoadInfo;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.SubTransactionState.SubTransactionType;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TransactionEntry {

    private static final Logger LOG = LogManager.getLogger(TransactionEntry.class);

    private String label = "";
    private DatabaseIf database;
    private long dbId = -1;

    // for insert into values for one table
    private Table table;
    private Backend backend;
    private TTxnParams txnConf;
    private List<InternalService.PDataRow> dataToSend = new ArrayList<>();
    private long rowsInTransaction = 0;
    private Types.PUniqueId pLoadId;

    // for insert into select for multi tables
    private boolean isTransactionBegan = false;
    private long transactionId = -1;
    private TransactionState transactionState;
    private long timeoutTimestamp = -1;

    public TransactionEntry() {
    }

    public TransactionEntry(TTxnParams txnConf, Database db, Table table) {
        this.txnConf = txnConf;
        this.database = db;
        this.dbId = this.database.getId();
        this.table = table;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public DatabaseIf getDb() {
        return database;
    }

    public void setDb(Database db) {
        this.database = db;
        this.dbId = this.database.getId();
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Backend getBackend() {
        return backend;
    }

    public void setBackend(Backend backend) {
        this.backend = backend;
    }

    public TTxnParams getTxnConf() {
        return txnConf;
    }

    public void setTxnConf(TTxnParams txnConf) {
        this.txnConf = txnConf;
    }

    public boolean isTxnModel() {
        return txnConf != null && txnConf.isNeedTxn();
    }

    public boolean isInsertValuesTxnIniting() {
        return isTxnModel() && txnConf.getTxnId() == -1;
    }

    private boolean isInsertValuesTxnBegan() {
        return isTxnModel() && txnConf.getTxnId() != -1;
    }

    public List<InternalService.PDataRow> getDataToSend() {
        return dataToSend;
    }

    public void setDataToSend(List<InternalService.PDataRow> dataToSend) {
        this.dataToSend = dataToSend;
    }

    public void clearDataToSend() {
        dataToSend.clear();
    }

    public long getRowsInTransaction() {
        return rowsInTransaction;
    }

    public void setRowsInTransaction(long rowsInTransaction) {
        this.rowsInTransaction = rowsInTransaction;
    }

    public Types.PUniqueId getpLoadId() {
        return pLoadId;
    }

    public void setpLoadId(Types.PUniqueId pLoadId) {
        this.pLoadId = pLoadId;
    }

    // Used for insert into select, return the sub_txn_id for this insert
    public long beginTransaction(TableIf table) throws Exception {
        if (isInsertValuesTxnBegan()) {
            // FIXME: support mix usage of `insert into values` and `insert into select`
            throw new AnalysisException(
                    "Transaction insert can not insert into values and insert into select at the same time");
        }
        DatabaseIf database = table.getDatabase();
        if (!isTransactionBegan) {
            long timeoutSecond = ConnectContext.get().getExecTimeout();
            this.timeoutTimestamp = System.currentTimeMillis() + timeoutSecond * 1000;
            if (Env.getCurrentEnv().isMaster()) {
                this.transactionId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                        database.getId(), Lists.newArrayList(table.getId()), label,
                        new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                        LoadJobSourceType.INSERT_STREAMING, timeoutSecond);
            } else {
                String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
                MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(ConnectContext.get());
                TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();
                request.setDb(database.getFullName()).setTbl(table.getName()).setToken(token)
                        .setLabel(label).setUser("").setUserIp("").setPasswd("").setTimeout(timeoutSecond);
                TLoadTxnBeginResult result = masterTxnExecutor.beginTxn(request);
                this.transactionId = result.getTxnId();
            }
            this.isTransactionBegan = true;
            this.database = database;
            this.dbId = this.database.getId();
            this.transactionState = Env.getCurrentGlobalTransactionMgr()
                    .getTransactionState(database.getId(), transactionId);
            this.transactionState.resetSubTransactionStates();
            return this.transactionId;
        } else {
            if (this.database.getId() != database.getId()) {
                throw new AnalysisException(
                        "Transaction insert must be in the same database, expect db_id=" + this.database.getId());
            }
            this.transactionState.addTableId(table.getId());
            long subTxnId = Env.getCurrentGlobalTransactionMgr().getNextTransactionId();
            Env.getCurrentGlobalTransactionMgr().addSubTransaction(database.getId(), transactionId, subTxnId);
            return subTxnId;
        }
    }

    public TransactionStatus commitTransaction() throws Exception {
        if (isTransactionBegan) {
            try {
                if (Env.getCurrentEnv().isMaster()) {
                    beforeFinishTransaction();
                    if (Env.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(database, transactionId,
                            transactionState.getSubTransactionStates(),
                            ConnectContext.get().getSessionVariable().getInsertVisibleTimeoutMs())) {
                        return TransactionStatus.VISIBLE;
                    } else {
                        return TransactionStatus.COMMITTED;
                    }
                } else {
                    OriginStatement originStmt = new OriginStatement("commit", 0);
                    MasterOpExecutor masterOpExecutor = new MasterOpExecutor(originStmt, ConnectContext.get(),
                            RedirectStatus.NO_FORWARD, false);
                    LOG.info("forward commit to Master for txn_id={}", this.transactionId);
                    masterOpExecutor.execute();
                    return waitingTxnVisible(this.dbId, this.transactionId);
                }
            } catch (UserException e) {
                LOG.error("Failed to commit transaction", e);
                try {
                    abortTransaction(e.getMessage());
                } catch (Exception e1) {
                    LOG.error("Failed to abort transaction", e1);
                }
                throw e;
            }
        } else if (isInsertValuesTxnBegan()) {
            InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(this);
            if (dataToSend.size() > 0) {
                // send rest data
                executor.sendData();
            }
            // commit txn
            executor.commitTransaction();
            // wait txn visible
            return waitingTxnVisible(txnConf.getDbId(), txnConf.getTxnId());
        } else {
            LOG.info("No transaction to commit");
            return null;
        }
    }

    private TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request) throws Exception {
        TWaitingTxnStatusResult statusResult = null;
        if (Env.getCurrentEnv().isMaster()) {
            statusResult = Env.getCurrentGlobalTransactionMgr().getWaitingTxnStatus(request);
        } else {
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(ConnectContext.get());
            statusResult = masterTxnExecutor.getWaitingTxnStatus(request);
        }
        return statusResult;
    }

    public long abortTransaction() throws Exception {
        return abortTransaction("user rollback");
    }

    private long abortTransaction(String reason) throws Exception {
        if (isTransactionBegan) {
            if (Env.getCurrentEnv().isMaster()) {
                beforeFinishTransaction();
                Env.getCurrentGlobalTransactionMgr().abortTransaction(database.getId(), transactionId, reason);
                return transactionId;
            } else {
                OriginStatement originStmt = new OriginStatement("rollback", 0);
                MasterOpExecutor masterOpExecutor = new MasterOpExecutor(originStmt, ConnectContext.get(),
                        RedirectStatus.NO_FORWARD, false);
                LOG.info("forward rollback to Master for txn_id={}", transactionId);
                masterOpExecutor.execute();
                return transactionId;
            }
        } else if (isInsertValuesTxnBegan()) {
            InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(this);
            executor.abortTransaction();
            return txnConf.getTxnId();
        } else {
            LOG.info("No transaction to abort");
            return -1;
        }
    }

    private void beforeFinishTransaction() {
        if (isTransactionBegan) {
            List<Long> tableIds = transactionState.getTableIdList().stream().distinct().collect(Collectors.toList());
            transactionState.setTableIdList(tableIds);
            transactionState.getSubTransactionStates().sort((s1, s2) -> {
                if (s1.getSubTransactionType() == SubTransactionType.INSERT
                        && s2.getSubTransactionType() == SubTransactionType.DELETE) {
                    return 1;
                } else if (s1.getSubTransactionType() == SubTransactionType.DELETE
                        && s2.getSubTransactionType() == SubTransactionType.INSERT) {
                    return -1;
                } else {
                    return Long.compare(s1.getSubTransactionId(), s2.getSubTransactionId());
                }
            });
            LOG.info("subTransactionStates={}", transactionState.getSubTransactionStates());
            transactionState.resetSubTxnIds();
        }
    }

    public long getTransactionId() {
        if (isTransactionBegan) {
            return transactionId;
        } else if (isInsertValuesTxnBegan()) {
            return txnConf.getTxnId();
        } else {
            return -1;
        }
    }

    public void abortSubTransaction(long subTransactionId, Table table) {
        if (isTransactionBegan) {
            this.transactionState.removeTableId(table.getId());
            Env.getCurrentGlobalTransactionMgr().removeSubTransaction(table.getDatabase().getId(), subTransactionId);
        }
    }

    public void addTabletCommitInfos(long subTxnId, Table table, List<TTabletCommitInfo> commitInfos,
            SubTransactionType subTransactionType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("label={}, txn_id={}, sub_txn_id={}, table={}, commit_infos={}",
                    label, transactionId, subTxnId, table, commitInfos);
        }
        transactionState.getSubTransactionStates()
                .add(new SubTransactionState(subTxnId, table, commitInfos, subTransactionType));
        Preconditions.checkState(
                transactionState.getTableIdList().size() == transactionState.getSubTransactionStates().size(),
                "txn_id=" + transactionId + ", expect table_list=" + transactionState.getSubTransactionStates().stream()
                        .map(s -> s.getTable().getId()).collect(Collectors.toList()) + ", real table_list="
                        + transactionState.getTableIdList());
    }

    public boolean isTransactionBegan() {
        return this.isTransactionBegan;
    }

    public long getTimeout() {
        return (timeoutTimestamp - System.currentTimeMillis()) / 1000;
    }

    private TransactionStatus waitingTxnVisible(long dbId, long transactionId) throws Exception {
        // wait txn visible
        TWaitingTxnStatusRequest request = new TWaitingTxnStatusRequest();
        request.setDbId(dbId).setTxnId(transactionId);
        request.setLabelIsSet(false);
        request.setTxnIdIsSet(true);

        TWaitingTxnStatusResult statusResult = getWaitingTxnStatus(request);
        TransactionStatus txnStatus = TransactionStatus.valueOf(statusResult.getTxnStatusId());
        if (txnStatus == TransactionStatus.COMMITTED) {
            throw new AnalysisException("transaction commit successfully, BUT data will be visible later.");
        } else if (txnStatus != TransactionStatus.VISIBLE) {
            String errMsg = "commit failed, rollback.";
            if (statusResult.getStatus().isSetErrorMsgs()
                    && statusResult.getStatus().getErrorMsgs().size() > 0) {
                errMsg = String.join(". ", statusResult.getStatus().getErrorMsgs());
            }
            throw new AnalysisException(errMsg);
        }
        return txnStatus;
    }

    /**
     * handle 2 forward cases:
     * 1. the first dml stmt in a txn, now the txn is not began: only know the [label]
     *    beginTxn -> txnId
     *    calculate timeoutTimestamp
     *    set dbId
     * 2. the others dml stmts in a txn, now the txn is already began: know the [label, txnId, dbId, timeoutTimestamp]
     */
    public void setTxnInfoInMaster(TTxnLoadInfo txnLoadInfo) throws DdlException {
        this.setTxnConf(new TTxnParams().setNeedTxn(true).setTxnId(-1));
        this.label = txnLoadInfo.getLabel();
        if (txnLoadInfo.isSetTxnId()) {
            this.dbId = txnLoadInfo.getDbId();
            this.database = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
            this.transactionId = txnLoadInfo.getTxnId();
            this.transactionState = Env.getCurrentGlobalTransactionMgr().getTransactionState(dbId, transactionId);
            Preconditions.checkNotNull(this.transactionState,
                    "db_id" + dbId + " txn_id=" + transactionId + " not found");
            Preconditions.checkState(this.label.equals(this.transactionState.getLabel()), "expected label="
                    + this.label + ", real label=" + this.transactionState.getLabel());
            this.isTransactionBegan = true;
            this.timeoutTimestamp = txnLoadInfo.getTimeoutTimestamp();
        }
        LOG.info("set txn info in master, label={}, txnId={}, dbId={}, timeoutTimestamp={}",
                label, transactionId, dbId, timeoutTimestamp);
    }

    public TTxnLoadInfo getTxnInfoInMaster() {
        TTxnLoadInfo txnLoadInfo = new TTxnLoadInfo();
        txnLoadInfo.setLabel(label);
        txnLoadInfo.setTxnId(transactionId);
        txnLoadInfo.setDbId(dbId);
        txnLoadInfo.setTimeoutTimestamp(timeoutTimestamp);
        LOG.info("master return txn info: {}", txnLoadInfo);
        return txnLoadInfo;
    }

    public TTxnLoadInfo getTxnLoadInfoInObserver() throws AnalysisException {
        if (isInsertValuesTxnBegan()) {
            throw new AnalysisException(
                    "Transaction insert can not insert into values and insert into select at the same time");
        }
        TTxnLoadInfo txnLoadInfo = new TTxnLoadInfo();
        txnLoadInfo.setLabel(label);
        if (this.isTransactionBegan) {
            txnLoadInfo.setTxnId(transactionId);
            txnLoadInfo.setDbId(dbId);
            txnLoadInfo.setTimeoutTimestamp(timeoutTimestamp);
        }
        LOG.info("get txn load info in observer: {}", txnLoadInfo);
        return txnLoadInfo;
    }

    public void setTxnLoadInfoInObserver(TTxnLoadInfo txnLoadInfo) {
        Preconditions.checkState(txnLoadInfo.getLabel().equals(this.label),
                "expected label=" + this.label + ", real label=" + txnLoadInfo.getLabel());
        this.isTransactionBegan = true;
        this.transactionId = txnLoadInfo.txnId;
        this.timeoutTimestamp = txnLoadInfo.timeoutTimestamp;
        this.dbId = txnLoadInfo.dbId;
        LOG.info("set txn load info in observer, label={}, txnId={}, dbId={}, timeoutTimestamp={}",
                label, transactionId, dbId, timeoutTimestamp);
    }
}
