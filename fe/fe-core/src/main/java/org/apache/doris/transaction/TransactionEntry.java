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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cloud.transaction.CloudGlobalTransactionMgr;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.InsertStreamTxnExecutor;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.MasterTxnExecutor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TSubTxnInfo;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TTxnLoadInfo;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
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
import java.util.Set;
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
    private boolean isFirstTxnInsert = false;
    private volatile int txnSchemaVersion = -1;

    // for insert into select for multi tables
    private boolean isTransactionBegan = false;
    private long transactionId = -1;
    private TransactionState transactionState;
    private long timeoutTimestamp = -1;
    private List<SubTransactionState> subTransactionStates = new ArrayList<>();
    // Used for cloud mode, including all successful or failed sub transactions except the first one
    private long allSubTxnNum = 0;

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

    public boolean isFirstTxnInsert() {
        return isFirstTxnInsert;
    }

    public void setFirstTxnInsert(boolean firstTxnInsert) {
        isFirstTxnInsert = firstTxnInsert;
    }

    public int getTxnSchemaVersion() {
        return txnSchemaVersion;
    }

    public void setTxnSchemaVersion(int txnSchemaVersion) {
        this.txnSchemaVersion = txnSchemaVersion;
    }

    // Used for insert into select, return the sub_txn_id for this insert
    public long beginTransaction(TableIf table, SubTransactionType subTransactionType) throws Exception {
        if (isInsertValuesTxnBegan()) {
            // FIXME: support mix usage of `insert into values` and `insert into select`
            throw new AnalysisException(
                    "Transaction insert can not insert into values and insert into select at the same time");
        }
        if (Config.isCloudMode()) {
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getKeysType() == KeysType.UNIQUE_KEYS && olapTable.getEnableUniqueKeyMergeOnWrite()) {
                throw new UserException(
                        "Transaction load is not supported for merge on write unique keys table in cloud mode");
            }
        }
        DatabaseIf database = table.getDatabase();
        if (!isTransactionBegan) {
            long timeoutSecond = ConnectContext.get().getExecTimeout();
            this.timeoutTimestamp = System.currentTimeMillis() + timeoutSecond * 1000;
            if (Env.getCurrentEnv().isMaster() || Config.isCloudMode()) {
                this.transactionId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                        database.getId(), Lists.newArrayList(table.getId()), label,
                        new TxnCoordinator(TxnSourceType.FE, 0,
                                FrontendOptions.getLocalHostAddress(),
                                ExecuteEnv.getInstance().getStartupTime()),
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
            return this.transactionId;
        } else {
            if (this.database.getId() != database.getId()) {
                throw new AnalysisException(
                        "Transaction insert must be in the same database, expect db_id=" + this.database.getId());
            }
            // for delete type, make sure there is no insert for the same table
            if (subTransactionType == SubTransactionType.DELETE && subTransactionStates.stream()
                    .anyMatch(s -> s.getTable().getId() == table.getId()
                            && s.getSubTransactionType() == SubTransactionType.INSERT)) {
                throw new AnalysisException("Can not delete because there is a insert operation for the same table");
            }
            long subTxnId;
            if (Config.isCloudMode()) {
                TUniqueId queryId = ConnectContext.get().queryId();
                String label = String.format("tl_%x_%x", queryId.hi, queryId.lo);
                Set<Long> tableIds = getTableIds();
                tableIds.add(table.getId());
                Pair<Long, TransactionState> pair
                        = ((CloudGlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr()).beginSubTxn(
                        transactionId, table.getDatabase().getId(), tableIds, label, allSubTxnNum);
                this.transactionState = pair.second;
                subTxnId = pair.first;
            } else {
                subTxnId = Env.getCurrentGlobalTransactionMgr().getNextTransactionId();
                this.transactionState.addTableId(table.getId());
            }
            Env.getCurrentGlobalTransactionMgr().addSubTransaction(database.getId(), transactionId, subTxnId);
            allSubTxnNum++;
            return subTxnId;
        }
    }

    public TransactionStatus commitTransaction() throws Exception {
        if (isTransactionBegan) {
            try {
                // cloud mode does not commit on observer fe because CloudGlobalTransactionMgr#afterCommitTxnResp
                // will produce event
                if (Env.getCurrentEnv().isMaster()) {
                    beforeFinishTransaction();
                    // the report_tablet_interval_seconds is default 60
                    long commitTimeout = Math.min(Config.commit_timeout_second * 1000L,
                            Math.max(timeoutTimestamp - System.currentTimeMillis(), 0));
                    if (Env.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(database, transactionId,
                            subTransactionStates, commitTimeout)) {
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
            if (Env.getCurrentEnv().isMaster() || Config.isCloudMode()) {
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

    private void beforeFinishTransaction() throws DdlException {
        if (isTransactionBegan) {
            if (Config.isCloudMode()) {
                // null if this is observer
                if (transactionState == null) {
                    database = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
                    transactionState = Env.getCurrentGlobalTransactionMgr().getTransactionState(dbId, transactionId);
                }
            } else {
                List<Long> tableIds = transactionState.getTableIdList().stream().distinct()
                        .collect(Collectors.toList());
                transactionState.setTableIdList(tableIds);
            }
            LOG.info("subTransactionStates={}", subTransactionStates);
            transactionState.setSubTxnIds(subTransactionStates.stream().map(SubTransactionState::getSubTransactionId)
                    .collect(Collectors.toList()));
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
            if (Config.isCloudMode()) {
                try {
                    Set<Long> tableIds = getTableIds();
                    this.transactionState
                            = ((CloudGlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr()).abortSubTxn(
                            transactionId, subTransactionId, table.getDatabase().getId(), tableIds, allSubTxnNum);
                } catch (UserException e) {
                    LOG.error("Failed to remove table_id={} from txn_id={}", table.getId(), transactionId, e);
                }
            } else {
                this.transactionState.removeTableId(table.getId());
            }
            Env.getCurrentGlobalTransactionMgr().removeSubTransaction(table.getDatabase().getId(), subTransactionId);
        }
    }

    public void addTabletCommitInfos(long subTxnId, Table table, List<TTabletCommitInfo> commitInfos,
            SubTransactionType subTransactionType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("label={}, txn_id={}, sub_txn_id={}, table={}, commit_infos={}",
                    label, transactionId, subTxnId, table, commitInfos);
        }
        subTransactionStates.add(new SubTransactionState(subTxnId, table, commitInfos, subTransactionType));
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
            Preconditions.checkState(subTransactionStates.isEmpty(),
                    "subTxnStates is not empty: " + subTransactionStates);
            resetByTxnInfo(txnLoadInfo);
            this.transactionState = Env.getCurrentGlobalTransactionMgr().getTransactionState(dbId, transactionId);
            Preconditions.checkNotNull(this.transactionState,
                    "db_id" + dbId + " txn_id=" + transactionId + " not found");
            Preconditions.checkState(this.label.equals(this.transactionState.getLabel()), "expected label="
                    + this.label + ", real label=" + this.transactionState.getLabel());
            this.isTransactionBegan = true;
        }
        LOG.info("set txn info in master, label={}, txnId={}, dbId={}, timeoutTimestamp={}, allSubTxnNum={}, "
                + "subTxnStates={}", label, transactionId, dbId, timeoutTimestamp, allSubTxnNum, subTransactionStates);
    }

    public TTxnLoadInfo getTxnInfoInMaster() {
        TTxnLoadInfo txnLoadInfo = getTxnLoadInfo();
        LOG.info("master return txn info: {}", txnLoadInfo);
        return txnLoadInfo;
    }

    public TTxnLoadInfo getTxnLoadInfoInObserver() throws AnalysisException {
        if (isInsertValuesTxnBegan()) {
            throw new AnalysisException(
                    "Transaction insert can not insert into values and insert into select at the same time");
        }
        TTxnLoadInfo txnLoadInfo = getTxnLoadInfo();
        LOG.info("get txn load info in observer: {}", txnLoadInfo);
        return txnLoadInfo;
    }

    public void setTxnLoadInfoInObserver(TTxnLoadInfo txnLoadInfo) throws DdlException {
        Preconditions.checkState(txnLoadInfo.getLabel().equals(this.label),
                "expected label=" + this.label + ", real label=" + txnLoadInfo.getLabel());
        subTransactionStates.clear();
        resetByTxnInfo(txnLoadInfo);
        this.isTransactionBegan = true;
        LOG.info("set txn load info in observer, label={}, txnId={}, dbId={}, timeoutTimestamp={}, allSubTxnNum={}, "
                + "subTxnStates={}", label, transactionId, dbId, timeoutTimestamp, allSubTxnNum, subTransactionStates);
    }

    private void resetByTxnInfo(TTxnLoadInfo txnLoadInfo) throws DdlException {
        this.dbId = txnLoadInfo.getDbId();
        this.database = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
        this.transactionId = txnLoadInfo.getTxnId();
        this.timeoutTimestamp = txnLoadInfo.getTimeoutTimestamp();
        this.allSubTxnNum = txnLoadInfo.getAllSubTxnNum();
        for (TSubTxnInfo subTxnInfo : txnLoadInfo.getSubTxnInfos()) {
            TableIf table = database.getTableOrDdlException(subTxnInfo.getTableId());
            subTransactionStates.add(
                    new SubTransactionState(subTxnInfo.getSubTxnId(), (Table) table,
                            subTxnInfo.getTabletCommitInfos(),
                            SubTransactionState.getSubTransactionType(subTxnInfo.getSubTxnType())));
        }
    }

    private TTxnLoadInfo getTxnLoadInfo() {
        TTxnLoadInfo txnLoadInfo = new TTxnLoadInfo();
        txnLoadInfo.setLabel(label);
        if (this.isTransactionBegan) {
            txnLoadInfo.setTxnId(transactionId);
            txnLoadInfo.setDbId(dbId);
            txnLoadInfo.setTimeoutTimestamp(timeoutTimestamp);
            txnLoadInfo.setAllSubTxnNum(allSubTxnNum);
            for (SubTransactionState subTxnState : subTransactionStates) {
                txnLoadInfo.addToSubTxnInfos(new TSubTxnInfo()
                        .setSubTxnId(subTxnState.getSubTransactionId())
                        .setTableId(subTxnState.getTable().getId())
                        .setTabletCommitInfos(subTxnState.getTabletCommitInfos())
                        .setSubTxnType(SubTransactionState.getSubTransactionType(subTxnState.getSubTransactionType())));
            }
        }
        return txnLoadInfo;
    }

    private Set<Long> getTableIds() {
        return subTransactionStates.stream().map(s -> s.getTable().getId()).collect(Collectors.toSet());
    }
}
