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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.InsertStreamTxnExecutor;
import org.apache.doris.qe.MasterTxnExecutor;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class TransactionEntry {

    private static final Logger LOG = LogManager.getLogger(TransactionEntry.class);

    private String label = "";
    private DatabaseIf database;

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
    private List<Table> tableList = new ArrayList<>();
    private List<TTabletCommitInfo> tabletCommitInfos = new ArrayList<>();

    public TransactionEntry() {
    }

    public TransactionEntry(TTxnParams txnConf, Database db, Table table) {
        this.txnConf = txnConf;
        this.database = db;
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

    public boolean isTxnIniting() {
        return isTxnModel() && txnConf.getTxnId() == -1;
    }

    public boolean isTxnBegin() {
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

    // Used for insert into select
    public void beginTransaction(DatabaseIf database, TableIf table)
            throws DdlException, BeginTransactionException, MetaNotFoundException, AnalysisException,
            QuotaExceedException {
        if (isTxnBegin()) {
            // FIXME: support mix usage of `insert into values` and `insert into select`
            throw new AnalysisException(
                    "Transaction insert can not insert into values and insert into select at the same time");
        }
        if (!isTransactionBegan) {
            this.transactionId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                    database.getId(), Lists.newArrayList(table.getId()), label,
                    new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                    LoadJobSourceType.INSERT_STREAMING, ConnectContext.get().getExecTimeout());
            this.isTransactionBegan = true;
            this.database = database;
            this.transactionState = Env.getCurrentGlobalTransactionMgr()
                    .getTransactionState(database.getId(), transactionId);
        } else {
            if (this.database.getId() != database.getId()) {
                throw new AnalysisException(
                        "Transaction insert must be in the same database, expect db_id=" + this.database.getId());
            }
            this.transactionState.getTableIdList().add(table.getId());
        }
    }

    public TransactionStatus commitTransaction()
            throws Exception {
        if (isTransactionBegan) {
            if (Env.getCurrentGlobalTransactionMgr()
                    .commitAndPublishTransaction(database, tableList, transactionId,
                            TabletCommitInfo.fromThrift(tabletCommitInfos), ConnectContext.get().getExecTimeout())) {
                return TransactionStatus.VISIBLE;
            } else {
                return TransactionStatus.COMMITTED;
            }
        } else if (isTxnBegin()) {
            InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(this);
            if (dataToSend.size() > 0) {
                // send rest data
                executor.sendData();
            }
            // commit txn
            executor.commitTransaction();

            // wait txn visible
            TWaitingTxnStatusRequest request = new TWaitingTxnStatusRequest();
            request.setDbId(txnConf.getDbId()).setTxnId(txnConf.getTxnId());
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
        } else {
            LOG.info("No transaction to commit");
            return null;
        }
    }

    private TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request) throws Exception {
        TWaitingTxnStatusResult statusResult = null;
        if (Env.getCurrentEnv().isMaster()) {
            statusResult = Env.getCurrentGlobalTransactionMgr()
                    .getWaitingTxnStatus(request);
        } else {
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(ConnectContext.get());
            statusResult = masterTxnExecutor.getWaitingTxnStatus(request);
        }
        return statusResult;
    }

    public long abortTransaction()
            throws UserException, TException, ExecutionException, InterruptedException, TimeoutException {
        if (isTransactionBegan) {
            Env.getCurrentGlobalTransactionMgr().abortTransaction(database.getId(), transactionId, "user rollback");
            return transactionId;
        } else if (isTxnBegin()) {
            InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(this);
            executor.abortTransaction();
            return txnConf.getTxnId();
        } else {
            LOG.info("No transaction to abort");
            return -1;
        }
    }

    public long getTransactionId() {
        if (isTransactionBegan) {
            return transactionId;
        } else if (isTxnBegin()) {
            return txnConf.getTxnId();
        } else {
            return -1;
        }
    }

    public void addCommitInfos(Table table, List<TTabletCommitInfo> commitInfos) {
        this.tableList.add(table);
        this.tabletCommitInfos.addAll(commitInfos);
    }

    public boolean isTransactionBegan() {
        return this.isTransactionBegan;
    }
}
