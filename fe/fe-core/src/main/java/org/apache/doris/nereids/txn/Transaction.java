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

package org.apache.doris.nereids.txn;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoSelectCommand;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.InsertStreamTxnExecutor;
import org.apache.doris.qe.MasterTxnExecutor;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * transaction wrapper for Nereids
 */
public class Transaction {
    private final String dbName;
    private final String tableName;
    private final ConnectContext ctx;
    private final StmtExecutor executor;
    private long createAt;
    private long txnId;
    private String labelName;
    private long loadedRows = 0;
    private int filteredRows = 0;
    private TransactionStatus txnStatus = TransactionStatus.ABORTED;
    private String errMsg = "";

    /**
     * constructor
     */
    public Transaction(ConnectContext ctx, String dbName, String tableName, StmtExecutor executor)
            throws TException {
        check();
        this.ctx = ctx;
        this.dbName = dbName;
        this.tableName = tableName;
        this.executor = executor;
    }

    private void beginTxn() throws TException, UserException, ExecutionException, InterruptedException,
            TimeoutException {
        TransactionEntry txnEntry = ctx.getTxnEntry();
        TTxnParams txnConf = txnEntry.getTxnConf();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        long timeoutSecond = ctx.getExecTimeout();

        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;
        Database dbObj = Env.getCurrentInternalCatalog()
                .getDbOrException(dbName, s -> new TException("database is invalid for dbName: " + s));
        Table tblObj = dbObj.getTableOrException(tableName, s -> new TException("table is invalid: " + s));

        txnConf.setDbId(dbObj.getId()).setTbl(tableName).setDb(dbName);
        txnEntry.setTable(tblObj);
        txnEntry.setDb(dbObj);

        if (Env.getCurrentEnv().isMaster()) {
            txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                    txnConf.getDbId(), Lists.newArrayList(tblObj.getId()),
                    labelName, new TransactionState.TxnCoordinator(
                            TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                    sourceType, timeoutSecond);
            txnConf.setTxnId(txnId);
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            txnConf.setToken(token);
        } else {
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(ctx);
            TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();

            request.setDb(txnConf.getDb())
                    .setTbl(txnConf.getTbl())
                    .setToken(token)
                    .setCluster(dbObj.getClusterName())
                    .setLabel(labelName)
                    .setUser("")
                    .setUserIp("")
                    .setPasswd("");

            TLoadTxnBeginResult result = masterTxnExecutor.beginTxn(request);
            txnConf.setTxnId(result.getTxnId());
            txnConf.setToken(token);
        }

        TStreamLoadPutRequest request = new TStreamLoadPutRequest();

        long maxExecMemByte = sessionVariable.getMaxExecMemByte();
        String timeZone = sessionVariable.getTimeZone();
        int sendBatchParallelism = sessionVariable.getSendBatchParallelism();

        request.setTxnId(txnConf.getTxnId()).setDb(txnConf.getDb())
                .setTbl(txnConf.getTbl())
                .setFileType(TFileType.FILE_STREAM)
                .setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND)
                .setThriftRpcTimeoutMs(5000)
                .setLoadId(ctx.queryId())
                .setExecMemLimit(maxExecMemByte)
                .setTimeout((int) timeoutSecond)
                .setTimezone(timeZone)
                .setSendBatchParallelism(sendBatchParallelism);

        // execute begin txn
        InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
        executor.beginTransaction(request);
    }

    /**
     * execute insert txn for insert into select command.
     */
    public void executeInsertIntoSelectCommand(InsertIntoSelectCommand command)
            throws TException, UserException, ExecutionException, InterruptedException, TimeoutException {
        if (ctx.isTxnModel()) {
            PhysicalPlan physicalPlan = command.getPhysicalQuery();
            Preconditions.checkArgument(physicalPlan instanceof PhysicalOneRowRelation,
                    "insert into select in txn model is not supported");
            // List<NamedExpression> values = ((PhysicalOneRowRelation) physicalPlan).getProjects();
            beginTxn();
        }
        if (!(command.getLogicalQuery() instanceof PhysicalPlan)) {
            throw new TException("should be a physical plan in insert into select command");
        }
    }

    /**
     * execute insert txn for insert into select literal list command and insert into values command.
     */
    public void executeInsertIntoValuesCommand(List<List<Expr>> values)
            throws TException, UserException, ExecutionException, InterruptedException, TimeoutException {
        // TODO
        // TransactionStatus status = TransactionStatus.PREPARE;
        if (ctx.isTxnIniting()) {
            beginTxn();
        }
        TransactionEntry entry = ctx.getTxnEntry();
        int schemaSize = entry.getTable().getBaseSchema().size();
        for (List<Expr> valueList : values) {
            if (schemaSize != valueList.size()) {
                throw new TException(String.format("Column count: %d doesn't match value count: %d",
                        schemaSize, valueList.size()));
            }
        }
    }

    private void check() throws TException {
        TTxnParams txnConf = ctx.getTxnEntry().getTxnConf();
        if (!txnConf.getDb().equals(dbName) || !txnConf.getTbl().equals(tableName)) {
            throw new TException("only one table in one transaction");
        }
    }
}
