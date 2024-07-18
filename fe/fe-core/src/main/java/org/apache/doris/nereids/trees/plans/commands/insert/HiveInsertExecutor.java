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

import org.apache.doris.common.UserException;
import org.apache.doris.common.info.SimpleTableInfo;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSTransaction;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionType;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Insert executor for hive table
 */
public class HiveInsertExecutor extends BaseExternalTableInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(HiveInsertExecutor.class);

    /**
     * constructor
     */
    public HiveInsertExecutor(ConnectContext ctx, HMSExternalTable table,
            String labelName, NereidsPlanner planner,
            Optional<InsertCommandContext> insertCtx, boolean emptyInsert) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert);
    }

    @Override
    public void setCollectCommitInfoFunc() {
        HMSTransaction transaction = (HMSTransaction) transactionManager.getTransaction(txnId);
        coordinator.setHivePartitionUpdateFunc(transaction::updateHivePartitionUpdates);
    }

    @Override
    protected void beforeExec() {
        // check params
        HMSTransaction transaction = (HMSTransaction) transactionManager.getTransaction(txnId);
        Preconditions.checkArgument(insertCtx.isPresent(), "insert context must be present");
        HiveInsertCommandContext ctx = (HiveInsertCommandContext) insertCtx.get();
        TUniqueId tUniqueId = ConnectContext.get().queryId();
        Preconditions.checkArgument(tUniqueId != null, "query id shouldn't be null");
        ctx.setQueryId(DebugUtil.printId(tUniqueId));
        transaction.beginInsertTable(ctx);
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        HMSTransaction transaction = (HMSTransaction) transactionManager.getTransaction(txnId);
        loadedRows = transaction.getUpdateCnt();
        String dbName = ((HMSExternalTable) table).getDbName();
        String tbName = table.getName();
        transaction.finishInsertTable(new SimpleTableInfo(dbName, tbName));
    }

    @Override
    protected TransactionType transactionType() {
        return TransactionType.HMS;
    }
}
