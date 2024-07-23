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
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.TransactionType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Insert executor for iceberg table
 */
public class IcebergInsertExecutor extends BaseExternalTableInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(IcebergInsertExecutor.class);

    /**
     * constructor
     */
    public IcebergInsertExecutor(ConnectContext ctx, IcebergExternalTable table,
            String labelName, NereidsPlanner planner,
            Optional<InsertCommandContext> insertCtx,
            boolean emptyInsert) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert);
    }

    @Override
    public void setCollectCommitInfoFunc() {
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
        coordinator.setIcebergCommitDataFunc(transaction::updateIcebergCommitData);
    }

    @Override
    protected void beforeExec() {
        String dbName = ((IcebergExternalTable) table).getDbName();
        String tbName = table.getName();
        SimpleTableInfo tableInfo = new SimpleTableInfo(dbName, tbName);
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
        transaction.beginInsert(tableInfo);
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        String dbName = ((IcebergExternalTable) table).getDbName();
        String tbName = table.getName();
        SimpleTableInfo tableInfo = new SimpleTableInfo(dbName, tbName);
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
        this.loadedRows = transaction.getUpdateCnt();
        transaction.finishInsert(tableInfo, insertCtx);
    }

    @Override
    protected TransactionType transactionType() {
        return TransactionType.ICEBERG;
    }

}
