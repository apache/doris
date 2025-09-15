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
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.datasource.iceberg.rewrite.RewriteFileInfo;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.TransactionType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Rewrite executor for iceberg table data file rewrite operations.
 *
 * This executor is specifically designed for rewrite operations and uses
 * rewrite-specific transaction logic instead of insert transaction logic.
 */
public class IcebergRewriteExecutor extends BaseExternalTableInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(IcebergRewriteExecutor.class);

    /**
     * constructor
     */
    public IcebergRewriteExecutor(ConnectContext ctx, IcebergExternalTable table,
            String labelName, NereidsPlanner planner,
            Optional<InsertCommandContext> insertCtx,
            boolean emptyInsert) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert);
    }

    @Override
    protected void beforeExec() throws UserException {
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
        transaction.beginRewrite((ExternalTable) table);
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
        this.loadedRows = transaction.getUpdateCnt();
        transaction.finishRewrite();
    }

    @Override
    protected TransactionType transactionType() {
        return TransactionType.ICEBERG;
    }

    /**
     * Get rewrite file information from the associated IcebergTransaction
     *
     * @return RewriteFileInfo containing file statistics
     * @throws UserException if transaction is not available
     */
    public RewriteFileInfo getRewriteFileInfo() throws UserException {
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
        return transaction.getRewriteFileInfo();
    }
}
