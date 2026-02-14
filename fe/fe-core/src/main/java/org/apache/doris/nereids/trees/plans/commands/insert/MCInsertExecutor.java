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
import org.apache.doris.datasource.maxcompute.MCTransaction;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.TransactionType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

public class MCInsertExecutor extends BaseExternalTableInsertExecutor {

    private static final Logger LOG = LogManager.getLogger(MCInsertExecutor.class);

    public MCInsertExecutor(ConnectContext ctx, MaxComputeExternalTable table,
                            String labelName, NereidsPlanner planner,
                            Optional<InsertCommandContext> insertCtx,
                            boolean emptyInsert, long jobId) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert, jobId);
    }

    @Override
    protected void beforeExec() throws UserException {
        MCTransaction transaction = (MCTransaction) transactionManager.getTransaction(txnId);
        transaction.beginInsert((MaxComputeExternalTable) table, insertCtx);
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        MCTransaction transaction = (MCTransaction) transactionManager.getTransaction(txnId);
        this.loadedRows = transaction.getUpdateCnt();
        transaction.finishInsert();
    }

    @Override
    protected TransactionType transactionType() {
        return TransactionType.MAXCOMPUTE;
    }
}
