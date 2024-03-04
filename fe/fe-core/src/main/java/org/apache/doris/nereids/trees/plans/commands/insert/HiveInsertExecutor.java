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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.HiveTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.TransactionState;

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

    /**
     * constructor
     */
    public HiveInsertExecutor(ConnectContext ctx, HMSExternalTable table,
                              String labelName, NereidsPlanner planner,
                              Optional<InsertCommandContext> insertCtx) {
        super(ctx, table, labelName, planner, insertCtx);
    }

    public long getTxnId() {
        return txnId;
    }

    @Override
    public void beginTransaction() {

    }

    @Override
    protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        HiveTableSink hiveTableSink = (HiveTableSink) sink;
        // PhysicalHiveTableSink physicalHiveTableSink = (PhysicalHiveTableSink) physicalSink;
        try {
            hiveTableSink.init();
            hiveTableSink.complete(new Analyzer(Env.getCurrentEnv(), ctx));
            TransactionState state = Env.getCurrentGlobalTransactionMgr().getTransactionState(database.getId(), txnId);
            if (state == null) {
                throw new AnalysisException("txn does not exist: " + txnId);
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    protected void beforeExec() {

    }

    @Override
    protected void onComplete() throws UserException {

    }

    @Override
    protected void onFail(Throwable t) {

    }

    @Override
    protected void afterExec(StmtExecutor executor) {

    }
}
