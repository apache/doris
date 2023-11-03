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

package org.apache.doris.nereids.jobs.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.FailMsg;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.nereids.txn.Transaction;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.io.DataOutput;
import java.io.IOException;

/**
 * load task
 */
public class InsertLoadTask implements Writable {

    protected long taskId;
    protected String labelName;
    protected Transaction txn;
    protected InsertIntoTableCommand insertInto;
    protected LoadTaskState loadState;
    protected StmtExecutor executor;
    protected ConnectContext ctx;

    /**
     * insert into task
     * @param ctx ctx
     * @param executor stmt executor for insert into command
     * @param labelName label name
     * @param loadState load state
     * @param logicalPlan sql plan
     */
    public InsertLoadTask(ConnectContext ctx, StmtExecutor executor, String labelName,
                          LoadTaskState loadState, InsertIntoTableCommand logicalPlan) {
        this.ctx = ctx;
        this.executor = executor;
        this.loadState = loadState;
        this.labelName = labelName;
        this.taskId = Env.getCurrentEnv().getNextId();
        this.insertInto = logicalPlan;
    }

    public long getId() {
        return taskId;
    }

    public void prepare() {
        // loadState is PENDING
        loadState.onTaskPending();
    }

    public void execute(StmtExecutor executor, ConnectContext ctx) throws Exception {
        // loadState is LOADING
        loadState.onTaskRunning();
        insertInto.statefulRun(ctx, executor);
    }

    public void finished(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        loadState.onTaskFinished();
    }

    public void failed(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        // loadState is FAILED
    }

    public void abort(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        // loadState is CANCELLED
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }
}
