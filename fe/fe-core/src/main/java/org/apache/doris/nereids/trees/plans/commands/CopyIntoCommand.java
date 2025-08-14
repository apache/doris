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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.load.CloudLoadManager;
import org.apache.doris.cloud.load.CopyJob;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.CopyIntoInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * copy into command
 */
public class CopyIntoCommand extends Command implements ForwardWithSync {

    private CopyIntoInfo copyIntoInfo;

    /**
     * Use for copy into command.
     */
    public CopyIntoCommand(CopyIntoInfo info) {
        super(PlanType.COPY_INTO_COMMAND);
        this.copyIntoInfo = info;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        copyIntoInfo.validate(ctx);
        copyIntoInfo.setOriginStmt(executor.getOriginStmt());

        execute(Env.getCurrentEnv(), this);

        // copy into used
        if (executor.getContext().getState().getResultSet() != null) {
            if (executor.isProxy()) {
                executor.setProxyShowResultSet(executor.getContext().getState().getResultSet());
                return;
            }
            executor.sendResultSet(executor.getContext().getState().getResultSet());
        }
    }

    public CopyIntoInfo getCopyIntoInfo() {
        return copyIntoInfo;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCopyIntoCommand(this, context);
    }

    /**
     * execute
     */
    private static void execute(Env env, CopyIntoCommand command) throws Exception {
        CopyJob job = (CopyJob) (((CloudLoadManager) env.getLoadManager()).createLoadJobFromCopyIntoCommand(command));
        if (!command.getCopyIntoInfo().isAsync()) {
            // wait for execute finished
            waitJobCompleted(job);
            if (job.getState() == JobState.UNKNOWN || job.getState() == JobState.CANCELLED) {
                QueryState queryState = new QueryState();
                FailMsg failMsg = job.getFailMsg();
                EtlStatus loadingStatus = job.getLoadingStatus();
                List<List<String>> result = Lists.newArrayList();
                List<String> entry = Lists.newArrayList();
                entry.add(job.getCopyId());
                entry.add(job.getState().toString());
                entry.add(failMsg == null ? "" : failMsg.getCancelType().toString());
                entry.add(failMsg == null ? "" : failMsg.getMsg());
                entry.add("");
                entry.add("");
                entry.add("");
                entry.add(loadingStatus.getTrackingUrl());
                result.add(entry);
                queryState.setResultSet(new ShowResultSet(command.getCopyIntoInfo().getMetaData(), result));
                ConnectContext.get().setState(queryState);
                return;
            } else if (job.getState() == JobState.FINISHED) {
                EtlStatus loadingStatus = job.getLoadingStatus();
                Map<String, String> counters = loadingStatus.getCounters();
                QueryState queryState = new QueryState();
                List<List<String>> result = Lists.newArrayList();
                List<String> entry = Lists.newArrayList();
                entry.add(job.getCopyId());
                entry.add(job.getState().toString());
                entry.add("");
                entry.add("");
                entry.add(counters.getOrDefault(LoadJob.DPP_NORMAL_ALL, "0"));
                entry.add(counters.getOrDefault(LoadJob.DPP_ABNORMAL_ALL, "0"));
                entry.add(counters.getOrDefault(LoadJob.UNSELECTED_ROWS, "0"));
                entry.add(loadingStatus.getTrackingUrl());
                result.add(entry);
                queryState.setResultSet(new ShowResultSet(command.getCopyIntoInfo().getMetaData(), result));
                ConnectContext.get().setState(queryState);
                return;
            }
        }
        QueryState queryState = new QueryState();
        List<List<String>> result = Lists.newArrayList();
        List<String> entry = Lists.newArrayList();
        entry.add(job.getCopyId());
        entry.add(job.getState().toString());
        entry.add("");
        entry.add("");
        entry.add("");
        entry.add("");
        entry.add("");
        entry.add("");
        result.add(entry);
        queryState.setResultSet(new ShowResultSet(command.getCopyIntoInfo().getMetaData(), result));
        ConnectContext.get().setState(queryState);
    }

    private static void waitJobCompleted(CopyJob job) throws InterruptedException {
        // check the job is completed or not.
        // sleep 10ms, 1000 times(10s)
        // sleep 100ms, 1000 times(100s + 10s = 110s)
        // sleep 1000ms, 1000 times(1000s + 110s = 1110s)
        // sleep 5000ms...
        long retry = 0;
        long currentInterval = 10;
        while (!job.isCompleted()) {
            Thread.sleep(currentInterval);
            if (retry > 3010) {
                continue;
            }
            retry++;
            if (retry > 3000) {
                currentInterval = 5000;
            } else if (retry > 2000) {
                currentInterval = 1000;
            } else if (retry > 1000) {
                currentInterval = 100;
            }
        }
    }

}
