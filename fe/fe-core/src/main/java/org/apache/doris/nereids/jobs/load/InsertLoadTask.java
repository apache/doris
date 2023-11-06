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
import org.apache.doris.job.exception.JobException;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * load task
 */
public class InsertLoadTask extends StatefulLoadTask {

    protected long taskId;
    protected String labelName;
    protected InsertIntoTableCommand insertInto;
    protected LoadStatistic statistic;
    protected FailMsg failMsg;
    protected InsertIntoState insertState;

    /**
     * insert into task
     *
     * @param labelName            label name
     * @param logicalPlan          sql plan
     * @param loadStatistic        load statistic
     */
    public InsertLoadTask(String labelName, InsertIntoTableCommand logicalPlan,
                          LoadStatistic loadStatistic) {
        super(TaskType.PENDING);
        this.labelName = labelName;
        this.taskId = Env.getCurrentEnv().getNextId();
        this.insertInto = logicalPlan;
        this.statistic = loadStatistic;
    }

    public long getId() {
        return taskId;
    }

    public void run(StmtExecutor executor, ConnectContext ctx) throws JobException {
        try {
            insertInto.statefulRun(ctx, executor);
        } catch (Exception e) {
            throw new JobException(e);
        }
    }

    public void onFinished() {
        // check insertState
        // callback.update();
    }

    public void onFailed() {
        // check insertState
        // callback.update();
    }
}
