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

package org.apache.doris.scheduler.executor;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.nereids.jobs.load.BulkLoadTaskState;
import org.apache.doris.nereids.jobs.load.LoadTaskState;
import org.apache.doris.nereids.jobs.load.InsertLoadTask;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.job.ExecutorResult;
import org.apache.doris.scheduler.job.JobTask;

import lombok.extern.slf4j.Slf4j;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * we use this executor to execute sql job
 *
 */
@Slf4j
public class LoadJobExecutor implements Writable {

    private final Long jobId;
    protected String labelName;
    protected JobState state;
    protected List<TransientTaskExecutor> jobTasks = new ArrayList<>();
    private ConcurrentHashMap<Long, TransientTaskExecutor> taskIdToExecutor = new ConcurrentHashMap<>();

    /**
     * load job type
     */
    public enum LoadType {
        BULK,
        SPARK,
        LOCAL_FILE,
        UNKNOWN;
    }

    public static class LoadStatistic {

        // number of file to be loaded

        public int fileNum = 0;
        public long totalFileSizeB = 0;
        // init the statistic of specified load task
    }

    public LoadJobExecutor(ConnectContext ctx, StmtExecutor executor, String labelName,
                           List<InsertIntoTableCommand> plans) {
        this.jobId = Env.getCurrentEnv().getNextId();
        this.labelName = labelName;
        this.state = JobState.PENDING;
        for (InsertIntoTableCommand logicalPlan : plans) {
            BulkLoadTaskState state = new BulkLoadTaskState(LoadTaskState.TaskType.PENDING);
            InsertLoadTask task = new InsertLoadTask(ctx, executor, labelName, state, logicalPlan);
            LoadTaskExecutor jobTask = new LoadTaskExecutor(ctx, task);
            taskIdToExecutor.put(task.getId(), jobTask);
        }
    }

    public Long getId() {
        return jobId;
    }

    public String getLabel() {
        return labelName;
    }

    public List<? extends TransientTaskExecutor> getTaskExecutors() {
        return jobTasks;
    }

    public ConcurrentHashMap<Long, TransientTaskExecutor> getTaskIdToExecutor() {
        return taskIdToExecutor;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }
}
