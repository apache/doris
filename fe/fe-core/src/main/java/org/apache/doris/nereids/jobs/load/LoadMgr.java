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
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.load.DataTransFormMgr;
import org.apache.doris.nereids.jobs.load.replay.ReplayLoadOperation;
import org.apache.doris.scheduler.executor.LoadJobExecutor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * load manager
 */
public class LoadMgr extends DataTransFormMgr {
    private Map<Long, LoadJobExecutor> loadIdToJob = Maps.newHashMap(); // exportJobId to exportJob
    private Map<String, Long> labelToLoadJobId = Maps.newHashMap();

    public List<LoadJobExecutor> getExecutableJobs() {
        return Lists.newArrayList(loadIdToJob.values());
    }

    /**
     * add load job and add tasks
     * @param loadJob job
     */
    public void addLoadJob(LoadJobExecutor loadJob) {
        writeLock();
        try {
            if (labelToLoadJobId.containsKey(loadJob.getLabel())) {
                throw new LabelAlreadyUsedException(loadJob.getLabel());
            }
            unprotectAddJob(loadJob);
            loadJob.getTaskExecutors().forEach(executor -> {
                Long taskId = Env.getCurrentEnv().getTransientTaskManager().addMemoryTask(executor);
                loadJob.getTaskIdToExecutor().put(taskId, executor);
            });
            Env.getCurrentEnv().getEditLog().logLoadCreate(loadJob);
        } catch (LabelAlreadyUsedException e) {
            throw new RuntimeException(e);
        } finally {
            writeUnlock();
        }
    }

    private void unprotectAddJob(LoadJobExecutor job) {
        loadIdToJob.put(job.getId(), job);
        labelToLoadJobId.putIfAbsent(job.getLabel(), job.getId());
    }

    public void replayLoadTask(ReplayLoadOperation replayLoadOperation) {

    }

    public void waitLoadJobState() {


    }

    public void recordFinishedLoadJob() {

    }

    public void cancelLoadJob(LoadJobExecutor loadJob) {

    }

    /**
     * get running job
     *
     * @param jobId id
     * @return running job
     */
    public LoadJobExecutor getJob(long jobId) {
        LoadJobExecutor job;
        readLock();
        try {
            job = loadIdToJob.get(jobId);
        } finally {
            readUnlock();
        }
        return job;
    }
}
