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

package org.apache.doris.cooldown;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Writable;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.SendCooldownDeleteTask;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class CooldownDeleteJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(CooldownDeleteJob.class);

    public enum JobState {
        SEND_CONF, // send cooldown delete task to BE.
        RUNNING, // cooldown delete tasks are sent to BE, and waiting for them finished.
        FINISHED, // job is done
        CANCELLED; // job is cancelled(failed or be cancelled by user)

        public boolean isFinalState() {
            return this == CooldownDeleteJob.JobState.FINISHED || this == CooldownDeleteJob.JobState.CANCELLED;
        }
    }

    protected long jobId;
    protected CooldownDeleteJob.JobState jobState;
    protected long beId;
    protected List<CooldownDelete> cooldownDeleteList;

    protected String errMsg = "";
    protected long createTimeMs = -1;
    protected long finishedTimeMs = -1;
    protected long timeoutMs = -1;

    public long getJobId() {
        return jobId;
    }

    public JobState getJobState() {
        return jobState;
    }

    public List<CooldownDelete> getCooldownDeleteList() {
        return cooldownDeleteList;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    private AgentBatchTask cooldownBatchTask = new AgentBatchTask();

    public CooldownDeleteJob(long jobId, long beId, List<CooldownDelete> cooldownDeleteList, long timeoutMs) {
        this.jobId = jobId;
        this.jobState = JobState.SEND_CONF;
        this.beId = beId;
        this.cooldownDeleteList = cooldownDeleteList;
        this.createTimeMs = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
    }

    protected void runSendJob() throws CooldownException {
        Preconditions.checkState(jobState == JobState.SEND_CONF, jobState);
        LOG.info("begin to send cooldown delete tasks. job: {}", jobId);
        // write edit log to check this is master.
        Env.getCurrentEnv().getEditLog().logCooldownDeleteJob(this);
        if (!FeConstants.runningUnitTest) {
            SendCooldownDeleteTask sendCooldownDeleteTask = new SendCooldownDeleteTask(beId, cooldownDeleteList);
            cooldownBatchTask.addTask(sendCooldownDeleteTask);
            AgentTaskQueue.addBatchTask(cooldownBatchTask);
            AgentTaskExecutor.submit(cooldownBatchTask);
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();
        LOG.info("send cooldown delete job {} state to {}", jobId, this.jobState);
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() - createTimeMs > timeoutMs;
    }

    public boolean isDone() {
        return jobState.isFinalState();
    }

    /*
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    protected synchronized void cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return;
        }

        cancelInternal();

        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        LOG.info("cancel cooldown delete job {}, err: {}", jobId, errMsg);
    }

    /**
     * The keyword 'synchronized' only protects 2 methods:
     * run() and cancel()
     * Only these 2 methods can be visited by different thread(internal working thread and user connection thread)
     * So using 'synchronized' to make sure only one thread can run the job at one time.
     *
     * lock order:
     *      synchronized
     *      db lock
     */
    public synchronized void run() {
        if (isTimeout()) {
            cancelImpl("Timeout");
            return;
        }

        try {
            switch (jobState) {
                case SEND_CONF:
                    runSendJob();
                    break;
                default:
                    break;
            }
        } catch (CooldownException e) {
            cancelImpl(e.getMessage());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {}

    private void cancelInternal() {
        // clear tasks if has
        AgentTaskQueue.removeBatchTask(cooldownBatchTask, TTaskType.COOLDOWN_DELETE_FILE);
        jobState = CooldownDeleteJob.JobState.CANCELLED;
    }

}
