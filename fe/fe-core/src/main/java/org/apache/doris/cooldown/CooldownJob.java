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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushCooldownConfTask;
import org.apache.doris.thrift.TCooldownType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class CooldownJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(CooldownJob.class);

    public enum JobState {
        PENDING, // Job is created
        SEND_CONF, // send cooldown task to BE.
        RUNNING, // cooldown tasks are sent to BE, and waiting for them finished.
        FINISHED, // job is done
        CANCELLED; // job is cancelled(failed or be cancelled by user)

        public boolean isFinalState() {
            return this == CooldownJob.JobState.FINISHED || this == CooldownJob.JobState.CANCELLED;
        }
    }

    @SerializedName(value = "jobId")
    protected long jobId;
    @SerializedName(value = "jobState")
    protected CooldownJob.JobState jobState;
    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "tableId")
    protected long tableId;
    @SerializedName(value = "partitionId")
    protected long partitionId;
    @SerializedName(value = "indexId")
    protected long indexId;
    @SerializedName(value = "tabletId")
    protected long tabletId;
    @SerializedName(value = "replicaId")
    protected long replicaId;
    @SerializedName(value = "backendId")
    protected long backendId;
    @SerializedName(value = "cooldownType")
    TCooldownType cooldownType;

    @SerializedName(value = "errMsg")
    protected String errMsg = "";
    @SerializedName(value = "createTimeMs")
    protected long createTimeMs = -1;
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs = -1;
    @SerializedName(value = "timeoutMs")
    protected long timeoutMs = -1;

    private AgentBatchTask cooldownBatchTask = new AgentBatchTask();

    public long getJobId() {
        return jobId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public CooldownJob(long jobId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                       long replicaId, long backendId, TCooldownType cooldownType, long timeoutMs) {
        this.jobId = jobId;
        this.jobState = JobState.PENDING;
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.replicaId = replicaId;
        this.backendId = backendId;
        this.cooldownType = cooldownType;
        this.createTimeMs = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
    }

    protected void runPendingJob() throws CooldownException {
        Preconditions.checkState(jobState == CooldownJob.JobState.PENDING, jobState);
        this.jobState = JobState.SEND_CONF;
        // write edit log
        setCooldownType(cooldownType);
        Env.getCurrentEnv().getEditLog().logCooldownJob(this);
        LOG.info("send cooldown job {} state to {}", jobId, this.jobState);
    }

    protected void runSendJob() throws CooldownException {
        Preconditions.checkState(jobState == JobState.SEND_CONF, jobState);
        LOG.info("begin to send cooldown conf tasks. job: {}", jobId);
        if (!FeConstants.runningUnitTest) {
            AgentBatchTask batchTask = new AgentBatchTask();
            PushCooldownConfTask pushCooldownConfTask = new PushCooldownConfTask(backendId, dbId, tableId, partitionId,
                    indexId, tabletId, replicaId, cooldownType);
            batchTask.addTask(pushCooldownConfTask);

            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
        }

        this.jobState = JobState.RUNNING;
        // write edit log
        Env.getCurrentEnv().getEditLog().logCooldownJob(this);
        LOG.info("send cooldown job {} state to {}", jobId, this.jobState);
    }

    protected void runRunningJob() throws CooldownException {
        if (!cooldownBatchTask.isFinished()) {
            LOG.info("cooldown tasks not finished. job: {}", jobId);
            List<AgentTask> tasks = cooldownBatchTask.getUnfinishedTasks(2000);
            for (AgentTask task : tasks) {
                if (task.getFailedTimes() >= 3) {
                    task.setFinished(true);
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUSH_COOLDOWN_CONF, task.getSignature());
                    LOG.warn("push cooldown conf task failed after try three times: " + task.getErrorMsg());
                    throw new CooldownException("cooldown tasks failed on same tablet: " + tabletId);
                }
            }
            return;
        }
        this.jobState = CooldownJob.JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        Env.getCurrentEnv().getEditLog().logCooldownJob(this);
        LOG.info("push cooldown conf job finished: {}", jobId);
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
        LOG.info("cancel cooldown job {}, err: {}", jobId, errMsg);
        Env.getCurrentEnv().getEditLog().logCooldownJob(this);
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
                case PENDING:
                    runPendingJob();
                    break;
                case SEND_CONF:
                    runSendJob();
                    break;
                case RUNNING:
                    runRunningJob();
                    break;
                default:
                    break;
            }
        } catch (CooldownException e) {
            cancelImpl(e.getMessage());
        }
    }

    public void replay(CooldownJob replayedJob) {
        try {
            switch (replayedJob.jobState) {
                case PENDING:
                    replayCreateJob(replayedJob);
                    break;
                case SEND_CONF:
                    replayPengingJob();
                    break;
                case FINISHED:
                    replayRunningJob(replayedJob);
                    break;
                case CANCELLED:
                    replayCancelled(replayedJob);
                    break;
                default:
                    break;
            }
        } catch (CooldownException e) {
            LOG.warn("[INCONSISTENT META] replay cooldown job failed {}", replayedJob.jobId, e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static CooldownJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CooldownJob.class);
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     * These changes should be same as changes in CooldownHandler.createJob()
     */
    private void replayCreateJob(CooldownJob replayedJob) {
        jobId = replayedJob.jobId;
        dbId = replayedJob.dbId;
        tableId = replayedJob.tableId;
        partitionId = replayedJob.partitionId;
        indexId = replayedJob.indexId;
        tabletId = replayedJob.tabletId;
        replicaId = replayedJob.replicaId;
        cooldownType = replayedJob.cooldownType;
        createTimeMs = replayedJob.createTimeMs;
        timeoutMs = replayedJob.timeoutMs;
        jobState = JobState.PENDING;
        LOG.info("replay create cooldown job: {}, table id: {}, tablet id : {}", jobId, tableId, tabletId);
    }

    /**
     * Replay job in PENDING state. set cooldown type in Replica
     */
    private void replayPengingJob() throws CooldownException {
        setCooldownType(cooldownType);
        jobState = JobState.SEND_CONF;
        LOG.info("replay send cooldown conf, job: {}", jobId);
    }

    /**
     * Replay job in FINISHED state.
     * Should replay all changes in runRunningJob()
     */
    private void replayRunningJob(CooldownJob replayedJob) throws CooldownException {
        jobState = CooldownJob.JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        LOG.info("replay finished cooldown job: {}", jobId);
    }

    private void setCooldownType(TCooldownType cooldownType) throws CooldownException {
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new CooldownException("Database " + s + " does not exist"));
        OlapTable tbl;
        try {
            tbl = (OlapTable) db.getTableOrMetaException(tableId, TableIf.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new CooldownException(e.getMessage());
        }
        if (tbl != null) {
            tbl.writeLock();
            try {
                Partition partition = tbl.getPartition(partitionId);
                if (partition != null) {
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index != null) {
                        Tablet tablet = index.getTablet(tabletId);
                        if (tablet != null) {
                            Replica replica = tablet.getReplicaById(replicaId);
                            replica.setCooldownType(cooldownType);
                            LOG.info("setCooldownType to {} when cancel job: {}:{}", cooldownType.name(),
                                    tbl.getId(), jobId);
                            return;
                        }
                    }
                }
                throw new CooldownException("set cooldown type failed.");
            } finally {
                tbl.writeUnlock();
            }
        }
    }

    private void cancelInternal() {
        // clear tasks if has
        AgentTaskQueue.removeBatchTask(cooldownBatchTask, TTaskType.PUSH_COOLDOWN_CONF);
        jobState = CooldownJob.JobState.CANCELLED;
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(CooldownJob replayedJob) {
        cancelInternal();
        this.jobState = CooldownJob.JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        this.errMsg = replayedJob.errMsg;
        LOG.info("replay cancelled cooldown job: {}", jobId);
    }

}
