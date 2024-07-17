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

package org.apache.doris.alter;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.AlterInvertedIndexTask;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;


public class IndexChangeJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(IndexChangeJob.class);


    public enum JobState {
        // CHECKSTYLE OFF
        WAITING_TXN, // waiting for previous txns to be finished
        // CHECKSTYLE ON
        RUNNING, // waiting for inverted index tasks finished.
        FINISHED, // job is done
        CANCELLED; // job is cancelled

        public boolean isFinalState() {
            return this == JobState.FINISHED || this == JobState.CANCELLED;
        }
    }

    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "jobState")
    private JobState jobState;

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "partitionId")
    private long partitionId;
    @SerializedName(value = "partitionName")
    private String partitionName;

    @SerializedName(value = "errMsg")
    private String errMsg = "";
    @SerializedName(value = "createTimeMs")
    private long createTimeMs = -1;
    @SerializedName(value = "finishedTimeMs")
    private long finishedTimeMs = -1;
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;

    @SerializedName(value = "isDropOp")
    private boolean isDropOp = false;
    @SerializedName(value = "alterInvertedIndexes")
    private List<Index> alterInvertedIndexes = null;
    @SerializedName(value = "originIndexId")
    private long originIndexId;
    @SerializedName(value = "invertedIndexBatchTask")
    AgentBatchTask invertedIndexBatchTask = new AgentBatchTask();
    // save failed task after retry three times, tablet -> backends
    @SerializedName(value = "failedTabletBackends")
    protected Map<Long, List<Long>> failedTabletBackends = Maps.newHashMap();
    @SerializedName(value = "timeoutMs")
    protected long timeoutMs = -1;

    public IndexChangeJob() {
        this.jobId = -1;
        this.dbId = -1;
        this.tableId = -1;
        this.tableName = "";

        this.createTimeMs = System.currentTimeMillis();
        this.jobState = JobState.WAITING_TXN;
    }

    public IndexChangeJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) throws Exception {
        this.jobId = jobId;
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;

        this.createTimeMs = System.currentTimeMillis();
        this.jobState = JobState.WAITING_TXN;
        this.watershedTxnId = Env.getCurrentGlobalTransactionMgr().getNextTransactionId();
        this.timeoutMs = timeoutMs;
    }

    public long getJobId() {
        return jobId;
    }

    public long getOriginIndexId() {
        return originIndexId;
    }

    public JobState getJobState() {
        return jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public void setPartitionId(long partitionId) {
        this.partitionId = partitionId;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public void setOriginIndexId(long originIndexId) {
        this.originIndexId = originIndexId;
    }

    public void setAlterInvertedIndexInfo(boolean isDropOp, List<Index> alterInvertedIndexes) {
        this.isDropOp = isDropOp;
        this.alterInvertedIndexes = alterInvertedIndexes;
    }

    public boolean hasSameAlterInvertedIndex(boolean isDropOp, List<Index> inputAlterInvertedIndexes) {
        if (this.isDropOp == isDropOp) {
            for (Index inputIndex : inputAlterInvertedIndexes) {
                for (Index existIndex : this.alterInvertedIndexes) {
                    if (inputIndex.getIndexId() == existIndex.getIndexId()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public boolean isExpire() {
        return isDone() && (System.currentTimeMillis() - finishedTimeMs) / 1000 > Config.history_job_keep_max_second;
    }

    public boolean isDone() {
        return jobState.isFinalState();
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public void setFinishedTimeMs(long finishedTimeMs) {
        this.finishedTimeMs = finishedTimeMs;
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() - createTimeMs > timeoutMs;
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
                case WAITING_TXN:
                    runWaitingTxnJob();
                    break;
                case RUNNING:
                    runRunningJob();
                    break;
                default:
                    break;
            }
        } catch (AlterCancelException e) {
            cancelImpl(e.getMessage());
        }
    }

    public final synchronized boolean cancel(String errMsg) {
        return cancelImpl(errMsg);
    }

    /**
     * should be called before executing the job.
     * return false if table is not stable.
     */
    protected boolean checkTableStable(OlapTable tbl) throws AlterCancelException {
        tbl.writeLockOrAlterCancelException();
        try {
            boolean isStable = tbl.isStable(Env.getCurrentSystemInfo(),
                    Env.getCurrentEnv().getTabletScheduler());

            if (!isStable) {
                errMsg = "table is unstable";
                LOG.warn("wait table {} to be stable before doing index change job", tableId);
                return false;
            } else {
                // table is stable
                LOG.info("table {} is stable, start index change job {}", tableId, jobId);
                errMsg = "";
                return true;
            }
        } finally {
            tbl.writeUnlock();
        }
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return Env.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(
                watershedTxnId, dbId, Lists.newArrayList(tableId));
    }

    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);
        try {
            if (!isPreviousLoadFinished()) {
                LOG.info("wait transactions before {} to be finished, inverted index job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to send build or delete inverted index file tasks."
                + "job: {}, is delete: {}", jobId, isDropOp);
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new AlterCancelException("Database " + s + " does not exist"));
        OlapTable olapTable;
        try {
            olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        if (!checkTableStable(olapTable)) {
            return;
        }

        olapTable.readLock();
        try {
            List<Column> originSchemaColumns = olapTable.getSchemaByIndexId(originIndexId, true);
            for (Column col : originSchemaColumns) {
                TColumn tColumn = col.toThrift();
                col.setIndexFlag(tColumn, olapTable);
            }
            int originSchemaHash = olapTable.getSchemaHashByIndexId(originIndexId);
            Partition partition = olapTable.getPartition(partitionId);
            MaterializedIndex origIdx = partition.getIndex(originIndexId);
            for (Tablet originTablet : origIdx.getTablets()) {
                long taskSignature = Env.getCurrentEnv().getNextId();
                long originTabletId = originTablet.getId();
                List<Replica> originReplicas = originTablet.getReplicas();
                for (Replica originReplica : originReplicas) {
                    if (originReplica.getBackendId() < 0) {
                        LOG.warn("replica:{}, backendId: {}", originReplica, originReplica.getBackendId());
                        throw new AlterCancelException("originReplica:" + originReplica.getId()
                                + " backendId < 0");
                    }
                    AlterInvertedIndexTask alterInvertedIndexTask = new AlterInvertedIndexTask(
                            originReplica.getBackendId(), db.getId(), olapTable.getId(),
                            partitionId, originIndexId, originTabletId,
                            originSchemaHash, olapTable.getIndexes(),
                            alterInvertedIndexes, originSchemaColumns,
                            isDropOp, taskSignature, jobId);
                    invertedIndexBatchTask.addTask(alterInvertedIndexTask);
                }
            } // end for tablet

            LOG.info("invertedIndexBatchTask:{}", invertedIndexBatchTask);
            AgentTaskQueue.addBatchTask(invertedIndexBatchTask);
            AgentTaskExecutor.submit(invertedIndexBatchTask);
        } finally {
            olapTable.readUnlock();
        }
        this.jobState = JobState.RUNNING;
        LOG.info("transfer inverted index job {} state to {}", jobId, this.jobState);
    }

    protected void runRunningJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);
        // must check if db or table still exist first.
        // or if table is dropped, the tasks will never be finished,
        // and the job will be in RUNNING state forever.
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new AlterCancelException("Database " + s + " does not exist"));
        OlapTable tbl;
        try {
            tbl = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        if (!invertedIndexBatchTask.isFinished()) {
            LOG.info("inverted index tasks not finished. job: {}, partitionId: {}", jobId, partitionId);
            List<AgentTask> tasks = invertedIndexBatchTask.getUnfinishedTasks(2000);
            for (AgentTask task : tasks) {
                if (task.getFailedTimes() > 3) {
                    LOG.warn("alter inverted index task failed: " + task.getErrorMsg());
                    List<Long> failedBackends = failedTabletBackends.computeIfAbsent(task.getTabletId(),
                            k -> Lists.newArrayList());
                    failedBackends.add(task.getBackendId());
                    int expectSucceedTaskNum = tbl.getPartitionInfo()
                            .getReplicaAllocation(task.getPartitionId()).getTotalReplicaNum();
                    int failedTaskCount = failedBackends.size();
                    if (expectSucceedTaskNum - failedTaskCount < expectSucceedTaskNum / 2 + 1) {
                        throw new AlterCancelException("inverted index tasks failed on same tablet reach threshold "
                            + failedTaskCount);
                    }
                }
            }
            return;
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        Env.getCurrentEnv().getEditLog().logIndexChangeJob(this);
        LOG.info("inverted index job finished: {}", jobId);
    }

    /**
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    protected boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }

        cancelInternal();

        jobState = JobState.CANCELLED;
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        Env.getCurrentEnv().getEditLog().logIndexChangeJob(this);
        LOG.info("cancel index job {}, err: {}", jobId, errMsg);
        return true;
    }

    private void cancelInternal() {
        // clear tasks if has
        AgentTaskQueue.removeBatchTask(invertedIndexBatchTask, TTaskType.ALTER_INVERTED_INDEX);
        // TODO maybe delete already build index files
    }

    public void replay(IndexChangeJob replayedJob) {
        try {
            IndexChangeJob replayedIndexChangeJob = (IndexChangeJob) replayedJob;
            switch (replayedJob.jobState) {
                case WAITING_TXN:
                    replayCreateJob(replayedIndexChangeJob);
                    break;
                case FINISHED:
                    replayRunningJob(replayedIndexChangeJob);
                    break;
                case CANCELLED:
                    replayCancelled(replayedIndexChangeJob);
                    break;
                default:
                    break;
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("[INCONSISTENT META] replay inverted index job failed {}", replayedJob.getJobId(), e);
        }
    }

    private void replayCreateJob(IndexChangeJob replayedJob) throws MetaNotFoundException {
        // do nothing, resend inverted index task to be
        this.watershedTxnId = replayedJob.watershedTxnId;
        this.jobState = JobState.WAITING_TXN;
        LOG.info("replay waiting_txn inverted index job: {}, table id: {}", jobId, tableId);
    }

    private void replayRunningJob(IndexChangeJob replayedJob) {
        // do nothing, finish inverted index task
        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        LOG.info("replay finished inverted index job: {} table id: {}", jobId, tableId);
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(IndexChangeJob replayedJob) {
        cancelInternal();

        this.jobState = JobState.CANCELLED;
        this.errMsg = replayedJob.errMsg;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        LOG.info("cancel index job {}, err: {}", jobId, errMsg);
    }

    public static IndexChangeJob read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_122) {
            IndexChangeJob job = new IndexChangeJob();
            job.readFields(in);
            return job;
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, IndexChangeJob.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, IndexChangeJob.class);
        Text.writeString(out, json);
    }

    protected void readFields(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_122) {
            jobId = in.readLong();
            jobState = JobState.valueOf(Text.readString(in));
            dbId = in.readLong();
            tableId = in.readLong();
            tableName = Text.readString(in);
            partitionId = in.readLong();
            partitionName = Text.readString(in);
            errMsg = Text.readString(in);
            createTimeMs = in.readLong();
            finishedTimeMs = in.readLong();
            watershedTxnId = in.readLong();
            isDropOp = in.readBoolean();
            alterInvertedIndexes = Lists.newArrayList();
            int alterInvertedIndexesSize = in.readInt();
            for (int i = 0; i < alterInvertedIndexesSize; ++i) {
                Index alterIndex = Index.read(in);
                alterInvertedIndexes.add(alterIndex);
            }
            originIndexId = in.readLong();
            invertedIndexBatchTask = new AgentBatchTask();
        }
    }

    public String getAlterInvertedIndexesInfo() {
        String info = null;
        List<String> infoList = Lists.newArrayList();
        String invertedIndexChangeInfo = "";
        for (Index invertedIndex : alterInvertedIndexes) {
            invertedIndexChangeInfo += "[" + (isDropOp ? "DROP " : "ADD ") + invertedIndex.toString() + "], ";
        }
        infoList.add(invertedIndexChangeInfo);
        info = Joiner.on(", ").join(infoList.subList(0, infoList.size()));
        return info;
    }

    public void getInfo(List<List<Comparable>> infos) {
        // calc progress first. all index share the same process
        String progress = FeConstants.null_string;
        if (jobState == JobState.RUNNING && invertedIndexBatchTask.getTaskNum() > 0) {
            progress = invertedIndexBatchTask.getFinishedTaskNum() + "/" + invertedIndexBatchTask.getTaskNum();
        }

        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);
        info.add(tableName);
        info.add(partitionName);
        info.add(getAlterInvertedIndexesInfo());
        info.add(TimeUtils.longToTimeStringWithms(createTimeMs));
        info.add(TimeUtils.longToTimeStringWithms(finishedTimeMs));
        info.add(watershedTxnId);
        info.add(jobState.name());
        info.add(errMsg);
        info.add(progress);

        infos.add(info);
    }
}
