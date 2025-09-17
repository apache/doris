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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.PauseReason;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.SourceOffsetProviderFactory;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections.CollectionUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Log4j2
public class StreamingInsertJob extends AbstractJob<StreamingJobSchedulerTask, Map<Object, Object>> implements
        TxnStateChangeCallback, GsonPostProcessable {
    private final long dbId;
    private StreamingJobStatistic jobStatistic = new StreamingJobStatistic();
    @Getter
    protected PauseReason pauseReason;
    @Getter
    @Setter
    protected long latestAutoResumeTimestamp;
    @Getter
    @Setter
    protected long autoResumeCount;
    @Getter
    @SerializedName("props")
    private final Map<String, String> properties;
    private StreamingJobProperties jobProperties;
    @Getter
    @SerializedName("tvf")
    private String tvfType;
    private Map<String, String> originTvfProps;
    @Getter
    StreamingInsertTask runningStreamTask;
    SourceOffsetProvider offsetProvider;
    @Setter
    @Getter
    private long lastScheduleTaskTimestamp = -1L;
    private InsertIntoTableCommand baseCommand;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public StreamingInsertJob(String jobName,
            JobStatus jobStatus,
            String dbName,
            String comment,
            UserIdentity createUser,
            JobExecutionConfiguration jobConfig,
            Long createTimeMs,
            String executeSql,
            Map<String, String> properties) {
        super(getNextJobId(), jobName, jobStatus, dbName, comment, createUser,
                jobConfig, createTimeMs, executeSql);
        this.dbId = ConnectContext.get().getCurrentDbId();
        this.properties = properties;
        init();
    }

    private void init() {
        try {
            this.jobProperties = new StreamingJobProperties(properties);
            jobProperties.validate();
            // build time definition
            JobExecutionConfiguration execConfig = getJobConfig();
            TimerDefinition timerDefinition = new TimerDefinition();
            timerDefinition.setInterval(jobProperties.getMaxIntervalSecond());
            timerDefinition.setIntervalUnit(IntervalUnit.SECOND);
            timerDefinition.setStartTimeMs(execConfig.getTimerDefinition().getStartTimeMs());
            execConfig.setTimerDefinition(timerDefinition);

            UnboundTVFRelation currentTvf = getCurrentTvf();
            this.tvfType = currentTvf.getFunctionName();
            this.originTvfProps = currentTvf.getProperties().getMap();
            this.offsetProvider = SourceOffsetProviderFactory.createSourceOffsetProvider(currentTvf.getFunctionName());
        } catch (AnalysisException ae) {
            log.warn("parse streaming insert job failed, props: {}", properties, ae);
            throw new RuntimeException("parse streaming insert job failed, " +  ae.getMessage());
        } catch (Exception ex) {
            log.warn("init streaming insert job failed, sql: {}", getExecuteSql(), ex);
            throw new RuntimeException("init streaming insert job failed, " +  ex.getMessage());
        }
    }

    private UnboundTVFRelation getCurrentTvf() {
        if (baseCommand == null) {
            this.baseCommand = (InsertIntoTableCommand) new NereidsParser().parseSingle(getExecuteSql());
        }
        List<UnboundTVFRelation> allTVFRelation = baseCommand.getAllTVFRelation();
        Preconditions.checkArgument(allTVFRelation.size() == 1, "Only support one source in insert streaming job");
        UnboundTVFRelation unboundTVFRelation = allTVFRelation.get(0);
        return unboundTVFRelation;
    }

    @Override
    public void updateJobStatus(JobStatus status) throws JobException {
        lock.writeLock().lock();
        try {
            super.updateJobStatus(status);
            if (isFinalStatus()) {
                Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(getJobId());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public JobType getJobType() {
        return JobType.INSERT;
    }

    @Override
    protected void checkJobParamsInternal() {
    }

    @Override
    public boolean isReadyForScheduling(Map<Object, Object> taskContext) {
        return CollectionUtils.isEmpty(getRunningTasks());
    }

    @Override
    public List<StreamingJobSchedulerTask> createTasks(TaskType taskType, Map<Object, Object> taskContext) {
        List<StreamingJobSchedulerTask> newTasks = new ArrayList<>();
        StreamingJobSchedulerTask streamingJobSchedulerTask = new StreamingJobSchedulerTask(this);
        newTasks.add(streamingJobSchedulerTask);
        super.initTasks(newTasks, taskType);
        return newTasks;
    }

    protected StreamingInsertTask createStreamingInsertTask() {
        this.runningStreamTask = new StreamingInsertTask(getJobId(), AbstractTask.getNextTaskId(), getExecuteSql(),
                offsetProvider, getCurrentDbName(), jobProperties, originTvfProps, getCreateUser());
        Env.getCurrentEnv().getJobManager().getStreamingTaskManager().registerTask(runningStreamTask);
        this.runningStreamTask.setStatus(TaskStatus.PENDING);
        return runningStreamTask;
    }

    protected void fetchMeta() {
        try {
            if (originTvfProps == null) {
                this.originTvfProps = getCurrentTvf().getProperties().getMap();
            }
            offsetProvider.fetchRemoteMeta(originTvfProps);
        } catch (Exception ex) {
            log.warn("fetch remote meta failed, job id: {}", getJobId(), ex);
        }
    }

    public boolean needScheduleTask() {
        return (getJobStatus().equals(JobStatus.RUNNING) || getJobStatus().equals(JobStatus.PENDING));
    }

    // When consumer to EOF, delay schedule task appropriately can avoid too many small transactions.
    public boolean needDelayScheduleTask() {
        return System.currentTimeMillis() - lastScheduleTaskTimestamp > jobProperties.getMaxIntervalSecond() * 1000;
    }

    public boolean hasMoreDataToConsume() {
        return offsetProvider.hasMoreDataToConsume();
    }

    @Override
    public void onTaskFail(StreamingJobSchedulerTask task) throws JobException {
        // Here is the failure of StreamingJobSchedulerTask, no processing is required
        getRunningTasks().remove(task);
    }

    @Override
    public void onTaskSuccess(StreamingJobSchedulerTask task) throws JobException {
        // Here is the success of StreamingJobSchedulerTask, no processing is required
        getRunningTasks().remove(task);
    }

    public void onStreamTaskFail(StreamingInsertTask task) throws JobException {
        try {
            failedTaskCount.incrementAndGet();
            Env.getCurrentEnv().getJobManager().getStreamingTaskManager().removeRunningTask(task);
            if (getJobConfig().getExecuteType().equals(JobExecuteType.INSTANT)) {
                this.pauseReason = new PauseReason(InternalErrorCode.INTERNAL_ERR, task.getErrMsg());
            }
        } finally {
            lock.writeLock().unlock();
        }
        updateJobStatus(JobStatus.PAUSED);
    }

    public void onStreamTaskSuccess(StreamingInsertTask task) {
        try {
            succeedTaskCount.incrementAndGet();
            Env.getCurrentEnv().getJobManager().getStreamingTaskManager().removeRunningTask(task);
            StreamingInsertTask nextTask = createStreamingInsertTask();
            this.runningStreamTask = nextTask;
            //todo: maybe fetch from txn attachment?
            offsetProvider.updateOffset(task.getRunningOffset());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateJobStatisticAndOffset(StreamingTaskTxnCommitAttachment attachment) {
        this.jobStatistic.setScannedRows(this.jobStatistic.getScannedRows() + attachment.getScannedRows());
        this.jobStatistic.setLoadBytes(this.jobStatistic.getLoadBytes() + attachment.getLoadBytes());
        this.jobStatistic.setFileNumber(this.jobStatistic.getFileNumber() + attachment.getFileNumber());
        this.jobStatistic.setFileSize(this.jobStatistic.getFileSize() + attachment.getFileSize());
        offsetProvider.updateOffset(attachment.getOffset());
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return InsertJob.TASK_META_DATA;
    }

    @Override
    public List<String> getShowInfo() {
        return getCommonShowInfo();
    }

    @Override
    public TRow getTvfInfo() {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(getJobName()));
        trow.addToColumnValue(new TCell().setStringVal(getCreateUser().getQualifiedUser()));
        trow.addToColumnValue(new TCell().setStringVal(getJobConfig().getExecuteType().name()));
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(getJobStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(getExecuteSql()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getSucceedTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getFailedTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getCanceledTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(getComment()));

        if (offsetProvider != null && offsetProvider.getSyncOffset() != null) {
            trow.addToColumnValue(new TCell().setStringVal(offsetProvider.getSyncOffset()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        }

        if (offsetProvider != null && offsetProvider.getRemoteOffset() != null) {
            trow.addToColumnValue(new TCell().setStringVal(offsetProvider.getRemoteOffset()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        }

        trow.addToColumnValue(new TCell().setStringVal(
                jobStatistic == null ? FeConstants.null_string : jobStatistic.toJson()));
        trow.addToColumnValue(
                new TCell().setStringVal(pauseReason == null ? FeConstants.null_string : pauseReason.getMsg()));
        return trow;
    }

    @Override
    public String formatMsgWhenExecuteQueueFull(Long taskId) {
        return commonFormatMsgWhenExecuteQueueFull(taskId, "streaming_task_queue_size",
                "job_streaming_task_consumer_thread_num");
    }

    @Override
    public List<StreamingJobSchedulerTask> queryTasks() {
        if (!getRunningTasks().isEmpty()) {
            return getRunningTasks();
        } else {
            return Arrays.asList(new StreamingJobSchedulerTask(this));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public long getId() {
        return getJobId();
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        boolean shouldRealseLock = false;
        lock.writeLock().lock();
        try {
            ArrayList<Long> taskIds = new ArrayList<>();
            taskIds.add(runningStreamTask.getTaskId());
            List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager().queryLoadJobsByJobIds(taskIds);
            if (loadJobs.size() != 1) {
                throw new TransactionException("load job not found, insert job id is " + runningStreamTask.getTaskId());
            }
            LoadJob loadJob = loadJobs.get(0);

            if (txnState.getTransactionId() != loadJob.getTransactionId()
                    || !runningStreamTask.getStatus().equals(TaskStatus.RUNNING)) {
                shouldRealseLock = true;
                throw new TransactionException("txn " + txnState.getTransactionId() + "should be aborted.");
            }

            LoadStatistic loadStatistic = loadJob.getLoadStatistic();
            txnState.setTxnCommitAttachment(new StreamingTaskTxnCommitAttachment(
                        getJobId(),
                        runningStreamTask.getTaskId(),
                        loadStatistic.getScannedRows(),
                        loadStatistic.getLoadBytes(),
                        loadStatistic.getFileNumber(),
                        loadStatistic.getTotalFileSizeB(),
                        runningStreamTask.getRunningOffset()));
        } finally {
            if (shouldRealseLock) {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void beforeAborted(TransactionState txnState) throws TransactionException {
    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        Preconditions.checkNotNull(txnState.getTxnCommitAttachment(), txnState);
        StreamingTaskTxnCommitAttachment attachment =
                (StreamingTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
        updateJobStatisticAndOffset(attachment);
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        Preconditions.checkNotNull(txnState.getTxnCommitAttachment(), txnState);
        StreamingTaskTxnCommitAttachment attachment =
                (StreamingTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
        updateJobStatisticAndOffset(attachment);
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {

    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {

    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (offsetProvider == null && tvfType != null) {
            offsetProvider = SourceOffsetProviderFactory.createSourceOffsetProvider(tvfType);
        }

        if (jobProperties == null && properties != null) {
            jobProperties = new StreamingJobProperties(properties);
        }
    }
}
