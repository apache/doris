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
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.common.FailureReason;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.SourceOffsetProviderFactory;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterJobCommand;
import org.apache.doris.nereids.trees.plans.commands.info.BaseViewInfo;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertUtils;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.rpc.RpcException;
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
import org.apache.commons.lang3.StringUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Log4j2
public class StreamingInsertJob extends AbstractJob<StreamingJobSchedulerTask, Map<Object, Object>> implements
        TxnStateChangeCallback, GsonPostProcessable {
    private long dbId;
    private StreamingJobStatistic jobStatistic = new StreamingJobStatistic();
    @Getter
    @Setter
    @SerializedName("fr")
    protected FailureReason failureReason;
    @Getter
    @Setter
    protected long latestAutoResumeTimestamp;
    @Getter
    @Setter
    protected long autoResumeCount;
    @Getter
    @SerializedName("props")
    private Map<String, String> properties;
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
    private ConcurrentLinkedQueue<StreamingInsertTask> streamInsertTaskQueue = new ConcurrentLinkedQueue<>();
    @Setter
    @Getter
    private String jobRuntimeMsg = "";

    public StreamingInsertJob(String jobName,
            JobStatus jobStatus,
            String dbName,
            String comment,
            UserIdentity createUser,
            JobExecutionConfiguration jobConfig,
            Long createTimeMs,
            String executeSql,
            Map<String, String> properties) {
        super(Env.getCurrentEnv().getNextId(), jobName, jobStatus, dbName, comment, createUser,
                jobConfig, createTimeMs, executeSql);
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
            // validate offset props
            if (jobProperties.getOffsetProperty() != null) {
                Offset offset = validateOffset(jobProperties.getOffsetProperty());
                this.offsetProvider.updateOffset(offset);
            }
        } catch (AnalysisException ae) {
            log.warn("parse streaming insert job failed, props: {}", properties, ae);
            throw new RuntimeException(ae.getMessage());
        } catch (Exception ex) {
            log.warn("init streaming insert job failed, sql: {}", getExecuteSql(), ex);
            throw new RuntimeException(ex.getMessage());
        }
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        if (lock.writeLock().isHeldByCurrentThread()) {
            lock.writeLock().unlock();
        }
    }

    private UnboundTVFRelation getCurrentTvf() {
        initLogicalPlan(false);
        List<UnboundTVFRelation> allTVFRelation = baseCommand.getAllTVFRelation();
        Preconditions.checkArgument(allTVFRelation.size() == 1, "Only support one source in insert streaming job");
        UnboundTVFRelation unboundTVFRelation = allTVFRelation.get(0);
        return unboundTVFRelation;
    }

    private void makeConnectContext() {
        ConnectContext ctx = InsertTask.makeConnectContext(getCreateUser(), getCurrentDbName());
        StatementContext statementContext = new StatementContext();
        ctx.setStatementContext(statementContext);
    }

    public void initLogicalPlan(boolean regen) {
        if (regen || baseCommand == null) {
            this.baseCommand = null;
            makeConnectContext();
            LogicalPlan logicalPlan = new NereidsParser().parseSingle(getExecuteSql());
            this.baseCommand = (InsertIntoTableCommand) logicalPlan;
        }
    }

    /**
     * Check whether Offset can be serialized into the corresponding data source
     * */
    public Offset validateOffset(String offsetStr) throws AnalysisException {
        Offset offset;
        try {
            offset = offsetProvider.deserializeOffsetProperty(offsetStr);
        } catch (Exception ex) {
            log.info("initialize offset failed, offset: {}", offsetStr, ex);
            throw new AnalysisException("Failed to initialize offset, " + ex.getMessage());
        }
        if (offset == null || !offset.isValidOffset()) {
            throw new AnalysisException("Invalid format for offset: " + offsetStr);
        }
        return offset;
    }

    public void alterJob(AlterJobCommand alterJobCommand) throws AnalysisException, JobException {
        List<String> logParts = new ArrayList<>();
        // update sql
        if (StringUtils.isNotEmpty(alterJobCommand.getSql())) {
            setExecuteSql(alterJobCommand.getSql());
            initLogicalPlan(true);
            String encryptedSql = generateEncryptedSql();
            logParts.add("sql: " + encryptedSql);
        }

        // update properties
        if (!alterJobCommand.getProperties().isEmpty()) {
            modifyPropertiesInternal(alterJobCommand.getProperties());
            logParts.add("properties: " + alterJobCommand.getProperties());
        }
        log.info("Alter streaming job {}, {}", getJobId(), String.join(", ", logParts));
    }

    @Override
    public void updateJobStatus(JobStatus status) throws JobException {
        lock.writeLock().lock();
        try {
            super.updateJobStatus(status);
            if (JobStatus.PAUSED.equals(getJobStatus())) {
                clearRunningStreamTask(status);
            }
            if (isFinalStatus()) {
                Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(getJobId());
            }
            log.info("Streaming insert job {} update status to {}", getJobId(), getJobStatus());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void resetFailureInfo(FailureReason reason) {
        this.setFailureReason(reason);
        // Currently, only delayMsg is present here, which needs to be cleared when the status changes.
        this.setJobRuntimeMsg("");
    }

    @Override
    public void cancelAllTasks(boolean needWaitCancelComplete) throws JobException {
        lock.writeLock().lock();
        try {
            if (runningStreamTask == null) {
                return;
            }
            runningStreamTask.cancel(needWaitCancelComplete);
            canceledTaskCount.incrementAndGet();
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
        return CollectionUtils.isEmpty(getRunningTasks()) && !isFinalStatus();
    }

    @Override
    public boolean isJobRunning() {
        return !isFinalStatus();
    }

    private boolean isFinalStatus() {
        return getJobStatus().equals(JobStatus.STOPPED) || getJobStatus().equals(JobStatus.FINISHED);
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
        if (originTvfProps == null) {
            this.originTvfProps = getCurrentTvf().getProperties().getMap();
        }
        this.runningStreamTask = new StreamingInsertTask(getJobId(), Env.getCurrentEnv().getNextId(), getExecuteSql(),
                offsetProvider, getCurrentDbName(), jobProperties, originTvfProps, getCreateUser());
        Env.getCurrentEnv().getJobManager().getStreamingTaskManager().registerTask(runningStreamTask);
        this.runningStreamTask.setStatus(TaskStatus.PENDING);
        log.info("create new streaming insert task for job {}, task {} ",
                getJobId(), runningStreamTask.getTaskId());
        recordTasks(runningStreamTask);
        return runningStreamTask;
    }

    public void recordTasks(StreamingInsertTask task) {
        if (Config.max_streaming_task_show_count < 1) {
            return;
        }
        streamInsertTaskQueue.add(task);

        while (streamInsertTaskQueue.size() > Config.max_streaming_task_show_count) {
            streamInsertTaskQueue.poll();
        }
    }

    /**
     * for show command to display all streaming insert tasks of this job.
     */
    public List<StreamingInsertTask> queryAllStreamTasks() {
        if (CollectionUtils.isEmpty(streamInsertTaskQueue)) {
            return new ArrayList<>();
        }
        List<StreamingInsertTask> tasks = new ArrayList<>(streamInsertTaskQueue);
        Comparator<StreamingInsertTask> taskComparator =
                Comparator.comparingLong(StreamingInsertTask::getCreateTimeMs).reversed();
        tasks.sort(taskComparator);
        return tasks;
    }

    protected void fetchMeta() {
        try {
            if (originTvfProps == null) {
                this.originTvfProps = getCurrentTvf().getProperties().getMap();
            }
            offsetProvider.fetchRemoteMeta(originTvfProps);
        } catch (Exception ex) {
            log.warn("fetch remote meta failed, job id: {}", getJobId(), ex);
            failureReason = new FailureReason(InternalErrorCode.GET_REMOTE_DATA_ERROR,
                    "Failed to fetch meta, " + ex.getMessage());
        }
    }

    public boolean needScheduleTask() {
        readLock();
        try {
            return (getJobStatus().equals(JobStatus.RUNNING)
                    || getJobStatus().equals(JobStatus.PENDING));
        } finally {
            readUnlock();
        }
    }

    public void clearRunningStreamTask(JobStatus newJobStatus) {
        if (runningStreamTask != null) {
            log.info("clear running streaming insert task for job {}, task {}, status {} ",
                    getJobId(), runningStreamTask.getTaskId(), runningStreamTask.getStatus());
            runningStreamTask.cancel(JobStatus.STOPPED.equals(newJobStatus) ? false : true);
            runningStreamTask.closeOrReleaseResources();
        }
    }

    public boolean hasMoreDataToConsume() {
        return offsetProvider.hasMoreDataToConsume();
    }

    @Override
    public void onTaskFail(StreamingJobSchedulerTask task) throws JobException {
        if (task.getErrMsg() != null) {
            this.failureReason = new FailureReason(task.getErrMsg());
        }
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
            this.failureReason = new FailureReason(task.getErrMsg());
        } finally {
            writeUnlock();
        }
        updateJobStatus(JobStatus.PAUSED);
    }

    public void onStreamTaskSuccess(StreamingInsertTask task) {
        try {
            succeedTaskCount.incrementAndGet();
            Env.getCurrentEnv().getJobManager().getStreamingTaskManager().removeRunningTask(task);
            StreamingInsertTask nextTask = createStreamingInsertTask();
            this.runningStreamTask = nextTask;
            log.info("Streaming insert job {} create next streaming insert task {} after task {} success",
                    getJobId(), nextTask.getTaskId(), task.getTaskId());
        } finally {
            writeUnlock();
        }
    }

    private void updateJobStatisticAndOffset(StreamingTaskTxnCommitAttachment attachment) {
        if (this.jobStatistic == null) {
            this.jobStatistic = new StreamingJobStatistic();
        }
        this.jobStatistic.setScannedRows(this.jobStatistic.getScannedRows() + attachment.getScannedRows());
        this.jobStatistic.setLoadBytes(this.jobStatistic.getLoadBytes() + attachment.getLoadBytes());
        this.jobStatistic.setFileNumber(this.jobStatistic.getFileNumber() + attachment.getNumFiles());
        this.jobStatistic.setFileSize(this.jobStatistic.getFileSize() + attachment.getFileBytes());
        offsetProvider.updateOffset(offsetProvider.deserializeOffset(attachment.getOffset()));
    }

    private void updateCloudJobStatisticAndOffset(StreamingTaskTxnCommitAttachment attachment) {
        if (this.jobStatistic == null) {
            this.jobStatistic = new StreamingJobStatistic();
        }
        this.jobStatistic.setScannedRows(attachment.getScannedRows());
        this.jobStatistic.setLoadBytes(attachment.getLoadBytes());
        this.jobStatistic.setFileNumber(attachment.getNumFiles());
        this.jobStatistic.setFileSize(attachment.getFileBytes());
        offsetProvider.updateOffset(offsetProvider.deserializeOffset(attachment.getOffset()));
    }

    @Override
    public void onRegister() throws JobException {
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(this);
    }

    @Override
    public void onReplayCreate() throws JobException {
        onRegister();
        super.onReplayCreate();
    }

    /**
     * Because the offset statistics of the streamingInsertJob are all stored in txn,
     * only some fields are replayed here.
     * @param replayJob
     */
    public void replayOnUpdated(StreamingInsertJob replayJob) {
        setJobStatus(replayJob.getJobStatus());
        try {
            modifyPropertiesInternal(replayJob.getProperties());
            // When the pause state is restarted, it also needs to be updated
            if (Config.isCloudMode()) {
                replayOnCloudMode();
            }
        } catch (Exception e) {
            // should not happen
            log.error("replay modify streaming insert job properties failed, job id: {}", getJobId(), e);
        }
        setExecuteSql(replayJob.getExecuteSql());
        setSucceedTaskCount(replayJob.getSucceedTaskCount());
        setFailedTaskCount(replayJob.getFailedTaskCount());
        setCanceledTaskCount(replayJob.getCanceledTaskCount());
    }

    /**
     * When updating offset, you need to reset the currentOffset
     */
    private void modifyPropertiesInternal(Map<String, String> inputProperties) throws AnalysisException, JobException {
        StreamingJobProperties inputStreamProps = new StreamingJobProperties(inputProperties);
        if (StringUtils.isNotEmpty(inputStreamProps.getOffsetProperty())) {
            Offset offset = validateOffset(inputStreamProps.getOffsetProperty());
            this.offsetProvider.updateOffset(offset);
            if (Config.isCloudMode()) {
                resetCloudProgress(offset);
            }
        }
        this.properties.putAll(inputProperties);
        this.jobProperties = new StreamingJobProperties(this.properties);
    }

    private void resetCloudProgress(Offset offset) throws JobException {
        Cloud.ResetStreamingJobOffsetRequest.Builder builder = Cloud.ResetStreamingJobOffsetRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id);
        builder.setDbId(getDbId());
        builder.setJobId(getJobId());
        builder.setOffset(offset.toSerializedJson());

        Cloud.ResetStreamingJobOffsetResponse response;
        try {
            response = MetaServiceProxy.getInstance().resetStreamingJobOffset(builder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                log.warn("failed to reset streaming job cloud offset, response: {}", response);
                if (response.getStatus().getCode() == Cloud.MetaServiceCode.ROUTINE_LOAD_PROGRESS_NOT_FOUND) {
                    log.warn("not found streaming job offset, response: {}", response);
                    return;
                } else {
                    throw new JobException(response.getStatus().getMsg());
                }
            }
        } catch (RpcException e) {
            log.info("failed to reset cloud progress, ", e);
            throw new JobException(e.getMessage());
        }
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
        trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        trow.addToColumnValue(new TCell().setStringVal(getJobStatus().name()));

        trow.addToColumnValue(new TCell().setStringVal(StringUtils.isNotEmpty(getEncryptedSql())
                ? getEncryptedSql() : generateEncryptedSql()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getSucceedTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getFailedTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getCanceledTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(getComment()));
        trow.addToColumnValue(new TCell().setStringVal(properties != null && !properties.isEmpty()
                ? GsonUtils.GSON.toJson(properties) : FeConstants.null_string));

        if (offsetProvider != null && StringUtils.isNotEmpty(offsetProvider.getShowCurrentOffset())) {
            trow.addToColumnValue(new TCell().setStringVal(offsetProvider.getShowCurrentOffset()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        }

        if (offsetProvider != null && StringUtils.isNotEmpty(offsetProvider.getShowMaxOffset())) {
            trow.addToColumnValue(new TCell().setStringVal(offsetProvider.getShowMaxOffset()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        }

        trow.addToColumnValue(new TCell().setStringVal(
                jobStatistic == null ? FeConstants.null_string : jobStatistic.toJson()));
        trow.addToColumnValue(new TCell().setStringVal(failureReason == null
                ? FeConstants.null_string : failureReason.getMsg()));
        trow.addToColumnValue(new TCell().setStringVal(jobRuntimeMsg == null
                ? FeConstants.null_string : jobRuntimeMsg));
        return trow;
    }

    private static boolean checkPrivilege(ConnectContext ctx, LogicalPlan logicalPlan) throws AnalysisException {
        if (!(logicalPlan instanceof InsertIntoTableCommand)) {
            throw new AnalysisException("Only support insert command");
        }
        LogicalPlan logicalQuery = ((InsertIntoTableCommand) logicalPlan).getLogicalQuery();
        List<String> targetTable = InsertUtils.getTargetTableQualified(logicalQuery, ctx);
        Preconditions.checkArgument(targetTable.size() == 3, "target table name is invalid");
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx,
                InternalCatalog.INTERNAL_CATALOG_NAME,
                targetTable.get(1),
                targetTable.get(2),
                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ctx.getQualifiedUser(),
                    ctx.getRemoteIP(),
                    targetTable.get(1),
                    targetTable.get(2));
        }
        return true;
    }

    public boolean checkPrivilege(ConnectContext ctx) throws AnalysisException {
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(getExecuteSql());
        return checkPrivilege(ctx, logicalPlan);
    }

    public static boolean checkPrivilege(ConnectContext ctx, String sql) throws AnalysisException {
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        return checkPrivilege(ctx, logicalPlan);
    }

    public boolean hasPrivilege(UserIdentity userIdentity) {
        ConnectContext ctx = InsertTask.makeConnectContext(userIdentity, getCurrentDbName());
        try {
            LogicalPlan logicalPlan = new NereidsParser().parseSingle(getExecuteSql());
            LogicalPlan logicalQuery = ((InsertIntoTableCommand) logicalPlan).getLogicalQuery();
            List<String> targetTable = InsertUtils.getTargetTableQualified(logicalQuery, ctx);
            Preconditions.checkArgument(targetTable.size() == 3, "target table name is invalid");
            return Env.getCurrentEnv().getAccessManager().checkTblPriv(userIdentity,
                    InternalCatalog.INTERNAL_CATALOG_NAME,
                    targetTable.get(1),
                    targetTable.get(2),
                    PrivPredicate.LOAD);
        } finally {
            ctx.cleanup();
        }
    }

    private String generateEncryptedSql() {
        makeConnectContext();
        TreeMap<Pair<Integer, Integer>, String> indexInSqlToString = new TreeMap<>(new Pair.PairComparator<>());
        new NereidsParser().parseForEncryption(getExecuteSql(), indexInSqlToString);
        BaseViewInfo.rewriteSql(indexInSqlToString, getExecuteSql());
        String encryptSql = BaseViewInfo.rewriteSql(indexInSqlToString, getExecuteSql());
        setEncryptedSql(encryptSql);
        return encryptSql;
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
        boolean shouldReleaseLock = false;
        writeLock();
        try {
            if (runningStreamTask.getIsCanceled().get()) {
                log.info("streaming insert job {} task {} is canceled, skip beforeCommitted",
                        getJobId(), runningStreamTask.getTaskId());
                return;
            }

            ArrayList<Long> taskIds = new ArrayList<>();
            taskIds.add(runningStreamTask.getTaskId());
            // todo: Check whether the taskid of runningtask is consistent with the taskid associated with txn

            List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager().queryLoadJobsByJobIds(taskIds);
            if (loadJobs.size() == 0) {
                throw new TransactionException("load job not found, insert job id is " + runningStreamTask.getTaskId());
            }
            LoadJob loadJob = loadJobs.get(0);
            LoadStatistic loadStatistic = loadJob.getLoadStatistic();
            txnState.setTxnCommitAttachment(new StreamingTaskTxnCommitAttachment(
                        getJobId(),
                        runningStreamTask.getTaskId(),
                        loadStatistic.getScannedRows(),
                        loadStatistic.getLoadBytes(),
                        loadStatistic.getFileNumber(),
                        loadStatistic.getTotalFileSizeB(),
                        runningStreamTask.getRunningOffset().toSerializedJson()));
        } finally {
            if (shouldReleaseLock) {
                writeUnlock();
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
        succeedTaskCount.incrementAndGet();
    }

    public long getDbId() {
        if (dbId <= 0) {
            try {
                this.dbId = Env.getCurrentInternalCatalog().getDbOrAnalysisException(getCurrentDbName()).getId();
            } catch (AnalysisException e) {
                log.warn("failed to get db id for streaming insert job {}, db name: {}, msg: {}",
                        getJobId(), getCurrentDbName(), e.getMessage());
                failureReason = new FailureReason(InternalErrorCode.DB_ERR, "Failed to get db id, " + e.getMessage());
            }
        }
        return dbId;
    }

    public void replayOnCloudMode() throws JobException {
        Cloud.GetStreamingTaskCommitAttachRequest.Builder builder =
                Cloud.GetStreamingTaskCommitAttachRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id);
        builder.setDbId(getDbId());
        builder.setJobId(getJobId());

        Cloud.GetStreamingTaskCommitAttachResponse response = null;
        try {
            response = MetaServiceProxy.getInstance().getStreamingTaskCommitAttach(builder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                log.warn("failed to get streaming task commit attach, response: {}", response);
                if (response.getStatus().getCode() == Cloud.MetaServiceCode.STREAMING_JOB_PROGRESS_NOT_FOUND) {
                    log.warn("not found streaming job progress, response: {}", response);
                    return;
                } else {
                    throw new JobException(response.getStatus().getMsg());
                }
            }
        } catch (RpcException e) {
            log.info("failed to get streaming task commit attach {}", response, e);
            throw new JobException(e.getMessage());
        }

        StreamingTaskTxnCommitAttachment commitAttach =
                new StreamingTaskTxnCommitAttachment(response.getCommitAttach());
        updateCloudJobStatisticAndOffset(commitAttach);
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

        if (null == getSucceedTaskCount()) {
            setSucceedTaskCount(new AtomicLong(0));
        }
        if (null == getFailedTaskCount()) {
            setFailedTaskCount(new AtomicLong(0));
        }
        if (null == getCanceledTaskCount()) {
            setCanceledTaskCount(new AtomicLong(0));
        }

        if (null == streamInsertTaskQueue) {
            streamInsertTaskQueue = new ConcurrentLinkedQueue<>();
        }

        if (null == lock) {
            this.lock = new ReentrantReadWriteLock(true);
        }
    }
}
