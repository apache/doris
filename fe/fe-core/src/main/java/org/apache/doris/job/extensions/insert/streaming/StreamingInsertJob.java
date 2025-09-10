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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.PauseReason;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.SourceOffsetProviderFactory;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StreamingInsertJob extends AbstractJob<StreamingJobSchedulerTask, Map<Object, Object>> implements
        TxnStateChangeCallback {

    @SerializedName("did")
    private final long dbId;
    @Getter
    @SerializedName("st")
    protected JobStatus status;
    private LoadStatistic loadStatistic = new LoadStatistic();
    @SerializedName("fm")
    private FailMsg failMsg;
    @Getter
    protected PauseReason pauseReason;
    @Getter
    @Setter
    protected long latestAutoResumeTimestamp;
    @Getter
    @Setter
    protected long autoResumeCount;
    @Getter
    @SerializedName("jp")
    private StreamingJobProperties jobProperties;
    StreamingInsertTask runningStreamingtask;
    SourceOffsetProvider offsetProvider;

    public StreamingInsertJob(String jobName,
            JobStatus jobStatus,
            String dbName,
            String comment,
            UserIdentity createUser,
            JobExecutionConfiguration jobConfig,
            Long createTimeMs,
            String executeSql,
            StreamingJobProperties jobProperties) {
        super(getNextJobId(), jobName, jobStatus, dbName, comment, createUser,
                jobConfig, createTimeMs, executeSql);
        this.dbId = ConnectContext.get().getCurrentDbId();
        this.jobProperties = jobProperties;
        String tvfType = parseTvfType();
        this.offsetProvider = SourceOffsetProviderFactory.createSourceOffsetProvider(tvfType);
    }

    private String parseTvfType() {
        NereidsParser parser = new NereidsParser();
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(getExecuteSql());
        LogicalPlan logicalQuery = command.getLogicalQuery();
        logicalQuery.children();
        //todo: Judging TVF based on plan
        return "s3";
    }

    @Override
    public void updateJobStatus(JobStatus status) throws JobException {
        super.updateJobStatus(status);
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
        return true;
    }

    @Override
    public List<StreamingJobSchedulerTask> createTasks(TaskType taskType, Map<Object, Object> taskContext) {
        return Collections.emptyList();
    }

    protected StreamingInsertTask createStreamingInsertTask() {
        InsertIntoTableCommand command = offsetProvider.rewriteTvfParams(getExecuteSql());
        this.runningStreamingtask = new StreamingInsertTask(command,
                loadStatistic, getCurrentDbName(), offsetProvider.getCurrentOffset(), jobProperties);
        return this.runningStreamingtask;
    }

    protected void fetchMeta() {
        offsetProvider.fetchRemoteMeta();
    }

    @Override
    public void onTaskFail(StreamingJobSchedulerTask task) throws JobException {
        if (getJobConfig().getExecuteType().equals(JobExecuteType.INSTANT)) {
            this.failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, task.getErrMsg());
        }
        // not edit log
    }

    @Override
    public void onTaskSuccess(StreamingJobSchedulerTask task) throws JobException {
        // not edit log
        // need to create new stream insert task and throw to task scheduler
    }


    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return ShowResultSetMetaData.builder().build();
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
        trow.addToColumnValue(new TCell().setStringVal(getJobConfig().convertRecurringStrategyToString()));
        trow.addToColumnValue(new TCell().setStringVal(getJobStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(getExecuteSql()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getSucceedTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getFailedTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getCanceledTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(getComment()));
        trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        trow.addToColumnValue(new TCell().setStringVal(loadStatistic.toJson()));
        trow.addToColumnValue(new TCell().setStringVal(failMsg == null ? FeConstants.null_string : failMsg.getMsg()));
        return trow;
    }

    @Override
    public String formatMsgWhenExecuteQueueFull(Long taskId) {
        return commonFormatMsgWhenExecuteQueueFull(taskId, "streaming_task_queue_size",
                "job_streaming_task_consumer_thread_num");
    }

    @Override
    public List<StreamingJobSchedulerTask> queryTasks() {
        return new ArrayList<>();
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

    }

    @Override
    public void beforeAborted(TransactionState txnState) throws TransactionException {

    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {

    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {

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
}
