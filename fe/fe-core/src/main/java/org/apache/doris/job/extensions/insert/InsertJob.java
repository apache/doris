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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.ErrorTabletInfo;
import org.apache.doris.transaction.TabletCommitInfo;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true)
@Log4j2
public class InsertJob extends AbstractInsertJob<InsertTask> implements GsonPostProcessable {

    @Getter
    private Map<String, String> properties = new HashMap<>();
    @Getter
    private Set<String> tableNames;
    private AuthorizationInfo authorizationInfo;

    private List<ErrorTabletInfo> errorTabletInfos = new ArrayList<>();
    private List<TabletCommitInfo> commitInfos = new ArrayList<>();
    //we used insertTaskQueue to store the task info, and we will query the task info from it
    @Deprecated
    @SerializedName("tis")
    ConcurrentLinkedQueue<Long> historyTaskIdList = new ConcurrentLinkedQueue<>();
    protected InsertTask.TaskType taskType = InsertTask.TaskType.PENDING;
    protected InsertTask.MergeType mergeType = InsertTask.MergeType.APPEND;
    @SerializedName("lt")
    protected InsertJob.LoadType loadType;
    @SerializedName("plans")
    protected List<InsertIntoTableCommand> plans = new ArrayList<>();
    protected LoadStatistic loadStatistic = new LoadStatistic();
    // 0: the job status is pending
    // n/100: n is the number of task which has been finished
    // 99: all tasks have been finished
    // 100: txn status is visible and load has been finished
    @SerializedName("pg")
    protected int progress;

    /**
     * load job type
     */
    public enum LoadType {
        BULK,
        SPARK,
        LOCAL_FILE,
        UNKNOWN

    }

    public enum Priority {
        HIGH(0),
        NORMAL(1),
        LOW(2);

        Priority(int value) {
            this.value = value;
        }

        private final int value;

        public int getValue() {
            return value;
        }
    }

    public InsertJob(String jobName,
                     JobStatus jobStatus,
                     String dbName,
                     String comment,
                     UserIdentity createUser,
                     JobExecutionConfiguration jobConfig,
                     Long createTimeMs,
                     String executeSql) {
        super(getNextId(), jobName, jobStatus, dbName, comment, createUser,
                jobConfig, createTimeMs, executeSql);
        this.dbId = ConnectContext.get().getCurrentDbId();
    }

    public InsertJob(ConnectContext ctx,
                     StmtExecutor executor,
                     String labelName,
                     List<InsertIntoTableCommand> plans,
                     Set<String> sinkTableNames,
                     Map<String, String> properties,
                     String comment,
                     JobExecutionConfiguration jobConfig) {
        super(getNextId(), labelName, JobStatus.RUNNING, null,
                comment, ctx.getCurrentUserIdentity(), jobConfig);
        this.ctx = ctx;
        this.plans = plans;
        this.stmtExecutor = executor;
        this.dbId = ctx.getCurrentDbId();
        this.labelName = labelName;
        this.tableNames = sinkTableNames;
        this.properties = properties;
        // TODO: not support other type yet
        this.loadType = InsertJob.LoadType.BULK;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        if (null == plans) {
            plans = new ArrayList<>();
        }
        if (null == commitInfos) {
            commitInfos = new ArrayList<>();
        }
        if (null == loadStatistic) {
            loadStatistic = new LoadStatistic();
        }
        if (null == historyTaskIdList) {
            historyTaskIdList = new ConcurrentLinkedQueue<>();
        }
        if (null == historyTaskIdList) {
            historyTaskIdList = new ConcurrentLinkedQueue<>();
        }
    }

    protected void checkJobParamsInternal() {
        if (plans.isEmpty() && StringUtils.isBlank(getExecuteSql())) {
            throw new IllegalArgumentException("command or sql is null,must be set");
        }
        if (!plans.isEmpty() && !getJobConfig().getExecuteType().equals(JobExecuteType.INSTANT)) {
            throw new IllegalArgumentException("command must be null when executeType is not instant");
        }
    }

    @Override
    public List<InsertTask> queryTasks() {
        if (historyTaskIdList.isEmpty() && insertTaskQueue.isEmpty()) {
            return new ArrayList<>();
        }

        //TODO it's will be refactor, we will storage task info in job inner and query from it

        // merge task info from insertTaskQueue and historyTaskIdList
        List<Long> taskIds = insertTaskQueue.stream().map(InsertTask::getTaskId).collect(Collectors.toList());
        taskIds.addAll(historyTaskIdList);
        taskIds.stream().distinct().collect(Collectors.toList());
        if (getJobConfig().getExecuteType().equals(JobExecuteType.INSTANT)) {
            return queryLoadTasksByTaskIds(taskIds);
        }
        // query from load job
        List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager().queryLoadJobsByJobIds(taskIds);

        Map<Long, LoadJob> loadJobMap = loadJobs.stream().collect(Collectors.toMap(LoadJob::getId, loadJob -> loadJob));
        List<InsertTask> tasksRsp = new ArrayList<>();
        //read task info from insertTaskQueue
        insertTaskQueue.forEach(task -> {
            if (task.getJobInfo() == null) {
                LoadJob loadJob = loadJobMap.get(task.getTaskId());
                if (loadJob != null) {
                    task.setJobInfo(loadJob);
                }
            }
            tasksRsp.add(task);
        });
        if (CollectionUtils.isEmpty(historyTaskIdList)) {
            return tasksRsp;
        }

        historyTaskIdList.forEach(historyTaskId -> {
            LoadJob loadJob = loadJobMap.get(historyTaskId);
            if (null == loadJob) {
                return;
            }
            InsertTask task = new InsertTask(loadJob.getLabel(), getCurrentDbName(), getExecuteSql(), getCreateUser());
            task.setJobId(getJobId());
            task.setTaskId(loadJob.getId());
            task.setJobInfo(loadJob);
            task.setJobId(getJobId());
            task.setTaskId(loadJob.getId());
            task.setJobInfo(loadJob);
            tasksRsp.add(task);
        });
        return tasksRsp;


    }

    public List<InsertTask> queryLoadTasksByTaskIds(List<Long> taskIdList) {
        if (taskIdList.isEmpty()) {
            return new ArrayList<>();
        }
        List<InsertTask> queryTasks = new ArrayList<>();
        insertTaskQueue.forEach(task -> {
            if (taskIdList.contains(task.getTaskId())) {
                queryTasks.add(task);
            }
        });
        return queryTasks;
    }

    @Override
    public JobType getJobType() {
        return JobType.INSERT;
    }

    @Override
    public List<InsertTask> createTasks(TaskType taskType, Map<Object, Object> taskContext, Long groupId) {
        List<InsertTask> newTasks = new ArrayList<>();
        if (plans.isEmpty()) {
            InsertTask task = new InsertTask(labelName, getCurrentDbName(), getExecuteSql(), getCreateUser());
            newTasks.add(task);
        } else {
            // use for load stmt
            for (InsertIntoTableCommand logicalPlan : plans) {
                if (!logicalPlan.getLabelName().isPresent()) {
                    throw new IllegalArgumentException("Load plan need label name.");
                }
                InsertTask task = new InsertTask(logicalPlan, ctx, stmtExecutor, loadStatistic);
                newTasks.add(task);
            }
        }
        initTasks(newTasks, taskType, groupId);
        recordTasks(newTasks);
        return new ArrayList<>(newTasks);
    }


    @Override
    public ShowResultSetMetaData getJobMetaData() {
        return super.getJobMetaData();
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return TASK_META_DATA;
    }

    @Override
    public void onTaskFail(InsertTask task) throws JobException {
        if (getJobConfig().getExecuteType().equals(JobExecuteType.INSTANT)) {
            this.failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, task.getErrMsg());
        }
        super.onTaskFail(task);
    }

    @Override
    public List<String> getShowInfo() {
        try {
            // check auth
            checkAuth("SHOW LOAD");
            List<String> jobInfo = Lists.newArrayList();
            // jobId
            jobInfo.add(getJobId().toString());
            // label
            if (StringUtils.isEmpty(labelName)) {
                jobInfo.add(FeConstants.null_string);
            } else {
                jobInfo.add(labelName);
            }
            // state
            if (getJobStatus() == JobStatus.STOPPED) {
                jobInfo.add("CANCELLED");
            } else {
                jobInfo.add(getJobStatus().name());
            }

            // progress
            String progress = Env.getCurrentProgressManager().getProgressInfo(String.valueOf(getJobId()));
            switch (getJobStatus()) {
                case RUNNING:
                    if (isPending()) {
                        jobInfo.add("ETL:0%; LOAD:0%");
                    } else {
                        jobInfo.add("ETL:100%; LOAD:" + progress + "%");
                    }
                    break;
                case FINISHED:
                    jobInfo.add("ETL:100%; LOAD:100%");
                    break;
                case STOPPED:
                default:
                    jobInfo.add("ETL:N/A; LOAD:N/A");
                    break;
            }
            // type
            jobInfo.add(loadType.name());

            // etl info
            if (loadStatistic.getCounters().size() == 0) {
                jobInfo.add(FeConstants.null_string);
            } else {
                jobInfo.add(Joiner.on("; ").withKeyValueSeparator("=").join(loadStatistic.getCounters()));
            }

            // task info
            jobInfo.add("cluster:" + getResourceName() + "; timeout(s):" + getTimeout()
                    + "; max_filter_ratio:" + getMaxFilterRatio() + "; priority:" + getPriority());
            // error msg
            if (failMsg == null) {
                jobInfo.add(FeConstants.null_string);
            } else {
                jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
            }

            // create time
            jobInfo.add(TimeUtils.longToTimeString(getCreateTimeMs()));
            // etl start time
            jobInfo.add(TimeUtils.longToTimeString(getStartTimeMs()));
            // etl end time
            jobInfo.add(TimeUtils.longToTimeString(getStartTimeMs()));
            // load start time
            jobInfo.add(TimeUtils.longToTimeString(getStartTimeMs()));
            // load end time
            jobInfo.add(TimeUtils.longToTimeString(getFinishTimeMs()));
            // tracking urls
            List<String> trackingUrl = insertTaskQueue.stream()
                    .map(task -> {
                        if (StringUtils.isNotEmpty(task.getTrackingUrl())) {
                            return task.getTrackingUrl();
                        } else {
                            return FeConstants.null_string;
                        }
                    })
                    .collect(Collectors.toList());
            if (trackingUrl.isEmpty()) {
                jobInfo.add(FeConstants.null_string);
            } else {
                jobInfo.add(trackingUrl.toString());
            }
            // job details
            jobInfo.add(loadStatistic.toJson());
            // transaction id
            jobInfo.add(String.valueOf(0));
            // error tablets
            jobInfo.add(errorTabletsToJson());
            // user, some load job may not have user info
            if (getCreateUser() == null || getCreateUser().getQualifiedUser() == null) {
                jobInfo.add(FeConstants.null_string);
            } else {
                jobInfo.add(getCreateUser().getQualifiedUser());
            }
            // comment
            jobInfo.add(getComment());
            return jobInfo;
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    private String getPriority() {
        return properties.getOrDefault(LoadStmt.PRIORITY, Priority.NORMAL.name());
    }

    public double getMaxFilterRatio() {
        return Double.parseDouble(properties.getOrDefault(LoadStmt.MAX_FILTER_RATIO_PROPERTY, "0.0"));
    }

    public long getTimeout() {
        if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
            return Long.parseLong(properties.get(LoadStmt.TIMEOUT_PROPERTY));
        }
        return Config.broker_load_default_timeout_second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public String errorTabletsToJson() {
        Map<Long, String> map = new HashMap<>();
        errorTabletInfos.stream().limit(Config.max_error_tablet_of_broker_load)
                .forEach(p -> map.put(p.getTabletId(), p.getMsg()));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(map);
    }

    public void updateLoadingStatus(Long beId, TUniqueId loadId, TUniqueId fragmentId, long scannedRows,
                                    long scannedBytes, boolean isDone) {
        loadStatistic.updateLoadProgress(beId, loadId, fragmentId, scannedRows, scannedBytes, isDone);
        progress = (int) ((double) finishedTaskIds.size() / insertTaskQueue.size() * 100);
        if (progress == 100) {
            progress = 99;
        }
    }

    private void checkAuth(String command) throws DdlException {
        if (authorizationInfo == null) {
            // use the old method to check priv
            checkAuthWithoutAuthInfo(command);
            return;
        }
        if (!Env.getCurrentEnv().getAccessManager().checkPrivByAuthInfo(ConnectContext.get(), authorizationInfo,
                PrivPredicate.LOAD)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    Privilege.LOAD_PRIV);
        }
    }

    /**
     * This method is compatible with old load job without authorization info
     * If db or table name could not be found by id, it will throw the NOT_EXISTS_ERROR
     *
     * @throws DdlException
     */
    private void checkAuthWithoutAuthInfo(String command) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
        // check auth
        if (tableNames == null || tableNames.isEmpty()) {
            // forward compatibility
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                            PrivPredicate.LOAD)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        Privilege.LOAD_PRIV);
            }
        } else {
            for (String tblName : tableNames) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                                tblName, PrivPredicate.LOAD)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                            command,
                            ConnectContext.get().getQualifiedUser(),
                            ConnectContext.get().getRemoteIP(), db.getFullName() + ": " + tblName);
                }
            }
        }
    }

    public void unprotectReadEndOperation(InsertJob replayLog) {
        setJobStatus(replayLog.getJobStatus());
        progress = replayLog.getProgress();
        setStartTimeMs(replayLog.getStartTimeMs());
        setFinishTimeMs(replayLog.getFinishTimeMs());
        failMsg = replayLog.failMsg;
    }

    @Override
    public void onReplayEnd(AbstractJob replayJob) throws JobException {
        if (!(replayJob instanceof InsertJob)) {
            return;
        }
        AbstractJob insertJob = replayJob;
        unprotectReadEndOperation((InsertJob) insertJob);
        log.info(new LogBuilder(LogKey.LOAD_JOB,
                insertJob.getJobId()).add("operation", insertJob).add("msg", "replay end load job").build());
    }

    public String getResourceName() {
        // TODO: get tvf param from tvf relation
        return "N/A";
    }

    public boolean isRunning() {
        return getJobStatus() != JobStatus.FINISHED;
    }

    public boolean isPending() {
        return getJobStatus() != JobStatus.FINISHED;
    }

    public boolean isCancelled() {
        return getJobStatus() == JobStatus.STOPPED;
    }

    @Override
    public void onRegister() throws JobException {
        try {
            if (StringUtils.isNotEmpty(labelName)) {
                Env.getCurrentEnv().getLabelProcessor().addJob(this);
            }
        } catch (LabelAlreadyUsedException e) {
            throw new JobException(e);
        }
    }

    @Override
    public void onUnRegister() throws JobException {
        // TODO: need record cancelled jobs in order to show cancelled job
        // Env.getCurrentEnv().getLabelProcessor().removeJob(getDbId(), getLabelName());
    }

    public void recordTasks(List<InsertTask> tasks) {
        if (Config.max_persistence_task_count < 1) {
            return;
        }
        insertTaskQueue.addAll(tasks);

        while (insertTaskQueue.size() > Config.max_persistence_task_count) {
            insertTaskQueue.poll();
            //since we have insertTaskQueue, we do not need to store the task id in historyTaskIdList, so we clear it
            historyTaskIdList.clear();
        }
        Env.getCurrentEnv().getEditLog().logUpdateJob(this);
    }

    @Override
    public void cancelAllTasks() throws JobException {
        try {
            checkAuth("CANCEL LOAD");
            super.cancelAllTasks();
            this.failMsg = new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel");
        } catch (DdlException e) {
            throw new JobException(e);
        }
    }

    public int getProgress() {
        return progress;
    }
}
