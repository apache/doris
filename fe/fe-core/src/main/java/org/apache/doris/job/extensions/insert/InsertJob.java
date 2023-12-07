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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
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
import org.apache.doris.nereids.jobs.load.replay.ReplayLoadLog;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.ErrorTabletInfo;
import org.apache.doris.transaction.TabletCommitInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class InsertJob extends AbstractJob<InsertTask> {

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("Id", ScalarType.createStringType()),
            new Column("Name", ScalarType.createStringType()),
            new Column("Definer", ScalarType.createStringType()),
            new Column("ExecuteType", ScalarType.createStringType()),
            new Column("RecurringStrategy", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("ExecuteSql", ScalarType.createStringType()),
            new Column("CreateTime", ScalarType.createStringType()),
            new Column("Comment", ScalarType.createStringType()));

    private static final ShowResultSetMetaData TASK_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TaskId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Label", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                    .addColumn(new Column("EtlInfo", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TaskInfo", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ErrorMsg", ScalarType.createVarchar(20)))

                    .addColumn(new Column("CreateTimeMs", ScalarType.createVarchar(20)))
                    .addColumn(new Column("FinishTimeMs", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TrackingUrl", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LoadStatistic", ScalarType.createVarchar(20)))
                    .addColumn(new Column("User", ScalarType.createVarchar(20)))
                    .build();

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    @SerializedName("taskIdList")
    ConcurrentLinkedQueue<Long> taskIdList;

    private final long dbId;
    private String labelName;
    private List<InsertIntoTableCommand> plans;
    private InsertJob.LoadType loadType;
    // 0: the job status is pending
    // n/100: n is the number of task which has been finished
    // 99: all tasks have been finished
    // 100: txn status is visible and load has been finished
    private int progress;
    private long createTimestamp = System.currentTimeMillis();
    private long startTimestamp = -1;
    private long finishTimestamp = -1;
    private FailMsg failMsg;
    private LoadStatistic loadStatistic = new LoadStatistic();
    private Set<Long> finishedTaskIds = new HashSet<>();
    private Set<String> tableNames;
    private ConcurrentHashMap<Long, InsertTask> idToTasks = new ConcurrentHashMap<>();
    private Map<String, String> properties;
    private AuthorizationInfo authorizationInfo;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private ConnectContext ctx;
    private StmtExecutor stmtExecutor;
    private List<ErrorTabletInfo> errorTabletInfos = new ArrayList<>();
    private List<TabletCommitInfo> commitInfos = new ArrayList<>();

    // max save task num, do we need to config it?
    private static final int MAX_SAVE_TASK_NUM = 50;

    /**
     * load job type
     */
    public enum LoadType {
        BULK,
        SPARK,
        LOCAL_FILE,
        UNKNOWN

    }

    public InsertJob(ReplayLoadLog.ReplayCreateLoadLog replayLoadLog) {
        super(replayLoadLog.getId());
        setJobId(replayLoadLog.getId());
        this.dbId = replayLoadLog.getDbId();
    }

    public InsertJob(Long jobId, String jobName,
                     JobStatus jobStatus,
                     String currentDbName,
                     String comment,
                     UserIdentity createUser,
                     JobExecutionConfiguration jobConfig,
                     Long createTimeMs,
                     String executeSql) {
        super(jobId, jobName, jobStatus, currentDbName, comment, createUser,
                jobConfig, createTimeMs, executeSql, null);
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
        super(Env.getCurrentEnv().getNextId(), labelName, JobStatus.RUNNING, null,
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
    public List<InsertTask> createTasks(TaskType taskType) {
        if (plans.isEmpty()) {
            InsertTask task = new InsertTask(labelName, getCurrentDbName(), getExecuteSql(), getCreateUser());
            task.setJobId(getJobId());
            task.setTaskType(taskType);
            task.setTaskId(Env.getCurrentEnv().getNextId());
            ArrayList<InsertTask> tasks = new ArrayList<>();
            tasks.add(task);
            super.initTasks(tasks);
            addNewTask(task.getTaskId());
            return tasks;
        } else {
            return createBatchTasks(taskType);
        }
    }

    private List<InsertTask> createBatchTasks(TaskType taskType) {
        ArrayList<InsertTask> tasks = new ArrayList<>();
        for (InsertIntoTableCommand logicalPlan : plans) {
            InsertTask task = new InsertTask(logicalPlan, ctx, stmtExecutor, loadStatistic);
            task.setJobId(getJobId());
            task.setTaskType(taskType);
            idToTasks.put(task.getTaskId(), task);
            initTasks(tasks);
        }
        return new ArrayList<>(idToTasks.values());
    }

    public void addNewTask(long id) {

        if (CollectionUtils.isEmpty(taskIdList)) {
            taskIdList = new ConcurrentLinkedQueue<>();
            Env.getCurrentEnv().getEditLog().logUpdateJob(this);
            taskIdList.add(id);
            return;
        }
        taskIdList.add(id);
        if (taskIdList.size() >= MAX_SAVE_TASK_NUM) {
            taskIdList.poll();
        }
        Env.getCurrentEnv().getEditLog().logUpdateJob(this);
    }

    @Override
    public void cancelTaskById(long taskId) throws JobException {
        super.cancelTaskById(taskId);
    }

    public void cancelJob(FailMsg failMsg) throws DdlException {
        writeLock();
        try {
            checkAuth("CANCEL LOAD");
            cancelAllTasks();
            logFinalOperation();
            this.failMsg = failMsg;
        } catch (JobException e) {
            throw new RuntimeException(e);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void cancelAllTasks() throws JobException {
        super.cancelAllTasks();
    }

    @Override
    public boolean isReadyForScheduling() {
        return true;
    }


    @Override
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
        if (CollectionUtils.isEmpty(taskIdList)) {
            return new ArrayList<>();
        }
        //TODO it's will be refactor, we will storage task info in job inner and query from it
        List<Long> taskIdList = new ArrayList<>(this.taskIdList);
        Collections.reverse(taskIdList);
        List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager().queryLoadJobsByJobIds(taskIdList);
        if (CollectionUtils.isEmpty(loadJobs)) {
            return new ArrayList<>();
        }
        List<InsertTask> tasks = new ArrayList<>();
        loadJobs.forEach(loadJob -> {
            InsertTask task;
            try {
                task = new InsertTask(loadJob.getLabel(), loadJob.getDb().getFullName(), null, getCreateUser());
            } catch (MetaNotFoundException e) {
                log.warn("load job not found,job id is {}", loadJob.getId());
                return;
            }
            task.setJobId(getJobId());
            task.setTaskId(loadJob.getId());
            task.setLoadJob(loadJob);
            tasks.add(task);
        });
        return tasks;
    }

    @Override
    public JobType getJobType() {
        return JobType.INSERT;
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
    public void onTaskFail(InsertTask task) {
        getRunningTasks().remove(task);
    }

    @Override
    public void onTaskSuccess(InsertTask task) {
        super.onTaskSuccess(task);
    }

    @Override
    public List<String> getShowInfo() {
        List<String> jobInfo = new ArrayList<>();

        // jobId
        jobInfo.add(String.valueOf(getJobId()));
        // label
        jobInfo.add(labelName);
        // state
        jobInfo.add(getJobStatus().name());

        // progress
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
        if (isCancelled()) {
            jobInfo.add(FeConstants.null_string);
        } else {
            //            Map<String, String> counters = status.getCounters();
            List<String> info = new ArrayList<>();
            //            for (String key : counters.keySet()) {
            //                // XXX: internal etl job return all counters
            //                if (key.equalsIgnoreCase("HDFS bytes read")
            //                        || key.equalsIgnoreCase("Map input records")
            //                        || key.startsWith("dpp.")) {
            //                    info.add(key + "=" + counters.get(key));
            //                }
            //            } // end for counters
            if (info.isEmpty()) {
                jobInfo.add(FeConstants.null_string);
            } else {
                jobInfo.add(StringUtils.join(info, "; "));
            }
        }

        // task info
        jobInfo.add("cluster:" + "N/A"
                + "; timeout(s):" + properties.get(LoadStmt.TIMEOUT_PROPERTY)
                + "; max_filter_ratio:" + properties.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY));

        // error msg
        if (isCancelled()) {
            jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
        } else {
            jobInfo.add(FeConstants.null_string);
        }

        // create time
        jobInfo.add(TimeUtils.longToTimeString(createTimestamp));
        // Deprecated. etl end time
        jobInfo.add(TimeUtils.longToTimeString(startTimestamp));
        // Deprecated. etl end time
        jobInfo.add(TimeUtils.longToTimeString(startTimestamp));
        // load start time
        jobInfo.add(TimeUtils.longToTimeString(startTimestamp));
        // load end time
        jobInfo.add(TimeUtils.longToTimeString(finishTimestamp));
        // tracking url
        // jobInfo.add(status.getTrackingUrl());
        // job detail(not used for hadoop load, just return an empty string)
        jobInfo.add(loadStatistic.toJson());
        // transaction id
        // jobInfo.add(String.valueOf(transactionId));
        // error tablets(not used for hadoop load, just return an empty string)
        jobInfo.add(errorTabletsToJson());
        // user
        jobInfo.add(getCreateUser().getQualifiedUser());
        // comment
        jobInfo.add(getComment());
        return jobInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public String getLabel() {
        return labelName;
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
        progress = (int) ((double) finishedTaskIds.size() / idToTasks.size() * 100);
        if (progress == 100) {
            progress = 99;
        }
    }

    /**
     * This method will cancel job without edit log and lock
     */
    protected void logFinalOperation() {
        Env.getCurrentEnv().getEditLog().logLoadEnd(ReplayLoadLog.logEndLoadOperation(this));
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
        if (tableNames.isEmpty()) {
            // forward compatibility
            if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), db.getFullName(),
                    PrivPredicate.LOAD)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        Privilege.LOAD_PRIV);
            }
        } else {
            for (String tblName : tableNames) {
                if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), db.getFullName(),
                        tblName, PrivPredicate.LOAD)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                            command,
                            ConnectContext.get().getQualifiedUser(),
                            ConnectContext.get().getRemoteIP(), db.getFullName() + ": " + tblName);
                }
            }
        }
    }

    public void unprotectReadEndOperation(ReplayLoadLog.ReplayEndLoadLog replayLoadLog) {
        setJobStatus(replayLoadLog.getLoadingStatus());
        progress = replayLoadLog.getProgress();
        startTimestamp = replayLoadLog.getStartTimestamp();
        finishTimestamp = replayLoadLog.getFinishTimestamp();
        failMsg = replayLoadLog.getFailMsg();
    }

    public boolean isRunning() {
        return getJobStatus() != JobStatus.FINISHED;
    }

    public boolean isPending() {
        return getJobStatus() != JobStatus.FINISHED;
    }

    public boolean isCommitted() {
        return getJobStatus() != JobStatus.FINISHED;
    }

    public boolean isFinished() {
        return getJobStatus() == JobStatus.FINISHED;
    }

    public boolean isCancelled() {
        return getJobStatus() == JobStatus.STOPPED;
    }

    public int getProgress() {
        return progress;
    }

    protected void readLock() {
        lock.readLock().lock();
    }

    protected void readUnlock() {
        lock.readLock().unlock();
    }

    protected void writeLock() {
        lock.writeLock().lock();
    }

    protected void writeUnlock() {
        lock.writeLock().unlock();
    }
}
