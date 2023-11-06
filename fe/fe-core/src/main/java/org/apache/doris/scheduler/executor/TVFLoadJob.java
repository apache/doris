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

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.jobs.load.InsertLoadTask;
import org.apache.doris.nereids.jobs.load.replay.ReplayLoadLog;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.ErrorTabletInfo;
import org.apache.doris.transaction.TabletCommitInfo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * we use this executor to execute sql job
 *
 */
@Slf4j
@Getter
public class TVFLoadJob extends AbstractJob<TvfLoadTask> {
    private final Long jobId;
    protected String labelName;
    protected JobState state;
    protected LoadType loadType;
    protected long transactionId;
    // 0: the job status is pending
    // n/100: n is the number of task which has been finished
    // 99: all tasks have been finished
    // 100: txn status is visible and load has been finished
    protected int progress;
    protected long createTimestamp = System.currentTimeMillis();
    protected long startTimestamp = -1;
    protected long finishTimestamp = -1;
    protected FailMsg failMsg;
    protected LoadStatistic loadStatistic = new LoadStatistic();
    protected List<TvfLoadTask> jobTasks = new ArrayList<>();

    protected List<ErrorTabletInfo> errorTabletInfos = new ArrayList<>();
    protected List<TabletCommitInfo> commitInfos = new ArrayList<>();
    protected Set<Long> finishedTaskIds = new HashSet<>();
    protected Set<String> tableNames;
    protected UserIdentity userInfo = UserIdentity.UNKNOWN;
    protected ConcurrentHashMap<Long, TvfLoadTask> idToTasks = new ConcurrentHashMap<>();
    protected String comment;
    protected Map<String, String> properties;

    public TVFLoadJob(ReplayLoadLog replayLoadLog) {
        jobId = replayLoadLog.getId();
    }

    public LoadType getType() {
        return loadType;
    }

    public boolean isTxnDone() {
        return state == JobState.COMMITTED || state == JobState.FINISHED || state == JobState.CANCELLED;
    }

    public void cancelJob(FailMsg userCancel) {

    }

    @Override
    protected void checkJobParamsInternal() {

    }

    @Override
    public List createTasks(TaskType taskType) {
        state = JobState.LOADING;
        return null;
    }

    @Override
    public boolean isReadyForScheduling() {
        return state == JobState.LOADING;
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return null;
    }

    @Override
    public JobType getJobType() {
        return JobType.LOAD;
    }

    @Override
    public List<TvfLoadTask> queryTasks() {
        return new ArrayList<>(idToTasks.values());
    }

    /**
     * load job type
     */
    public enum LoadType {
        BULK,
        SPARK,
        LOCAL_FILE,
        UNKNOWN
    }

    public TVFLoadJob(ConnectContext ctx,
                      StmtExecutor executor,
                      String labelName,
                      List<InsertIntoTableCommand> plans,
                      Set<String> sinkTableNames,
                      Map<String, String> properties,
                      String comment) {
        this.jobId = Env.getCurrentEnv().getNextId();
        this.labelName = labelName;
        this.state = JobState.PENDING;
        for (InsertIntoTableCommand logicalPlan : plans) {
            InsertLoadTask task = new InsertLoadTask(labelName, logicalPlan, loadStatistic);
            TvfLoadTask jobTask = new TvfLoadTask(ctx, executor, task);
            idToTasks.put(task.getId(), jobTask);
        }
        this.userInfo = ctx.getCurrentUserIdentity();
        this.tableNames = sinkTableNames;
        this.properties = initJobProperties(properties);
        this.comment = comment;
        // TODO: not support other type yet
        this.loadType = LoadType.BULK;
    }

    private Map<String, String> initJobProperties(Map<String, String> properties) {
        return properties;
    }

    public Set<String> getTableNames() {
        return tableNames;
    }

    public Long getId() {
        return jobId;
    }

    public String getLabel() {
        return labelName;
    }

    public String getProgress() {
        return String.valueOf(progress);
    }

    public void unprotectReadEndOperation(ReplayLoadLog.ReplayEndLoadLog replayLoadLog) {
        state = replayLoadLog.getLoadingState();
        progress = replayLoadLog.getProgress();
        startTimestamp = replayLoadLog.getStartTimestamp();
        finishTimestamp = replayLoadLog.getFinishTimestamp();
        failMsg = replayLoadLog.getFailMsg();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, TVFLoadJob.class);
        Text.writeString(out, json);
    }

    public List<Comparable> getDetail() {
        List<Comparable> jobInfo = new ArrayList<>();

        // jobId
        jobInfo.add(String.valueOf(jobId));
        // label
        jobInfo.add(labelName);
        // state
        jobInfo.add(state.name());

        // progress
        switch (state) {
            case PENDING:
                jobInfo.add("ETL:0%; LOAD:0%");
                break;
            case ETL:
                jobInfo.add("ETL:" + progress + "%; LOAD:0%");
                break;
            case LOADING:
                jobInfo.add("ETL:100%; LOAD:" + progress + "%");
                break;
            case COMMITTED:
                jobInfo.add("ETL:100%; LOAD:99%");
                break;
            case FINISHED:
                jobInfo.add("ETL:100%; LOAD:100%");
                break;
            case CANCELLED:
            default:
                jobInfo.add("ETL:N/A; LOAD:N/A");
                break;
        }

        // type
        jobInfo.add(loadType.name());

        // etl info
        if (state == JobState.CANCELLED) {
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
        if (state == JobState.CANCELLED) {
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
        jobInfo.add(transactionId);
        // error tablets(not used for hadoop load, just return an empty string)
        jobInfo.add(errorTabletsToJson());
        // user
        jobInfo.add(userInfo.getQualifiedUser());
        // comment
        jobInfo.add(comment);
        return jobInfo;
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

}
