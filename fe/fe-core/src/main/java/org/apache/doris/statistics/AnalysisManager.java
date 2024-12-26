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

package org.apache.doris.statistics;

import org.apache.doris.analysis.AnalyzeDBStmt;
import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.analysis.AnalyzeTblStmt;
import org.apache.doris.analysis.DropAnalyzeJobStmt;
import org.apache.doris.analysis.DropCachedStatsStmt;
import org.apache.doris.analysis.DropStatsStmt;
import org.apache.doris.analysis.KillAnalysisJobStmt;
import org.apache.doris.analysis.ShowAnalyzeStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.ThreadPoolManager.BlockedPolicy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.AnalyzeDeletionLog;
import org.apache.doris.persist.TableStatsDeletionLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMode;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TInvalidateFollowerStatsCacheRequest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.CronExpression;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AnalysisManager implements Writable {

    private static final Logger LOG = LogManager.getLogger(AnalysisManager.class);

    // Tracking running manually submitted async tasks, keep in mem only
    protected final ConcurrentMap<Long, Map<Long, BaseAnalysisTask>> analysisJobIdToTaskMap = new ConcurrentHashMap<>();

    private StatisticsCache statisticsCache;

    private AnalysisTaskExecutor taskExecutor;
    private ThreadPoolExecutor dropStatsExecutors;

    // Store task information in metadata.
    protected final NavigableMap<Long, AnalysisInfo> analysisTaskInfoMap =
            Collections.synchronizedNavigableMap(new TreeMap<>());

    // Store job information in metadata.
    protected final NavigableMap<Long, AnalysisInfo> analysisJobInfoMap =
            Collections.synchronizedNavigableMap(new TreeMap<>());

    // Tracking and control sync analyze tasks, keep in mem only
    private final ConcurrentMap<ConnectContext, SyncTaskCollection> ctxToSyncTask = new ConcurrentHashMap<>();

    private final Map<Long, TableStatsMeta> idToTblStats = new ConcurrentHashMap<>();

    private final Map<Long, AnalysisJob> idToAnalysisJob = new ConcurrentHashMap<>();

    private final String progressDisplayTemplate = "%d Finished  |  %d Failed  |  %d In Progress  |  %d Total";

    public AnalysisManager() {
        if (!Env.isCheckpointThread()) {
            this.taskExecutor = new AnalysisTaskExecutor(Config.statistics_simultaneously_running_task_num,
                    Integer.MAX_VALUE, "Manual Analysis Job Executor");
            this.statisticsCache = new StatisticsCache();
            this.dropStatsExecutors = ThreadPoolManager.newDaemonThreadPool(
                    1, 3, 10,
                    TimeUnit.DAYS, new LinkedBlockingQueue<>(20),
                    new ThreadPoolExecutor.DiscardPolicy(),
                    "Drop stats executor", true);
        }
    }

    public StatisticsCache getStatisticsCache() {
        return statisticsCache;
    }

    public void createAnalyze(AnalyzeStmt analyzeStmt, boolean proxy)
            throws DdlException, AnalysisException, ExecutionException, InterruptedException {
        if (!StatisticsUtil.statsTblAvailable() && !FeConstants.runningUnitTest) {
            throw new DdlException("Stats table not available, please make sure your cluster status is normal");
        }
        if (analyzeStmt instanceof AnalyzeDBStmt) {
            createAnalysisJobs((AnalyzeDBStmt) analyzeStmt, proxy);
        } else if (analyzeStmt instanceof AnalyzeTblStmt) {
            createAnalysisJob((AnalyzeTblStmt) analyzeStmt, proxy);
        }
    }

    public void createAnalysisJobs(AnalyzeDBStmt analyzeDBStmt, boolean proxy) throws DdlException, AnalysisException {
        DatabaseIf<TableIf> db = analyzeDBStmt.getDb();
        if (analyzeDBStmt.getAnalyzeProperties().getProperties().containsKey("use.auto.analyzer")) {
            throw new DdlException("Analyze database doesn't support use.auto.analyzer property.");
        }
        List<AnalysisInfo> analysisInfos = buildAnalysisInfosForDB(db, analyzeDBStmt.getAnalyzeProperties());
        if (!analyzeDBStmt.isSync()) {
            sendJobId(analysisInfos, proxy);
        }
    }

    public List<AnalysisInfo> buildAnalysisInfosForDB(DatabaseIf<TableIf> db, AnalyzeProperties analyzeProperties)
            throws AnalysisException {
        db.readLock();
        List<TableIf> tbls = db.getTables();
        List<AnalysisInfo> analysisInfos = new ArrayList<>();
        try {
            List<AnalyzeTblStmt> analyzeStmts = new ArrayList<>();
            for (TableIf table : tbls) {
                if (table instanceof View) {
                    continue;
                }
                TableName tableName = new TableName(db.getCatalog().getName(), db.getFullName(),
                        table.getName());
                // columnNames null means to add all visitable columns.
                // Will get all the visible columns in analyzeTblStmt.check()
                AnalyzeTblStmt analyzeTblStmt = new AnalyzeTblStmt(analyzeProperties, tableName,
                        null, db.getId(), table);
                try {
                    analyzeTblStmt.check();
                } catch (AnalysisException analysisException) {
                    LOG.warn("Failed to build analyze job: {}",
                            analysisException.getMessage(), analysisException);
                }
                analyzeStmts.add(analyzeTblStmt);
            }
            for (AnalyzeTblStmt analyzeTblStmt : analyzeStmts) {
                try {
                    analysisInfos.add(buildAndAssignJob(analyzeTblStmt));
                } catch (DdlException e) {
                    LOG.warn("Failed to build analyze job: {}",
                            e.getMessage(), e);
                }
            }
        } finally {
            db.readUnlock();
        }
        return analysisInfos;
    }

    // Each analyze stmt corresponding to an analysis job.
    public void createAnalysisJob(AnalyzeTblStmt stmt, boolean proxy)
            throws DdlException, AnalysisException, ExecutionException, InterruptedException {
        // Using auto analyzer if user specifies.
        if (stmt.getAnalyzeProperties().getProperties().containsKey("use.auto.analyzer")) {
            StatisticsAutoCollector autoCollector = Env.getCurrentEnv().getStatisticsAutoCollector();
            autoCollector.processOneJob(stmt.getTable(), JobPriority.MANUAL_AUTO);
            return;
        }
        AnalysisInfo jobInfo = buildAndAssignJob(stmt);
        if (jobInfo == null) {
            return;
        }
        sendJobId(ImmutableList.of(jobInfo), proxy);
    }

    @Nullable
    @VisibleForTesting
    protected AnalysisInfo buildAndAssignJob(AnalyzeTblStmt stmt) throws DdlException {
        AnalysisInfo jobInfo = buildAnalysisJobInfo(stmt);
        if (jobInfo.jobColumns.isEmpty()) {
            // No statistics need to be collected or updated
            return null;
        }
        // Only OlapTable and Hive HMSExternalTable support sample analyze.
        if ((stmt.getSamplePercent() > 0 || stmt.getSampleRows() > 0) && !canSample(stmt.getTable())) {
            String message = String.format("Table %s doesn't support sample analyze.", stmt.getTable().getName());
            LOG.info(message);
            throw new DdlException(message);
        }

        boolean isSync = stmt.isSync();
        Map<Long, BaseAnalysisTask> analysisTaskInfos = new HashMap<>();
        createTaskForEachColumns(jobInfo, analysisTaskInfos, isSync);
        if (!jobInfo.partitionOnly && stmt.isAllColumns()
                && StatisticsUtil.isExternalTable(jobInfo.catalogId, jobInfo.dbId, jobInfo.tblId)) {
            createTableLevelTaskForExternalTable(jobInfo, analysisTaskInfos, isSync);
        }
        constructJob(jobInfo, analysisTaskInfos.values());
        if (isSync) {
            syncExecute(analysisTaskInfos.values());
            jobInfo.state = AnalysisState.FINISHED;
            updateTableStats(jobInfo);
            return null;
        }
        recordAnalysisJob(jobInfo);
        analysisJobIdToTaskMap.put(jobInfo.jobId, analysisTaskInfos);
        if (!jobInfo.scheduleType.equals(ScheduleType.PERIOD)) {
            analysisTaskInfos.values().forEach(taskExecutor::submitTask);
        }
        return jobInfo;
    }

    private void sendJobId(List<AnalysisInfo> analysisInfos, boolean proxy) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("Job_Id", ScalarType.createVarchar(19)));
        columns.add(new Column("Catalog_Name", ScalarType.createVarchar(1024)));
        columns.add(new Column("DB_Name", ScalarType.createVarchar(1024)));
        columns.add(new Column("Table_Name", ScalarType.createVarchar(1024)));
        columns.add(new Column("Columns", ScalarType.createVarchar(1024)));
        ShowResultSetMetaData commonResultSetMetaData = new ShowResultSetMetaData(columns);
        List<List<String>> resultRows = new ArrayList<>();
        for (AnalysisInfo analysisInfo : analysisInfos) {
            if (analysisInfo == null) {
                continue;
            }
            List<String> row = new ArrayList<>();
            row.add(String.valueOf(analysisInfo.jobId));
            CatalogIf<? extends DatabaseIf<? extends TableIf>> c = StatisticsUtil.findCatalog(analysisInfo.catalogId);
            row.add(c.getName());
            Optional<? extends DatabaseIf<? extends TableIf>> databaseIf = c.getDb(analysisInfo.dbId);
            row.add(databaseIf.isPresent() ? databaseIf.get().getFullName() : "DB may get deleted");
            if (databaseIf.isPresent()) {
                Optional<? extends TableIf> table = databaseIf.get().getTable(analysisInfo.tblId);
                row.add(table.isPresent() ? table.get().getName() : "Table may get deleted");
            } else {
                row.add("DB not exists anymore");
            }
            row.add(analysisInfo.colName);
            resultRows.add(row);
        }
        ShowResultSet commonResultSet = new ShowResultSet(commonResultSetMetaData, resultRows);
        try {
            if (!proxy) {
                ConnectContext.get().getExecutor().sendResultSet(commonResultSet);
            } else {
                ConnectContext.get().getExecutor().setProxyShowResultSet(commonResultSet);
            }
        } catch (Throwable t) {
            LOG.warn("Failed to send job id to user", t);
        }
    }

    // Make sure colName of job has all the column as this AnalyzeStmt specified, no matter whether it will be analyzed
    // or not.
    @VisibleForTesting
    public AnalysisInfo buildAnalysisJobInfo(AnalyzeTblStmt stmt) throws DdlException {
        AnalysisInfoBuilder infoBuilder = new AnalysisInfoBuilder();
        long jobId = Env.getCurrentEnv().getNextId();
        TableIf table = stmt.getTable();
        Set<String> columnNames = stmt.getColumnNames();
        Set<String> partitionNames = stmt.getPartitionNames();
        boolean partitionOnly = stmt.isPartitionOnly();
        boolean isSamplingPartition = stmt.isSamplingPartition();
        boolean isAllPartition = stmt.isStarPartition();
        long partitionCount = stmt.getPartitionCount();
        int samplePercent = stmt.getSamplePercent();
        int sampleRows = stmt.getSampleRows();
        AnalysisType analysisType = stmt.getAnalysisType();
        AnalysisMode analysisMode = stmt.getAnalysisMode();
        AnalysisMethod analysisMethod = stmt.getAnalysisMethod();
        ScheduleType scheduleType = stmt.getScheduleType();
        CronExpression cronExpression = stmt.getCron();

        infoBuilder.setJobId(jobId);
        infoBuilder.setCatalogId(stmt.getCatalogId());
        infoBuilder.setDBId(stmt.getDbId());
        infoBuilder.setTblId(stmt.getTable().getId());
        infoBuilder.setPartitionNames(partitionNames);
        infoBuilder.setPartitionOnly(partitionOnly);
        infoBuilder.setSamplingPartition(isSamplingPartition);
        infoBuilder.setAllPartition(isAllPartition);
        infoBuilder.setPartitionCount(partitionCount);
        infoBuilder.setJobType(JobType.MANUAL);
        infoBuilder.setState(AnalysisState.PENDING);
        infoBuilder.setLastExecTimeInMs(System.currentTimeMillis());
        infoBuilder.setAnalysisType(analysisType);
        infoBuilder.setAnalysisMode(analysisMode);
        infoBuilder.setAnalysisMethod(analysisMethod);
        infoBuilder.setScheduleType(scheduleType);
        infoBuilder.setCronExpression(cronExpression);
        infoBuilder.setForceFull(stmt.forceFull());
        infoBuilder.setUsingSqlForPartitionColumn(stmt.usingSqlForPartitionColumn());
        if (analysisMethod == AnalysisMethod.SAMPLE) {
            infoBuilder.setSamplePercent(samplePercent);
            infoBuilder.setSampleRows(sampleRows);
        }

        if (analysisType == AnalysisType.HISTOGRAM) {
            int numBuckets = stmt.getNumBuckets();
            int maxBucketNum = numBuckets > 0 ? numBuckets : StatisticConstants.HISTOGRAM_MAX_BUCKET_NUM;
            infoBuilder.setMaxBucketNum(maxBucketNum);
        }

        long periodTimeInMs = stmt.getPeriodTimeInMs();
        infoBuilder.setPeriodTimeInMs(periodTimeInMs);
        List<Pair<String, String>> jobColumns = table.getColumnIndexPairs(columnNames);
        infoBuilder.setJobColumns(jobColumns);
        StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
        for (Pair<String, String> pair : jobColumns) {
            stringJoiner.add(pair.toString());
        }
        infoBuilder.setColName(stringJoiner.toString());
        infoBuilder.setTaskIds(Lists.newArrayList());
        infoBuilder.setTblUpdateTime(table.getUpdateTime());
        infoBuilder.setEmptyJob(table instanceof OlapTable && table.getRowCount() == 0
                && analysisMethod.equals(AnalysisMethod.SAMPLE));
        long rowCount = StatisticsUtil.isEmptyTable(table, analysisMethod) ? 0 : table.getRowCount();
        infoBuilder.setRowCount(rowCount);
        infoBuilder.setPriority(JobPriority.MANUAL);
        infoBuilder.setTableVersion(table instanceof OlapTable ? ((OlapTable) table).getVisibleVersion() : 0);
        return infoBuilder.build();
    }

    @VisibleForTesting
    public void recordAnalysisJob(AnalysisInfo jobInfo) {
        if (jobInfo.scheduleType == ScheduleType.PERIOD && jobInfo.lastExecTimeInMs > 0) {
            return;
        }
        AnalysisInfoBuilder jobInfoBuilder = new AnalysisInfoBuilder(jobInfo);
        AnalysisInfo analysisInfo = jobInfoBuilder.setTaskId(-1).build();
        replayCreateAnalysisJob(analysisInfo);
    }

    public void createTaskForEachColumns(AnalysisInfo jobInfo, Map<Long, BaseAnalysisTask> analysisTasks,
            boolean isSync) throws DdlException {
        List<Pair<String, String>> jobColumns = jobInfo.jobColumns;
        TableIf table = jobInfo.getTable();
        for (Pair<String, String> pair : jobColumns) {
            AnalysisInfoBuilder colTaskInfoBuilder = new AnalysisInfoBuilder(jobInfo);
            colTaskInfoBuilder.setAnalysisType(AnalysisType.FUNDAMENTALS);
            long taskId = Env.getCurrentEnv().getNextId();
            long indexId = -1;
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                indexId = olapTable.getIndexIdByName(pair.first);
                if (indexId == olapTable.getBaseIndexId()) {
                    indexId = -1;
                }
            }
            AnalysisInfo analysisInfo = colTaskInfoBuilder.setColName(pair.second).setIndexId(indexId)
                    .setTaskId(taskId).setLastExecTimeInMs(System.currentTimeMillis()).build();
            analysisTasks.put(taskId, createTask(analysisInfo));
            jobInfo.addTaskId(taskId);
            if (isSync) {
                continue;
            }
            replayCreateAnalysisTask(analysisInfo);
        }
    }

    // Change to public for unit test.
    public void logCreateAnalysisTask(AnalysisInfo analysisInfo) {
        replayCreateAnalysisTask(analysisInfo);
        Env.getCurrentEnv().getEditLog().logCreateAnalysisTasks(analysisInfo);
    }

    // Change to public for unit test.
    public void logCreateAnalysisJob(AnalysisInfo analysisJob) {
        replayCreateAnalysisJob(analysisJob);
        Env.getCurrentEnv().getEditLog().logCreateAnalysisJob(analysisJob);
    }

    @VisibleForTesting
    public void createTableLevelTaskForExternalTable(AnalysisInfo jobInfo,
            Map<Long, BaseAnalysisTask> analysisTasks,
            boolean isSync) throws DdlException {

        if (jobInfo.analysisType == AnalysisType.HISTOGRAM) {
            return;
        }
        AnalysisInfoBuilder colTaskInfoBuilder = new AnalysisInfoBuilder(jobInfo);
        long taskId = Env.getCurrentEnv().getNextId();
        AnalysisInfo analysisInfo = colTaskInfoBuilder.setIndexId(-1L).setLastExecTimeInMs(System.currentTimeMillis())
                .setTaskId(taskId).setColName("TableRowCount").setExternalTableLevelTask(true).build();
        analysisTasks.put(taskId, createTask(analysisInfo));
        jobInfo.addTaskId(taskId);
        if (isSync) {
            // For sync job, don't need to persist, return here and execute it immediately.
            return;
        }
        replayCreateAnalysisTask(analysisInfo);
    }

    public void updateTaskStatus(AnalysisInfo info, AnalysisState taskState, String message, long time) {
        if (analysisJobIdToTaskMap.get(info.jobId) == null) {
            return;
        }
        info.state = taskState;
        info.message = message;
        // Update the task cost time when task finished or failed. And only log the final state.
        if (taskState.equals(AnalysisState.FINISHED) || taskState.equals(AnalysisState.FAILED)) {
            info.timeCostInMs = time - info.lastExecTimeInMs;
            info.lastExecTimeInMs = time;
            // Persist task info for manual job.
            if (info.jobType.equals(JobType.MANUAL)) {
                logCreateAnalysisTask(info);
            } else {
                replayCreateAnalysisTask(info);
            }
        }
        info.lastExecTimeInMs = time;
        AnalysisInfo job = analysisJobInfoMap.get(info.jobId);
        // Job may get deleted during execution.
        if (job == null) {
            return;
        }
        // Synchronize the job state change in job level.
        synchronized (job) {
            job.lastExecTimeInMs = time;
            if (taskState.equals(AnalysisState.FAILED)) {
                String errMessage = String.format("%s:[%s] ", info.colName, message);
                job.message = job.message == null ? errMessage : job.message + errMessage;
            }
            // Set the job state to RUNNING when its first task becomes RUNNING.
            if (info.state.equals(AnalysisState.RUNNING) && job.state.equals(AnalysisState.PENDING)) {
                job.state = AnalysisState.RUNNING;
                job.markStartTime(System.currentTimeMillis());
                replayCreateAnalysisJob(job);
            }
            boolean allFinished = true;
            boolean hasFailure = false;
            for (BaseAnalysisTask task : analysisJobIdToTaskMap.get(info.jobId).values()) {
                AnalysisInfo taskInfo = task.info;
                if (taskInfo.state.equals(AnalysisState.RUNNING) || taskInfo.state.equals(AnalysisState.PENDING)) {
                    allFinished = false;
                    break;
                }
                if (taskInfo.state.equals(AnalysisState.FAILED)) {
                    hasFailure = true;
                }
            }
            if (allFinished) {
                if (hasFailure) {
                    job.markFailed();
                } else {
                    job.markFinished();
                    try {
                        updateTableStats(job);
                    } catch (Throwable e) {
                        LOG.warn("Failed to update Table statistics in job: {}", info.toString(), e);
                    }
                }
                logCreateAnalysisJob(job);
                analysisJobIdToTaskMap.remove(job.jobId);
            }
        }
    }

    @VisibleForTesting
    public void updateTableStats(AnalysisInfo jobInfo) {
        TableIf tbl = StatisticsUtil.findTable(jobInfo.catalogId, jobInfo.dbId, jobInfo.tblId);
        // External Table only update table stats when all tasks finished.
        // Because it needs to get the row count from the result of row count task.
        if (tbl instanceof ExternalTable && !jobInfo.state.equals(AnalysisState.FINISHED)) {
            return;
        }
        TableStatsMeta tableStats = findTableStatsStatus(tbl.getId());
        if (tableStats == null) {
            updateTableStatsStatus(new TableStatsMeta(jobInfo.emptyJob ? 0 : tbl.getRowCount(), jobInfo, tbl));
        } else {
            tableStats.update(jobInfo, tbl);
            logCreateTableStats(tableStats);
        }
        if (jobInfo.jobColumns != null) {
            jobInfo.jobColumns.clear();
        }
        if (jobInfo.partitionNames != null) {
            jobInfo.partitionNames.clear();
        }
    }

    @VisibleForTesting
    public void updateTableStatsForAlterStats(AnalysisInfo jobInfo, TableIf tbl) {
        TableStatsMeta tableStats = findTableStatsStatus(tbl.getId());
        if (tableStats == null) {
            updateTableStatsStatus(new TableStatsMeta(0, jobInfo, tbl));
        } else {
            tableStats.update(jobInfo, tbl);
            logCreateTableStats(tableStats);
        }
    }

    public List<AnalysisInfo> showAnalysisJob(ShowAnalyzeStmt stmt) {
        return findShowAnalyzeResult(stmt);
    }

    private List<AnalysisInfo> findShowAnalyzeResult(ShowAnalyzeStmt stmt) {
        String state = stmt.getStateValue();
        TableName tblName = stmt.getDbTableName();
        TableIf tbl = null;
        if (tblName != null) {
            tbl = StatisticsUtil.findTable(tblName.getCtl(), tblName.getDb(), tblName.getTbl());
        }
        long tblId = tbl == null ? -1 : tbl.getId();
        synchronized (analysisJobInfoMap) {
            return analysisJobInfoMap.values().stream()
                .filter(a -> stmt.getJobId() == 0 || a.jobId == stmt.getJobId())
                .filter(a -> state == null || a.state.equals(AnalysisState.valueOf(state)))
                .filter(a -> tblName == null || a.tblId == tblId)
                .filter(a -> stmt.isAuto() && a.jobType.equals(JobType.SYSTEM)
                    || !stmt.isAuto() && a.jobType.equals(JobType.MANUAL))
                .sorted(Comparator.comparingLong(a -> a.jobId))
                .collect(Collectors.toList());
        }
    }

    public String getJobProgress(long jobId) {
        List<AnalysisInfo> tasks = findTasksByTaskIds(jobId);
        if (tasks == null) {
            return "N/A";
        }
        int finished = 0;
        int failed = 0;
        int inProgress = 0;
        int total = tasks.size();
        for (AnalysisInfo info : tasks) {
            switch (info.state) {
                case FINISHED:
                    finished++;
                    break;
                case FAILED:
                    failed++;
                    break;
                default:
                    inProgress++;
                    break;
            }
        }
        return String.format(progressDisplayTemplate, finished, failed, inProgress, total);
    }

    @VisibleForTesting
    public void syncExecute(Collection<BaseAnalysisTask> tasks) {
        SyncTaskCollection syncTaskCollection = new SyncTaskCollection(tasks);
        ConnectContext ctx = ConnectContext.get();
        ThreadPoolExecutor syncExecPool = createThreadPoolForSyncAnalyze();
        try {
            ctxToSyncTask.put(ctx, syncTaskCollection);
            syncTaskCollection.execute(syncExecPool);
        } finally {
            syncExecPool.shutdown();
            ctxToSyncTask.remove(ctx);
        }
    }

    private ThreadPoolExecutor createThreadPoolForSyncAnalyze() {
        String poolName = "SYNC ANALYZE THREAD POOL";
        return new ThreadPoolExecutor(0,
                ConnectContext.get().getSessionVariable().parallelSyncAnalyzeTaskNum,
                ThreadPoolManager.KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SYNC ANALYZE" + "-%d")
                        .build(), new BlockedPolicy(poolName,
                StatisticsUtil.getAnalyzeTimeout()));
    }

    public void dropCachedStats(DropCachedStatsStmt stmt) {
        long catalogId = stmt.getCatalogIdId();
        long dbId = stmt.getDbId();
        long tblId = stmt.getTblId();
        dropCachedStats(catalogId, dbId, tblId);
    }

    public void dropStats(DropStatsStmt dropStatsStmt) throws DdlException {
        if (dropStatsStmt.dropExpired) {
            Env.getCurrentEnv().getStatisticsCleaner().clear();
            return;
        }

        TableStatsMeta tableStats = findTableStatsStatus(dropStatsStmt.getTblId());
        if (tableStats == null) {
            return;
        }
        Set<String> cols = dropStatsStmt.getColumnNames();
        long catalogId = dropStatsStmt.getCatalogIdId();
        long dbId = dropStatsStmt.getDbId();
        long tblId = dropStatsStmt.getTblId();
        // Remove tableMetaStats if drop whole table stats.
        if (dropStatsStmt.isAllColumns()) {
            removeTableStats(tblId);
            Env.getCurrentEnv().getEditLog().logDeleteTableStats(new TableStatsDeletionLog(tblId));
        }
        invalidateLocalStats(catalogId, dbId, tblId, dropStatsStmt.isAllColumns() ? null : cols, tableStats);
        // Drop stats ddl is master only operation.
        invalidateRemoteStats(catalogId, dbId, tblId, cols, dropStatsStmt.isAllColumns());
        StatisticsRepository.dropStatisticsByColNames(catalogId, dbId, tblId, cols);
    }

    public void dropStats(TableIf table) {
        try {
            TableStatsMeta tableStats = findTableStatsStatus(table.getId());
            if (tableStats == null) {
                return;
            }
            long catalogId = table.getDatabase().getCatalog().getId();
            long dbId = table.getDatabase().getId();
            long tableId = table.getId();
            asyncDropStatsTask(table, catalogId, dbId, tableId, tableStats);
        } catch (Throwable e) {
            LOG.warn("Failed to drop stats for table {}", table.getName(), e);
        }
    }

    class DropStatsTask implements Runnable {
        private final long catalogId;
        private final long dbId;
        private final long tableId;
        private final TableStatsMeta tableStats;
        private final TableIf table;

        public DropStatsTask(TableIf table, long catalogId, long dbId, long tableId, TableStatsMeta tableStats) {
            this.catalogId = catalogId;
            this.dbId = dbId;
            this.tableId = tableId;
            this.tableStats = tableStats;
            this.table = table;
        }

        @Override
        public void run() {
            try {
                removeTableStats(tableId);
                Env.getCurrentEnv().getEditLog().logDeleteTableStats(new TableStatsDeletionLog(tableId));
                Set<String> cols = table.getSchemaAllIndexes(false).stream().map(Column::getName)
                        .collect(Collectors.toSet());
                StatisticsRepository.dropStatisticsByColNames(catalogId, dbId, table.getId(), cols);
                invalidateLocalStats(catalogId, dbId, tableId, null, tableStats);
                // Drop stats ddl is master only operation.
                invalidateRemoteStats(catalogId, dbId, tableId, cols, true);
            } catch (Throwable e) {
                LOG.warn("Failed to drop stats for table {}", table.getName(), e);
            }
        }
    }

    public void asyncDropStatsTask(TableIf table, long catalogId, long dbId, long tableId, TableStatsMeta tableStats) {
        try {
            dropStatsExecutors.submit(new DropStatsTask(table, catalogId, dbId, tableId, tableStats));
        } catch (Throwable t) {
            LOG.info("Failed to submit async drop stats job. reason: {}", t.getMessage());
        }
    }

    public void dropCachedStats(long catalogId, long dbId, long tableId) {
        TableIf table = StatisticsUtil.findTable(catalogId, dbId, tableId);
        StatisticsCache statsCache = Env.getCurrentEnv().getStatisticsCache();
        Set<String> columns = table.getSchemaAllIndexes(false)
                .stream().map(Column::getName).collect(Collectors.toSet());
        for (String column : columns) {
            List<Long> indexIds = Lists.newArrayList();
            if (table instanceof OlapTable) {
                indexIds = ((OlapTable) table).getMvColumnIndexIds(column);
            } else {
                indexIds.add(-1L);
            }
            for (long indexId : indexIds) {
                statsCache.invalidate(catalogId, dbId, tableId, indexId, column);
            }
        }
    }

    public void invalidateLocalStats(long catalogId, long dbId, long tableId,
            Set<String> columns, TableStatsMeta tableStats) {
        TableIf table = StatisticsUtil.findTable(catalogId, dbId, tableId);
        StatisticsCache statsCache = Env.getCurrentEnv().getStatisticsCache();
        if (columns == null) {
            columns = table.getSchemaAllIndexes(false)
                .stream().map(Column::getName).collect(Collectors.toSet());
        }

        for (String column : columns) {
            List<Long> indexIds = Lists.newArrayList();
            if (table instanceof OlapTable) {
                indexIds = ((OlapTable) table).getMvColumnIndexIds(column);
            } else {
                indexIds.add(-1L);
            }
            for (long indexId : indexIds) {
                String indexName = table.getName();
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    if (indexId == -1) {
                        indexName = olapTable.getIndexNameById(olapTable.getBaseIndexId());
                    } else {
                        indexName = olapTable.getIndexNameById(indexId);
                    }
                }
                if (tableStats != null) {
                    tableStats.removeColumn(indexName, column);
                }
                statsCache.invalidate(catalogId, dbId, tableId, indexId, column);
            }
        }
        if (tableStats != null) {
            tableStats.updatedTime = 0;
            tableStats.userInjected = false;
        }
    }

    public void invalidateRemoteStats(long catalogId, long dbId, long tableId,
                                      Set<String> columns, boolean isAllColumns) {
        InvalidateStatsTarget target = new InvalidateStatsTarget(catalogId, dbId, tableId, columns, isAllColumns);
        TInvalidateFollowerStatsCacheRequest request = new TInvalidateFollowerStatsCacheRequest();
        request.key = GsonUtils.GSON.toJson(target);
        StatisticsCache statisticsCache = Env.getCurrentEnv().getStatisticsCache();
        SystemInfoService.HostInfo selfNode = Env.getCurrentEnv().getSelfNode();
        for (Frontend frontend : Env.getCurrentEnv().getFrontends(null)) {
            // Skip master
            if (selfNode.getHost().equals(frontend.getHost())) {
                continue;
            }
            statisticsCache.invalidateStats(frontend, request);
        }
        TableStatsMeta tableStats = findTableStatsStatus(tableId);
        if (tableStats != null) {
            logCreateTableStats(tableStats);
        }
    }

    public void handleKillAnalyzeStmt(KillAnalysisJobStmt killAnalysisJobStmt) throws DdlException {
        Map<Long, BaseAnalysisTask> analysisTaskMap = analysisJobIdToTaskMap.remove(killAnalysisJobStmt.jobId);
        if (analysisTaskMap == null) {
            throw new DdlException("Job not exists or already finished");
        }
        BaseAnalysisTask anyTask = analysisTaskMap.values().stream().findFirst().orElse(null);
        if (anyTask == null) {
            return;
        }
        checkPriv(anyTask);
        logKilled(analysisJobInfoMap.get(anyTask.getJobId()));
        for (BaseAnalysisTask taskInfo : analysisTaskMap.values()) {
            taskInfo.cancel();
            logKilled(taskInfo.info);
        }
    }

    private void logKilled(AnalysisInfo info) {
        info.state = AnalysisState.FAILED;
        info.message = "Killed by user: " + ConnectContext.get().getQualifiedUser();
        info.lastExecTimeInMs = System.currentTimeMillis();
        Env.getCurrentEnv().getEditLog().logCreateAnalysisTasks(info);
    }

    private void checkPriv(BaseAnalysisTask analysisTask) {
        checkPriv(analysisTask.info);
    }

    private void checkPriv(AnalysisInfo analysisInfo) {
        DBObjects dbObjects = StatisticsUtil.convertIdToObjects(analysisInfo.catalogId,
                analysisInfo.dbId, analysisInfo.tblId);
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), dbObjects.catalog.getName(), dbObjects.db.getFullName(),
                        dbObjects.table.getName(), PrivPredicate.SELECT)) {
            throw new RuntimeException("You need at least SELECT PRIV to corresponding table to kill this analyze"
                    + " job");
        }
    }

    public void cancelSyncTask(ConnectContext connectContext) {
        SyncTaskCollection syncTaskCollection = ctxToSyncTask.get(connectContext);
        if (syncTaskCollection != null) {
            syncTaskCollection.cancel();
        }
    }

    private BaseAnalysisTask createTask(AnalysisInfo analysisInfo) throws DdlException {
        try {
            TableIf table = StatisticsUtil.findTable(analysisInfo.catalogId,
                    analysisInfo.dbId, analysisInfo.tblId);
            return table.createAnalysisTask(analysisInfo);
        } catch (Throwable t) {
            LOG.warn("Failed to find table", t);
            throw new DdlException("Failed to create task", t);
        }
    }

    public void replayCreateAnalysisJob(AnalysisInfo jobInfo) {
        synchronized (analysisJobInfoMap) {
            while (analysisJobInfoMap.size() >= Config.analyze_record_limit) {
                analysisJobInfoMap.remove(analysisJobInfoMap.pollFirstEntry().getKey());
            }
            if (jobInfo.message != null && jobInfo.message.length() >= StatisticConstants.MSG_LEN_UPPER_BOUND) {
                jobInfo.message = jobInfo.message.substring(0, StatisticConstants.MSG_LEN_UPPER_BOUND);
            }
            this.analysisJobInfoMap.put(jobInfo.jobId, jobInfo);
        }
    }

    public void replayCreateAnalysisTask(AnalysisInfo taskInfo) {
        synchronized (analysisTaskInfoMap) {
            while (analysisTaskInfoMap.size() >= Config.analyze_record_limit) {
                analysisTaskInfoMap.remove(analysisTaskInfoMap.pollFirstEntry().getKey());
            }
            if (taskInfo.message != null && taskInfo.message.length() >= StatisticConstants.MSG_LEN_UPPER_BOUND) {
                taskInfo.message = taskInfo.message.substring(0, StatisticConstants.MSG_LEN_UPPER_BOUND);
            }
            this.analysisTaskInfoMap.put(taskInfo.taskId, taskInfo);
        }
    }

    public void replayDeleteAnalysisJob(AnalyzeDeletionLog log) {
        synchronized (analysisJobInfoMap) {
            this.analysisJobInfoMap.remove(log.id);
        }
    }

    public void replayDeleteAnalysisTask(AnalyzeDeletionLog log) {
        synchronized (analysisTaskInfoMap) {
            this.analysisTaskInfoMap.remove(log.id);
        }
    }

    private static class SyncTaskCollection {
        public volatile boolean cancelled;

        public final Collection<BaseAnalysisTask> tasks;

        public SyncTaskCollection(Collection<BaseAnalysisTask> tasks) {
            this.tasks = tasks;
        }

        public void cancel() {
            cancelled = true;
            tasks.forEach(BaseAnalysisTask::cancel);
        }

        public void execute(ThreadPoolExecutor executor) {
            List<String> colNames = Collections.synchronizedList(new ArrayList<>());
            List<String> errorMessages = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch countDownLatch = new CountDownLatch(tasks.size());
            for (BaseAnalysisTask task : tasks) {
                executor.submit(() -> {
                    try {
                        if (cancelled) {
                            errorMessages.add("Query Timeout or user Cancelled."
                                    + "Could set analyze_timeout to a bigger value.");
                            return;
                        }
                        try {
                            task.execute();
                        } catch (Throwable t) {
                            colNames.add(task.info.colName);
                            errorMessages.add(Util.getRootCauseMessage(t));
                            LOG.warn("Failed to analyze, info: {}", task, t);
                        }
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException t) {
                LOG.warn("Thread got interrupted when waiting sync analyze task execution finished", t);
            }
            if (!colNames.isEmpty()) {
                if (cancelled) {
                    throw new RuntimeException("User Cancelled or Timeout.");
                }
                throw new RuntimeException("Failed to analyze following columns:[" + String.join(",", colNames)
                        + "] Reasons: " + String.join(",", errorMessages));
            }
        }
    }

    public List<AnalysisInfo> findTasks(long jobId) {
        synchronized (analysisTaskInfoMap) {
            return analysisTaskInfoMap.values().stream().filter(i -> i.jobId == jobId).collect(Collectors.toList());
        }
    }

    public List<AnalysisInfo> findTasksByTaskIds(long jobId) {
        AnalysisInfo jobInfo = analysisJobInfoMap.get(jobId);
        if (jobInfo != null && jobInfo.taskIds != null) {
            return jobInfo.taskIds.stream().map(analysisTaskInfoMap::get).filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        return null;
    }

    public void removeAll(List<AnalysisInfo> analysisInfos) {
        synchronized (analysisTaskInfoMap) {
            for (AnalysisInfo analysisInfo : analysisInfos) {
                analysisTaskInfoMap.remove(analysisInfo.taskId);
            }
        }
    }

    public void dropAnalyzeJob(DropAnalyzeJobStmt analyzeJobStmt) throws DdlException {
        AnalysisInfo jobInfo = analysisJobInfoMap.get(analyzeJobStmt.getJobId());
        if (jobInfo == null) {
            throw new DdlException(String.format("Analyze job [%d] not exists", analyzeJobStmt.getJobId()));
        }
        checkPriv(jobInfo);
        long jobId = analyzeJobStmt.getJobId();
        AnalyzeDeletionLog analyzeDeletionLog = new AnalyzeDeletionLog(jobId);
        Env.getCurrentEnv().getEditLog().logDeleteAnalysisJob(analyzeDeletionLog);
        replayDeleteAnalysisJob(analyzeDeletionLog);
        removeAll(findTasks(jobId));
    }

    public static AnalysisManager readFields(DataInput in) throws IOException {
        AnalysisManager analysisManager = new AnalysisManager();
        readAnalysisInfo(in, analysisManager.analysisJobInfoMap, true);
        readAnalysisInfo(in, analysisManager.analysisTaskInfoMap, false);
        readIdToTblStats(in, analysisManager.idToTblStats);
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_128) {
            readAutoJobs(in, analysisManager);
        }
        return analysisManager;
    }

    private static void readAnalysisInfo(DataInput in, Map<Long, AnalysisInfo> map, boolean job) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            // AnalysisInfo is compatible with AnalysisJobInfo and AnalysisTaskInfo.
            AnalysisInfo analysisInfo = AnalysisInfo.read(in);
            // Unfinished manual once job/tasks doesn't need to keep in memory anymore.
            if (needAbandon(analysisInfo)) {
                continue;
            }
            map.put(job ? analysisInfo.jobId : analysisInfo.taskId, analysisInfo);
        }
    }

    // Need to abandon the unfinished manual once jobs/tasks while loading image and replay journal.
    // Journal only store finished tasks and jobs.
    public static boolean needAbandon(AnalysisInfo analysisInfo) {
        if (analysisInfo == null) {
            return true;
        }
        if (analysisInfo.scheduleType == null || analysisInfo.jobType == null) {
            return true;
        }
        return (AnalysisState.PENDING.equals(analysisInfo.state) || AnalysisState.RUNNING.equals(analysisInfo.state))
                && ScheduleType.ONCE.equals(analysisInfo.scheduleType)
                && JobType.MANUAL.equals(analysisInfo.jobType);
    }

    private static void readIdToTblStats(DataInput in, Map<Long, TableStatsMeta> map) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableStatsMeta tableStats = TableStatsMeta.read(in);
            map.put(tableStats.tblId, tableStats);
        }
    }

    // To be deprecated, keep it for meta compatibility now, will remove later.
    private static void readAutoJobs(DataInput in, AnalysisManager analysisManager) throws IOException {
        Type type = new TypeToken<LinkedList<AnalysisInfo>>() {}.getType();
        GsonUtils.GSON.fromJson(Text.readString(in), type);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        synchronized (analysisJobInfoMap) {
            writeJobInfo(out, analysisJobInfoMap);
        }
        synchronized (analysisTaskInfoMap) {
            writeJobInfo(out, analysisTaskInfoMap);
        }
        writeTableStats(out);
    }

    private void writeJobInfo(DataOutput out, Map<Long, AnalysisInfo> infoMap) throws IOException {
        out.writeInt(infoMap.size());
        for (Entry<Long, AnalysisInfo> entry : infoMap.entrySet()) {
            entry.getValue().write(out);
        }
    }

    private void writeTableStats(DataOutput out) throws IOException {
        synchronized (idToTblStats) {
            out.writeInt(idToTblStats.size());
            for (Entry<Long, TableStatsMeta> entry : idToTblStats.entrySet()) {
                entry.getValue().write(out);
            }
        }
    }

    // For unit test use only.
    public void addToJobIdTasksMap(long jobId, Map<Long, BaseAnalysisTask> tasks) {
        analysisJobIdToTaskMap.put(jobId, tasks);
    }

    public TableStatsMeta findTableStatsStatus(long tblId) {
        return idToTblStats.get(tblId);
    }

    // Invoke this when load transaction finished.
    public void updateUpdatedRows(Map<Long, Long> records) {
        if (!Env.getCurrentEnv().isMaster() || Env.isCheckpointThread() || records == null || records.isEmpty()) {
            return;
        }
        for (Entry<Long, Long> record : records.entrySet()) {
            TableStatsMeta statsStatus = idToTblStats.get(record.getKey());
            if (statsStatus != null) {
                statsStatus.updatedRows.addAndGet(record.getValue());
            }
        }
        logUpdateRowsRecord(new UpdateRowsEvent(records));
    }

    // Set to true means new partition loaded data
    public void setNewPartitionLoaded(List<Long> tableIds) {
        if (!Env.getCurrentEnv().isMaster() || Env.isCheckpointThread() || tableIds == null || tableIds.isEmpty()) {
            return;
        }
        for (long tableId : tableIds) {
            TableStatsMeta statsStatus = idToTblStats.get(tableId);
            if (statsStatus != null) {
                statsStatus.newPartitionLoaded.set(true);
            }
        }
        logNewPartitionLoadedEvent(new NewPartitionLoadedEvent(tableIds));
    }

    public void updateTableStatsStatus(TableStatsMeta tableStats) {
        replayUpdateTableStatsStatus(tableStats);
        logCreateTableStats(tableStats);
    }

    public void replayUpdateTableStatsStatus(TableStatsMeta tableStats) {
        synchronized (idToTblStats) {
            idToTblStats.put(tableStats.tblId, tableStats);
        }
    }

    public void logCreateTableStats(TableStatsMeta tableStats) {
        Env.getCurrentEnv().getEditLog().logCreateTableStats(tableStats);
    }

    public void logUpdateRowsRecord(UpdateRowsEvent record) {
        Env.getCurrentEnv().getEditLog().logUpdateRowsRecord(record);
    }

    public void logNewPartitionLoadedEvent(NewPartitionLoadedEvent event) {
        Env.getCurrentEnv().getEditLog().logNewPartitionLoadedEvent(event);
    }

    public void replayUpdateRowsRecord(UpdateRowsEvent event) {
        if (event == null || event.getRecords() == null) {
            return;
        }
        for (Entry<Long, Long> record : event.getRecords().entrySet()) {
            TableStatsMeta statsStatus = idToTblStats.get(record.getKey());
            if (statsStatus != null) {
                statsStatus.updatedRows.addAndGet(record.getValue());
            }
        }
    }

    public void replayNewPartitionLoadedEvent(NewPartitionLoadedEvent event) {
        if (event == null || event.getTableIds() == null) {
            return;
        }
        for (long tableId : event.getTableIds()) {
            TableStatsMeta statsStatus = idToTblStats.get(tableId);
            if (statsStatus != null) {
                statsStatus.newPartitionLoaded.set(true);
            }
        }
    }

    public void registerSysJob(AnalysisInfo jobInfo, Map<Long, BaseAnalysisTask> taskInfos) {
        recordAnalysisJob(jobInfo);
        analysisJobIdToTaskMap.put(jobInfo.jobId, taskInfos);
    }

    public void removeTableStats(long tableId) {
        synchronized (idToTblStats) {
            idToTblStats.remove(tableId);
        }
    }

    public Set<Long> getIdToTblStatsKeys() {
        return new HashSet<>(idToTblStats.keySet());
    }

    public ColStatsMeta findColStatsMeta(long tblId, String indexName, String colName) {
        TableStatsMeta tableStats = findTableStatsStatus(tblId);
        if (tableStats == null) {
            return null;
        }
        return tableStats.findColumnStatsMeta(indexName, colName);
    }

    public AnalysisJob findJob(long id) {
        return idToAnalysisJob.get(id);
    }

    public AnalysisInfo findJobInfo(long id) {
        return analysisJobInfoMap.get(id);
    }

    public void constructJob(AnalysisInfo jobInfo, Collection<? extends BaseAnalysisTask> tasks) {
        AnalysisJob job = new AnalysisJob(jobInfo, tasks);
        idToAnalysisJob.put(jobInfo.jobId, job);
    }

    public void removeJob(long id) {
        idToAnalysisJob.remove(id);
    }

    /**
     * Only OlapTable and Hive HMSExternalTable can sample for now.
     * @param table Table to check
     * @return Return true if the given table can do sample analyze. False otherwise.
     */
    public boolean canSample(TableIf table) {
        if (table instanceof OlapTable) {
            return true;
        }
        return table instanceof HMSExternalTable
                && ((HMSExternalTable) table).getDlaType().equals(HMSExternalTable.DLAType.HIVE);
    }
}
