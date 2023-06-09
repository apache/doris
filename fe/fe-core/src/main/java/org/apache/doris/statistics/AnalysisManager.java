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
import org.apache.doris.analysis.AnalyzeTblStmt;
import org.apache.doris.analysis.DropAnalyzeJobStmt;
import org.apache.doris.analysis.DropStatsStmt;
import org.apache.doris.analysis.KillAnalysisJobStmt;
import org.apache.doris.analysis.ShowAnalyzeStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.AnalyzeDeletionLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMode;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class AnalysisManager extends Daemon implements Writable {

    public AnalysisTaskScheduler taskScheduler;

    private static final Logger LOG = LogManager.getLogger(AnalysisManager.class);

    private ConcurrentMap<Long, Map<Long, BaseAnalysisTask>> analysisJobIdToTaskMap = new ConcurrentHashMap<>();

    private StatisticsCache statisticsCache;

    private AnalysisTaskExecutor taskExecutor;

    private final Map<Long, AnalysisInfo> analysisTaskInfoMap = Collections.synchronizedMap(new TreeMap<>());
    private final Map<Long, AnalysisInfo> analysisJobInfoMap = Collections.synchronizedMap(new TreeMap<>());

    private final ConcurrentMap<ConnectContext, SyncTaskCollection> ctxToSyncTask = new ConcurrentHashMap<>();

    public AnalysisManager() {
        super(TimeUnit.SECONDS.toMillis(StatisticConstants.ANALYZE_MANAGER_INTERVAL_IN_SECS));
        if (!Env.isCheckpointThread()) {
            this.taskScheduler = new AnalysisTaskScheduler();
            this.taskExecutor = new AnalysisTaskExecutor(taskScheduler);
            this.statisticsCache = new StatisticsCache();
            taskExecutor.start();
        }
    }

    @Override
    protected void runOneCycle() {
        clear();
    }

    private void clear() {
        clearMeta(analysisJobInfoMap, (a) ->
                        a.scheduleType.equals(ScheduleType.ONCE)
                                && System.currentTimeMillis() - a.lastExecTimeInMs
                                > TimeUnit.DAYS.toMillis(StatisticConstants.ANALYSIS_JOB_INFO_EXPIRATION_TIME_IN_DAYS),
                (id) -> {
                    Env.getCurrentEnv().getEditLog().logDeleteAnalysisJob(new AnalyzeDeletionLog(id));
                    return null;
                });
        clearMeta(analysisTaskInfoMap, (a) -> System.currentTimeMillis() - a.lastExecTimeInMs
                        > TimeUnit.DAYS.toMillis(StatisticConstants.ANALYSIS_JOB_INFO_EXPIRATION_TIME_IN_DAYS),
                (id) -> {
                    Env.getCurrentEnv().getEditLog().logDeleteAnalysisTask(new AnalyzeDeletionLog(id));
                    return null;
                });
    }

    private void clearMeta(Map<Long, AnalysisInfo> infoMap, Predicate<AnalysisInfo> isExpired,
            Function<Long, Void> writeLog) {
        synchronized (infoMap) {
            List<Long> expired = new ArrayList<>();
            for (Entry<Long, AnalysisInfo> entry : infoMap.entrySet()) {
                if (isExpired.test(entry.getValue())) {
                    expired.add(entry.getKey());
                }
            }
            for (Long k : expired) {
                infoMap.remove(k);
                writeLog.apply(k);
            }
        }
    }

    public StatisticsCache getStatisticsCache() {
        return statisticsCache;
    }

    public void createAnalysisJobs(AnalyzeDBStmt analyzeDBStmt) throws DdlException {
        DatabaseIf<TableIf> db = analyzeDBStmt.getDb();
        List<TableIf> tbls = db.getTables();
        List<AnalysisInfo> analysisInfos = new ArrayList<>();
        db.readLock();
        try {
            List<AnalyzeTblStmt> analyzeStmts = new ArrayList<>();
            for (TableIf table : tbls) {
                if (table instanceof View) {
                    continue;
                }
                TableName tableName = new TableName(analyzeDBStmt.getCtlIf().getName(), db.getFullName(),
                        table.getName());
                AnalyzeTblStmt analyzeTblStmt = new AnalyzeTblStmt(analyzeDBStmt.getAnalyzeProperties(), tableName,
                        table.getBaseSchema().stream().map(
                                Column::getName).collect(
                                Collectors.toList()), db.getId(), table);
                try {
                    analyzeTblStmt.check();
                } catch (AnalysisException analysisException) {
                    throw new DdlException(analysisException.getMessage(), analysisException);
                }
                analyzeStmts.add(analyzeTblStmt);
            }
            for (AnalyzeTblStmt analyzeTblStmt : analyzeStmts) {
                analysisInfos.add(buildAndAssignJob(analyzeTblStmt));
            }
            sendJobId(analysisInfos);
        } finally {
            db.readUnlock();
        }

    }

    // Each analyze stmt corresponding to an analysis job.
    public void createAnalysisJob(AnalyzeTblStmt stmt) throws DdlException {
        AnalysisInfo jobInfo = buildAndAssignJob(stmt);
        if (jobInfo == null) {
            return;
        }
        sendJobId(ImmutableList.of(jobInfo));
    }

    @Nullable
    private AnalysisInfo buildAndAssignJob(AnalyzeTblStmt stmt) throws DdlException {
        if (!StatisticsUtil.statsTblAvailable() && !FeConstants.runningUnitTest) {
            throw new DdlException("Stats table not available, please make sure your cluster status is normal");
        }

        AnalysisInfo jobInfo = buildAnalysisJobInfo(stmt);
        if (jobInfo.colToPartitions.isEmpty()) {
            // No statistics need to be collected or updated
            return null;
        }

        boolean isSync = stmt.isSync();
        Map<Long, BaseAnalysisTask> analysisTaskInfos = new HashMap<>();
        createTaskForEachColumns(jobInfo, analysisTaskInfos, isSync);
        createTaskForMVIdx(jobInfo, analysisTaskInfos, isSync);
        createTaskForExternalTable(jobInfo, analysisTaskInfos, isSync);

        if (!isSync) {
            persistAnalysisJob(jobInfo);
            analysisJobIdToTaskMap.put(jobInfo.jobId, analysisTaskInfos);
        }

        try {
            updateTableStats(jobInfo);
        } catch (Throwable e) {
            throw new DdlException("Failed to update Table statistics");
        }

        if (isSync) {
            syncExecute(analysisTaskInfos.values());
            return null;
        }

        analysisTaskInfos.values().forEach(taskScheduler::schedule);
        return jobInfo;
    }

    // Analysis job created by the system
    public void createAnalysisJob(AnalysisInfo info) throws DdlException {
        AnalysisInfo jobInfo = buildAnalysisJobInfo(info);
        if (jobInfo.colToPartitions.isEmpty()) {
            // No statistics need to be collected or updated
            return;
        }

        Map<Long, BaseAnalysisTask> analysisTaskInfos = new HashMap<>();
        createTaskForEachColumns(jobInfo, analysisTaskInfos, false);
        createTaskForMVIdx(jobInfo, analysisTaskInfos, false);

        persistAnalysisJob(jobInfo);
        analysisJobIdToTaskMap.put(jobInfo.jobId, analysisTaskInfos);

        try {
            updateTableStats(jobInfo);
        } catch (Throwable e) {
            LOG.warn("Failed to update Table statistics in job: {}", info.toString());
        }

        analysisTaskInfos.values().forEach(taskScheduler::schedule);
    }

    private void sendJobId(List<AnalysisInfo> analysisInfos) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("Catalog_Name", ScalarType.createVarchar(1024)));
        columns.add(new Column("DB_Name", ScalarType.createVarchar(1024)));
        columns.add(new Column("Table_Name", ScalarType.createVarchar(1024)));
        columns.add(new Column("Columns", ScalarType.createVarchar(1024)));
        columns.add(new Column("Job_Id", ScalarType.createVarchar(19)));
        ShowResultSetMetaData commonResultSetMetaData = new ShowResultSetMetaData(columns);
        List<List<String>> resultRows = new ArrayList<>();
        for (AnalysisInfo analysisInfo : analysisInfos) {
            List<String> row = new ArrayList<>();
            row.add(analysisInfo.catalogName);
            row.add(analysisInfo.dbName);
            row.add(analysisInfo.tblName);
            row.add(analysisInfo.colName);
            row.add(String.valueOf(analysisInfo.jobId));
            resultRows.add(row);
        }
        ShowResultSet commonResultSet = new ShowResultSet(commonResultSetMetaData, resultRows);
        try {
            ConnectContext.get().getExecutor().sendResultSet(commonResultSet);
        } catch (Throwable t) {
            LOG.warn("Failed to send job id to user", t);
        }
    }

    /**
     * Gets the partitions for which statistics are to be collected. First verify that
     * there are partitions that have been deleted but have historical statistics(invalid statistics),
     * if there are these partitions, we need to delete them to avoid errors in summary table level statistics.
     * Then get the partitions for which statistics need to be collected based on collection mode (incremental/full).
     * <p>
     * note:
     * If there is no invalid statistics, it does not need to collect/update
     * statistics if the following conditions are met:
     * - in full collection mode, the partitioned table does not have partitions
     * - in incremental collection mode, partition statistics already exist
     * <p>
     * TODO Supports incremental collection of statistics from materialized views
     */
    private Map<String, Set<String>> validateAndGetPartitions(TableIf table, Set<String> columnNames,
            AnalysisType analysisType, AnalysisMode analysisMode) throws DdlException {
        long tableId = table.getId();
        Set<String> partitionNames = table.getPartitionNames();

        Map<String, Set<String>> columnToPartitions = columnNames.stream()
                .collect(Collectors.toMap(
                        columnName -> columnName,
                        columnName -> new HashSet<>(partitionNames)
                ));

        if (analysisType == AnalysisType.HISTOGRAM) {
            // Collecting histograms does not need to support incremental collection,
            // and will automatically cover historical statistics
            return columnToPartitions;
        }

        // Get the partition granularity statistics that have been collected
        Map<String, Set<Long>> existColAndPartsForStats = StatisticsRepository
                .fetchColAndPartsForStats(tableId);

        if (existColAndPartsForStats.isEmpty()) {
            // There is no historical statistical information, no need to do validation
            return columnToPartitions;
        }

        Set<Long> existPartIdsForStats = new HashSet<>();
        existColAndPartsForStats.values().forEach(existPartIdsForStats::addAll);
        Map<Long, String> idToPartition = StatisticsUtil.getPartitionIdToName(table);
        // Get an invalid set of partitions (those partitions were deleted)
        Set<Long> invalidPartIds = existPartIdsForStats.stream()
                .filter(id -> !idToPartition.containsKey(id)).collect(Collectors.toSet());

        if (!invalidPartIds.isEmpty()) {
            // Delete invalid partition statistics to avoid affecting table statistics
            StatisticsRepository.dropStatistics(invalidPartIds);
        }

        if (analysisMode == AnalysisMode.INCREMENTAL && analysisType == AnalysisType.FUNDAMENTALS) {
            existColAndPartsForStats.values().forEach(partIds -> partIds.removeAll(invalidPartIds));
            // In incremental collection mode, just collect the uncollected partition statistics
            existColAndPartsForStats.forEach((columnName, partitionIds) -> {
                Set<String> existPartitions = partitionIds.stream()
                        .map(idToPartition::get)
                        .collect(Collectors.toSet());
                columnToPartitions.computeIfPresent(columnName, (colName, partNames) -> {
                    partNames.removeAll(existPartitions);
                    return partNames;
                });
            });
            if (invalidPartIds.isEmpty()) {
                // There is no invalid statistics, so there is no need to update table statistics,
                // remove columns that do not require re-collection of statistics
                columnToPartitions.entrySet().removeIf(entry -> entry.getValue().isEmpty());
            }
        }

        return columnToPartitions;
    }

    private AnalysisInfo buildAnalysisJobInfo(AnalyzeTblStmt stmt) throws DdlException {
        AnalysisInfoBuilder taskInfoBuilder = new AnalysisInfoBuilder();
        long jobId = Env.getCurrentEnv().getNextId();
        String catalogName = stmt.getCatalogName();
        String db = stmt.getDBName();
        TableName tbl = stmt.getTblName();
        StatisticsUtil.convertTableNameToObjects(tbl);
        String tblName = tbl.getTbl();
        TableIf table = stmt.getTable();
        Set<String> columnNames = stmt.getColumnNames();
        int samplePercent = stmt.getSamplePercent();
        int sampleRows = stmt.getSampleRows();
        AnalysisType analysisType = stmt.getAnalysisType();
        AnalysisMode analysisMode = stmt.getAnalysisMode();
        AnalysisMethod analysisMethod = stmt.getAnalysisMethod();
        ScheduleType scheduleType = stmt.getScheduleType();

        taskInfoBuilder.setJobId(jobId);
        taskInfoBuilder.setCatalogName(catalogName);
        taskInfoBuilder.setDbName(db);
        taskInfoBuilder.setTblName(tblName);
        StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
        for (String colName : columnNames) {
            stringJoiner.add(colName);
        }
        taskInfoBuilder.setColName(stringJoiner.toString());
        taskInfoBuilder.setJobType(JobType.MANUAL);
        taskInfoBuilder.setState(AnalysisState.PENDING);
        taskInfoBuilder.setAnalysisType(analysisType);
        taskInfoBuilder.setAnalysisMode(analysisMode);
        taskInfoBuilder.setAnalysisMethod(analysisMethod);
        taskInfoBuilder.setScheduleType(scheduleType);
        taskInfoBuilder.setLastExecTimeInMs(0);

        if (analysisMethod == AnalysisMethod.SAMPLE) {
            taskInfoBuilder.setSamplePercent(samplePercent);
            taskInfoBuilder.setSampleRows(sampleRows);
        }

        if (analysisType == AnalysisType.HISTOGRAM) {
            int numBuckets = stmt.getNumBuckets();
            int maxBucketNum = numBuckets > 0 ? numBuckets
                    : StatisticConstants.HISTOGRAM_MAX_BUCKET_NUM;
            taskInfoBuilder.setMaxBucketNum(maxBucketNum);
        }

        if (scheduleType == ScheduleType.PERIOD) {
            long periodTimeInMs = stmt.getPeriodTimeInMs();
            taskInfoBuilder.setPeriodTimeInMs(periodTimeInMs);
        }

        Map<String, Set<String>> colToPartitions = validateAndGetPartitions(table,
                columnNames, analysisType, analysisMode);
        taskInfoBuilder.setColToPartitions(colToPartitions);

        return taskInfoBuilder.build();
    }

    private AnalysisInfo buildAnalysisJobInfo(AnalysisInfo jobInfo) {
        AnalysisInfoBuilder taskInfoBuilder = new AnalysisInfoBuilder();
        taskInfoBuilder.setJobId(jobInfo.jobId);
        taskInfoBuilder.setCatalogName(jobInfo.catalogName);
        taskInfoBuilder.setDbName(jobInfo.dbName);
        taskInfoBuilder.setTblName(jobInfo.tblName);
        taskInfoBuilder.setJobType(JobType.SYSTEM);
        taskInfoBuilder.setState(AnalysisState.PENDING);
        taskInfoBuilder.setAnalysisType(jobInfo.analysisType);
        taskInfoBuilder.setAnalysisMode(jobInfo.analysisMode);
        taskInfoBuilder.setAnalysisMethod(jobInfo.analysisMethod);
        taskInfoBuilder.setScheduleType(jobInfo.scheduleType);
        taskInfoBuilder.setSamplePercent(jobInfo.samplePercent);
        taskInfoBuilder.setSampleRows(jobInfo.sampleRows);
        taskInfoBuilder.setMaxBucketNum(jobInfo.maxBucketNum);
        taskInfoBuilder.setPeriodTimeInMs(jobInfo.periodTimeInMs);
        taskInfoBuilder.setLastExecTimeInMs(jobInfo.lastExecTimeInMs);
        try {
            TableIf table = StatisticsUtil
                    .findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
            Map<String, Set<String>> colToPartitions = validateAndGetPartitions(table,
                    jobInfo.colToPartitions.keySet(), jobInfo.analysisType, jobInfo.analysisMode);
            taskInfoBuilder.setColToPartitions(colToPartitions);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return taskInfoBuilder.build();
    }

    private void persistAnalysisJob(AnalysisInfo jobInfo) throws DdlException {
        if (jobInfo.scheduleType == ScheduleType.PERIOD && jobInfo.lastExecTimeInMs > 0) {
            return;
        }
        AnalysisInfoBuilder jobInfoBuilder = new AnalysisInfoBuilder(jobInfo);
        AnalysisInfo analysisInfo = jobInfoBuilder.setTaskId(-1).build();
        logCreateAnalysisJob(analysisInfo);
    }

    private void createTaskForMVIdx(AnalysisInfo jobInfo, Map<Long, BaseAnalysisTask> analysisTasks,
            boolean isSync) throws DdlException {
        TableIf table;
        try {
            table = StatisticsUtil.findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
        } catch (Throwable e) {
            LOG.warn(e.getMessage());
            return;
        }

        TableType type = table.getType();
        if (jobInfo.analysisType != AnalysisType.INDEX || !type.equals(TableType.OLAP)) {
            // not need to collect statistics for materialized view
            return;
        }

        OlapTable olapTable = (OlapTable) table;

        try {
            olapTable.readLock();
            for (MaterializedIndexMeta meta : olapTable.getIndexIdToMeta().values()) {
                if (meta.getDefineStmt() == null) {
                    continue;
                }
                long indexId = meta.getIndexId();
                long taskId = Env.getCurrentEnv().getNextId();
                AnalysisInfoBuilder indexTaskInfoBuilder = new AnalysisInfoBuilder(jobInfo);
                AnalysisInfo analysisInfo = indexTaskInfoBuilder.setIndexId(indexId)
                        .setTaskId(taskId).build();
                if (isSync) {
                    return;
                }
                analysisTasks.put(taskId, createTask(analysisInfo));
                logCreateAnalysisJob(analysisInfo);
            }
        } finally {
            olapTable.readUnlock();
        }
    }

    private void createTaskForEachColumns(AnalysisInfo jobInfo, Map<Long, BaseAnalysisTask> analysisTasks,
            boolean isSync) throws DdlException {
        Map<String, Set<String>> columnToPartitions = jobInfo.colToPartitions;
        for (Entry<String, Set<String>> entry : columnToPartitions.entrySet()) {
            long indexId = -1;
            long taskId = Env.getCurrentEnv().getNextId();
            String colName = entry.getKey();
            AnalysisInfoBuilder colTaskInfoBuilder = new AnalysisInfoBuilder(jobInfo);
            if (jobInfo.analysisType != AnalysisType.HISTOGRAM) {
                colTaskInfoBuilder.setAnalysisType(AnalysisType.FUNDAMENTALS);
                colTaskInfoBuilder.setColToPartitions(Collections.singletonMap(colName, entry.getValue()));
            }
            AnalysisInfo analysisInfo = colTaskInfoBuilder.setColName(colName).setIndexId(indexId)
                    .setTaskId(taskId).build();
            analysisTasks.put(taskId, createTask(analysisInfo));
            if (isSync) {
                continue;
            }
            try {
                logCreateAnalysisTask(analysisInfo);
            } catch (Exception e) {
                throw new DdlException("Failed to create analysis task", e);
            }
        }
    }

    private void logCreateAnalysisTask(AnalysisInfo analysisInfo) {
        Env.getCurrentEnv().getEditLog().logCreateAnalysisTasks(analysisInfo);
        analysisTaskInfoMap.put(analysisInfo.taskId, analysisInfo);
    }

    private void logCreateAnalysisJob(AnalysisInfo analysisJob) {
        Env.getCurrentEnv().getEditLog().logCreateAnalysisJob(analysisJob);
        analysisJobInfoMap.put(analysisJob.jobId, analysisJob);
    }

    private void createTaskForExternalTable(AnalysisInfo jobInfo,
            Map<Long, BaseAnalysisTask> analysisTasks,
            boolean isSync) throws DdlException {
        TableIf table;
        try {
            table = StatisticsUtil.findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
        } catch (Throwable e) {
            LOG.warn(e.getMessage());
            return;
        }
        if (jobInfo.analysisType == AnalysisType.HISTOGRAM || table.getType() != TableType.HMS_EXTERNAL_TABLE) {
            return;
        }
        AnalysisInfoBuilder colTaskInfoBuilder = new AnalysisInfoBuilder(jobInfo);
        long taskId = Env.getCurrentEnv().getNextId();
        AnalysisInfo analysisInfo = colTaskInfoBuilder.setIndexId(-1L)
                .setTaskId(taskId).setExternalTableLevelTask(true).build();
        analysisTasks.put(taskId, createTask(analysisInfo));
        try {
            logCreateAnalysisJob(analysisInfo);
        } catch (Exception e) {
            throw new DdlException("Failed to create analysis task", e);
        }
    }

    public void updateTaskStatus(AnalysisInfo info, AnalysisState jobState, String message, long time) {
        if (analysisJobIdToTaskMap.get(info.jobId) == null) {
            return;
        }
        info.state = jobState;
        info.message = message;
        info.lastExecTimeInMs = time;
        logCreateAnalysisTask(info);

        AnalysisInfo job = analysisJobInfoMap.get(info.jobId);
        job.lastExecTimeInMs = time;
        if (info.state.equals(AnalysisState.RUNNING) && !job.state.equals(AnalysisState.PENDING)) {
            job.state = AnalysisState.RUNNING;
            Env.getCurrentEnv().getEditLog().logCreateAnalysisTasks(job);
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
                job.state = AnalysisState.FAILED;
                logCreateAnalysisJob(job);
            } else {
                job.state = AnalysisState.FINISHED;
                logCreateAnalysisJob(job);
            }
            analysisJobIdToTaskMap.remove(job.jobId);
        }
    }

    private void updateTableStats(AnalysisInfo jobInfo) throws Throwable {
        Map<String, String> params = buildTableStatsParams(jobInfo);
        TableIf tbl = StatisticsUtil.findTable(jobInfo.catalogName,
                jobInfo.dbName, jobInfo.tblName);

        // update olap table stats
        if (tbl.getType() == TableType.OLAP) {
            OlapTable table = (OlapTable) tbl;
            updateOlapTableStats(table, params);
        }

        // TODO support external table
    }

    @SuppressWarnings("rawtypes")
    private Map<String, String> buildTableStatsParams(AnalysisInfo jobInfo) throws Throwable {
        CatalogIf catalog = StatisticsUtil.findCatalog(jobInfo.catalogName);
        DatabaseIf db = StatisticsUtil.findDatabase(jobInfo.catalogName, jobInfo.dbName);
        TableIf tbl = StatisticsUtil.findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
        String indexId = String.valueOf(jobInfo.indexId);
        String id = StatisticsUtil.constructId(tbl.getId(), indexId);
        Map<String, String> commonParams = new HashMap<>();
        commonParams.put("id", id);
        commonParams.put("catalogId", String.valueOf(catalog.getId()));
        commonParams.put("dbId", String.valueOf(db.getId()));
        commonParams.put("tblId", String.valueOf(tbl.getId()));
        commonParams.put("indexId", indexId);
        commonParams.put("lastAnalyzeTimeInMs", String.valueOf(System.currentTimeMillis()));
        return commonParams;
    }

    private void updateOlapTableStats(OlapTable table, Map<String, String> params) throws Throwable {
        for (Partition partition : table.getPartitions()) {
            HashMap<String, String> partParams = Maps.newHashMap(params);
            long rowCount = partition.getBaseIndex().getRowCount();
            partParams.put("id", StatisticsUtil
                    .constructId(params.get("id"), partition.getId()));
            partParams.put("partId", String.valueOf(partition.getId()));
            partParams.put("rowCount", String.valueOf(rowCount));
            StatisticsRepository.persistTableStats(partParams);
        }

        HashMap<String, String> tblParams = Maps.newHashMap(params);
        long rowCount = table.getRowCount();
        tblParams.put("partId", "NULL");
        tblParams.put("rowCount", String.valueOf(rowCount));
        StatisticsRepository.persistTableStats(tblParams);
    }

    public List<AnalysisInfo> showAnalysisJob(ShowAnalyzeStmt stmt) {
        String state = stmt.getStateValue();
        TableName tblName = stmt.getDbTableName();
        return analysisJobInfoMap.values().stream()
                .filter(a -> stmt.getJobId() == 0 || a.jobId == stmt.getJobId())
                .filter(a -> state == null || a.state.equals(AnalysisState.valueOf(state)))
                .filter(a -> tblName == null || a.catalogName.equals(tblName.getCtl())
                        && a.dbName.equals(tblName.getDb()) && a.tblName.equals(tblName.getTbl()))
                .sorted(Comparator.comparingLong(a -> a.jobId))
                .collect(Collectors.toList());
    }

    private void syncExecute(Collection<BaseAnalysisTask> tasks) {
        SyncTaskCollection syncTaskCollection = new SyncTaskCollection(tasks);
        ConnectContext ctx = ConnectContext.get();
        try {
            ctxToSyncTask.put(ctx, syncTaskCollection);
            syncTaskCollection.execute();
        } finally {
            ctxToSyncTask.remove(ctx);
        }
    }

    public void dropStats(DropStatsStmt dropStatsStmt) throws DdlException {
        if (dropStatsStmt.dropExpired) {
            Env.getCurrentEnv().getStatisticsCleaner().clear();
            return;
        }
        Set<String> cols = dropStatsStmt.getColumnNames();
        long tblId = dropStatsStmt.getTblId();
        StatisticsRepository.dropStatistics(tblId, cols);
        for (String col : cols) {
            Env.getCurrentEnv().getStatisticsCache().invidate(tblId, -1L, col);
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
            taskInfo.markAsKilled();
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
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), analysisInfo.dbName, analysisInfo.tblName, PrivPredicate.SELECT)) {
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
            TableIf table = StatisticsUtil.findTable(analysisInfo.catalogName,
                    analysisInfo.dbName, analysisInfo.tblName);
            return table.createAnalysisTask(analysisInfo);
        } catch (Throwable t) {
            LOG.warn("Failed to find table", t);
            throw new DdlException("Failed to create task", t);
        }
    }

    public void replayCreateAnalysisJob(AnalysisInfo jobInfo) {
        this.analysisJobInfoMap.put(jobInfo.jobId, jobInfo);
    }

    public void replayCreateAnalysisTask(AnalysisInfo taskInfo) {
        this.analysisTaskInfoMap.put(taskInfo.taskId, taskInfo);
    }

    public void replayDeleteAnalysisJob(AnalyzeDeletionLog log) {
        this.analysisJobInfoMap.remove(log.id);
    }

    public void replayDeleteAnalysisTask(AnalyzeDeletionLog log) {
        this.analysisTaskInfoMap.remove(log.id);
    }

    private static class SyncTaskCollection {
        public volatile boolean cancelled;

        public final Collection<BaseAnalysisTask> tasks;

        public SyncTaskCollection(Collection<BaseAnalysisTask> tasks) {
            this.tasks = tasks;
        }

        public void cancel() {
            cancelled = true;
            tasks.forEach(BaseAnalysisTask::markAsKilled);
        }

        public void execute() {
            List<String> colNames = new ArrayList<>();
            for (BaseAnalysisTask task : tasks) {
                if (cancelled) {
                    continue;
                }
                try {
                    task.execute();
                    updateSyncTaskStatus(task, AnalysisState.FINISHED);
                } catch (Throwable t) {
                    colNames.add(task.info.colName);
                    updateSyncTaskStatus(task, AnalysisState.FAILED);
                    LOG.info("Failed to analyze, info: {}", task);
                }
            }
            if (!colNames.isEmpty()) {
                throw new RuntimeException("Failed to analyze following columns: " + String.join(",", colNames));
            }
        }

        private void updateSyncTaskStatus(BaseAnalysisTask task, AnalysisState state) {
            Env.getCurrentEnv().getAnalysisManager()
                    .updateTaskStatus(task.info, state, "", System.currentTimeMillis());
        }
    }

    public List<AnalysisInfo> findAutomaticAnalysisJobs() {
        synchronized (analysisJobInfoMap) {
            return analysisJobInfoMap.values().stream()
                    .filter(a ->
                            a.scheduleType.equals(ScheduleType.AUTOMATIC)
                                    && (!(a.state.equals(AnalysisState.RUNNING)
                                    || a.state.equals(AnalysisState.PENDING)))
                                    && System.currentTimeMillis() - a.lastExecTimeInMs
                                    > TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes))
                    .collect(Collectors.toList());
        }
    }

    public List<AnalysisInfo> findPeriodicJobs() {
        synchronized (analysisJobInfoMap) {
            return analysisJobInfoMap.values().stream()
                    .filter(a -> a.scheduleType.equals(ScheduleType.PERIOD)
                            && (a.state.equals(AnalysisState.FINISHED))
                            && System.currentTimeMillis() - a.lastExecTimeInMs > a.periodTimeInMs)
                    .collect(Collectors.toList());
        }
    }

    public List<AnalysisInfo> findTasks(long jobId) {
        synchronized (analysisTaskInfoMap) {
            return analysisTaskInfoMap.values().stream().filter(i -> i.jobId == jobId).collect(Collectors.toList());
        }
    }

    public void removeAll(List<AnalysisInfo> analysisInfos) {
        for (AnalysisInfo analysisInfo : analysisInfos) {
            analysisTaskInfoMap.remove(analysisInfo.taskId);
        }
    }

    public void dropAnalyzeJob(DropAnalyzeJobStmt analyzeJobStmt) throws DdlException {
        AnalysisInfo jobInfo = analysisJobInfoMap.get(analyzeJobStmt.getJobId());
        if (jobInfo == null) {
            throw new DdlException(String.format("Analyze job [%d] not exists", jobInfo.jobId));
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
        doRead(in, analysisManager.analysisJobInfoMap, true);
        doRead(in, analysisManager.analysisTaskInfoMap, false);
        return analysisManager;
    }

    private static void doRead(DataInput in, Map<Long, AnalysisInfo> map, boolean job) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            AnalysisInfo analysisInfo = AnalysisInfo.read(in);
            map.put(job ? analysisInfo.jobId : analysisInfo.taskId, analysisInfo);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        doWrite(out, analysisJobInfoMap);
        doWrite(out, analysisTaskInfoMap);
    }

    private void doWrite(DataOutput out, Map<Long, AnalysisInfo> infoMap) throws IOException {
        out.writeInt(infoMap.size());
        for (Entry<Long, AnalysisInfo> entry : infoMap.entrySet()) {
            entry.getValue().write(out);
        }
    }
}
