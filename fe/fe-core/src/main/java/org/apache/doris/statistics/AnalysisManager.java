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

import org.apache.doris.analysis.AnalyzeStmt;
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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMode;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class AnalysisManager {

    public final AnalysisTaskScheduler taskScheduler;

    private static final Logger LOG = LogManager.getLogger(AnalysisManager.class);

    private static final String UPDATE_JOB_STATE_SQL_TEMPLATE = "UPDATE "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_JOB_TABLE + " "
            + "SET state = '${jobState}' ${message} ${updateExecTime} "
            + "WHERE job_id = ${jobId} AND ${taskIdExpr}";

    private static final String SHOW_JOB_STATE_SQL_TEMPLATE = "SELECT "
            + "job_id, catalog_name, db_name, tbl_name, col_name, job_type, "
            + "analysis_type, message, last_exec_time_in_ms, state, schedule_type "
            + "FROM " + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_JOB_TABLE;

    // The time field that needs to be displayed
    private static final String LAST_EXEC_TIME_IN_MS = "last_exec_time_in_ms";

    private final ConcurrentMap<Long, Map<Long, BaseAnalysisTask>> analysisJobIdToTaskMap;

    private StatisticsCache statisticsCache;

    private final AnalysisTaskExecutor taskExecutor;

    private ConcurrentMap<ConnectContext, SyncTaskCollection> ctxToSyncTask = new ConcurrentHashMap<>();

    public AnalysisManager() {
        analysisJobIdToTaskMap = new ConcurrentHashMap<>();
        this.taskScheduler = new AnalysisTaskScheduler();
        taskExecutor = new AnalysisTaskExecutor(taskScheduler);
        this.statisticsCache = new StatisticsCache();
        taskExecutor.start();
    }

    public StatisticsCache getStatisticsCache() {
        return statisticsCache;
    }

    // Each analyze stmt corresponding to an analysis job.
    public void createAnalysisJob(AnalyzeStmt stmt) throws DdlException {
        if (!StatisticsUtil.statsTblAvailable() && !FeConstants.runningUnitTest) {
            throw new DdlException("Stats table not available, please make sure your cluster status is normal");
        }

        AnalysisJobInfo jobInfo = buildAnalysisJobInfo(stmt);
        if (jobInfo.colToPartitions.isEmpty()) {
            // No statistics need to be collected or updated
            return;
        }

        boolean isSync = stmt.isSync();
        Map<Long, BaseAnalysisTask> analysisTasks = new HashMap<>();
        createTaskForEachColumns(jobInfo, analysisTasks, isSync);
        createTaskForMVIdx(jobInfo, analysisTasks, isSync);
        createTaskForExternalTable(jobInfo, analysisTasks, isSync);

        ConnectContext ctx = ConnectContext.get();
        if (!isSync || ctx.getSessionVariable().enableSaveStatisticsSyncJob) {
            persistAnalysisJob(jobInfo);
            analysisJobIdToTaskMap.put(jobInfo.jobId, analysisTasks);
        }

        try {
            updateTableStats(jobInfo);
        } catch (Throwable e) {
            throw new DdlException("Failed to update Table statistics");
        }

        if (isSync) {
            syncExecute(analysisTasks.values());
            return;
        }

        analysisTasks.values().forEach(taskScheduler::schedule);
        sendJobId(jobInfo.jobId);
    }

    // Analysis job created by the system
    public void createAnalysisJob(AnalysisJobInfo info) throws DdlException {
        AnalysisJobInfo jobInfo = buildAnalysisJobInfo(info);
        if (jobInfo.colToPartitions.isEmpty()) {
            // No statistics need to be collected or updated
            return;
        }

        Map<Long, BaseAnalysisTask> analysisTasks = new HashMap<>();
        createTaskForEachColumns(jobInfo, analysisTasks, false);
        createTaskForMVIdx(jobInfo, analysisTasks, false);

        persistAnalysisJob(jobInfo);
        analysisJobIdToTaskMap.put(jobInfo.jobId, analysisTasks);

        try {
            updateTableStats(jobInfo);
        } catch (Throwable e) {
            LOG.warn("Failed to update Table statistics in job: {}", info.toString());
        }

        analysisTasks.values().forEach(taskScheduler::schedule);
    }

    private void sendJobId(long jobId) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("Job_Id", ScalarType.createVarchar(19)));
        ShowResultSetMetaData commonResultSetMetaData = new ShowResultSetMetaData(columns);
        List<List<String>> resultRows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        row.add(String.valueOf(jobId));
        resultRows.add(row);
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
    private Map<String, Set<Long>> validateAndGetPartitions(TableIf table, Set<String> columnNames,
            AnalysisType analysisType, AnalysisMode analysisMode) throws DdlException {
        Set<Long> partitionIds = StatisticsUtil.getPartitionIds(table);
        Map<String, Set<Long>> columnToPartitions = columnNames.stream()
                .collect(Collectors.toMap(
                        columnName -> columnName,
                        columnName -> new HashSet<>(partitionIds)
                ));

        if (analysisType == AnalysisType.HISTOGRAM) {
            // Collecting histograms does not need to support incremental collection,
            // and will automatically cover historical statistics
            return columnToPartitions;
        }

        // Get the partition granularity statistics that have been collected
        Map<String, Set<Long>> existColAndPartsForStats = StatisticsRepository
                .fetchColAndPartsForStats(table.getId());

        if (existColAndPartsForStats.isEmpty()) {
            // There is no historical statistical information, no need to do validation
            return columnToPartitions;
        }

        Set<Long> existPartIdsForStats = new HashSet<>();
        existColAndPartsForStats.values().forEach(existPartIdsForStats::addAll);
        // Get an invalid set of partitions (those partitions were deleted)
        Set<Long> invalidPartIds = existPartIdsForStats.stream()
                .filter(id -> !partitionIds.contains(id)).collect(Collectors.toSet());

        if (!invalidPartIds.isEmpty()) {
            // Delete invalid partition statistics to avoid affecting table statistics
            StatisticsRepository.dropStatistics(invalidPartIds);
        }

        if (analysisMode == AnalysisMode.INCREMENTAL && analysisType == AnalysisType.COLUMN) {
            existColAndPartsForStats.values().forEach(partIds -> partIds.removeAll(invalidPartIds));
            // In incremental collection mode, just collect the uncollected partition statistics
            existColAndPartsForStats.forEach((existColName, existPartitionIds) -> {
                columnToPartitions.computeIfPresent(existColName, (colName, partIds) -> {
                    partIds.removeAll(existPartitionIds);
                    return partIds;
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

    private AnalysisJobInfo buildAnalysisJobInfo(AnalyzeStmt stmt) throws DdlException {
        AnalysisJobInfo.Builder jobInfoBuilder = new AnalysisJobInfo.Builder();
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

        jobInfoBuilder.jobId(jobId);
        jobInfoBuilder.catalogName(catalogName);
        jobInfoBuilder.dbName(db);
        jobInfoBuilder.tblName(tblName);
        jobInfoBuilder.jobType(JobType.MANUAL);
        jobInfoBuilder.state(AnalysisState.PENDING);
        jobInfoBuilder.analysisType(analysisType);
        jobInfoBuilder.analysisMode(analysisMode);
        jobInfoBuilder.analysisMethod(analysisMethod);
        jobInfoBuilder.scheduleType(scheduleType);
        jobInfoBuilder.lastExecTimeInMs(0);

        if (analysisMethod == AnalysisMethod.SAMPLE) {
            jobInfoBuilder.samplePercent(samplePercent);
            jobInfoBuilder.sampleRows(sampleRows);
        }

        if (analysisType == AnalysisType.HISTOGRAM) {
            int numBuckets = stmt.getNumBuckets();
            int maxBucketNum = numBuckets > 0 ? numBuckets
                    : StatisticConstants.HISTOGRAM_MAX_BUCKET_NUM;
            jobInfoBuilder.maxBucketNum(maxBucketNum);
        }

        if (scheduleType == ScheduleType.PERIOD) {
            long periodTimeInMs = stmt.getPeriodTimeInMs();
            jobInfoBuilder.periodTimeInMs(periodTimeInMs);
        }

        Map<String, Set<Long>> colToPartitions = validateAndGetPartitions(table,
                columnNames, analysisType, analysisMode);
        jobInfoBuilder.colToPartitions(colToPartitions);

        return jobInfoBuilder.build();
    }

    private AnalysisJobInfo buildAnalysisJobInfo(AnalysisJobInfo jobInfo) {
        AnalysisJobInfo.Builder jobInfoBuilder = new AnalysisJobInfo.Builder();
        jobInfoBuilder.jobId(jobInfo.jobId);
        jobInfoBuilder.catalogName(jobInfo.catalogName);
        jobInfoBuilder.dbName(jobInfo.dbName);
        jobInfoBuilder.tblName(jobInfo.tblName);
        jobInfoBuilder.jobType(JobType.SYSTEM);
        jobInfoBuilder.state(AnalysisState.PENDING);
        jobInfoBuilder.analysisType(jobInfo.analysisType);
        jobInfoBuilder.analysisMode(jobInfo.analysisMode);
        jobInfoBuilder.analysisMethod(jobInfo.analysisMethod);
        jobInfoBuilder.scheduleType(jobInfo.scheduleType);
        jobInfoBuilder.samplePercent(jobInfo.samplePercent);
        jobInfoBuilder.sampleRows(jobInfo.sampleRows);
        jobInfoBuilder.maxBucketNum(jobInfo.maxBucketNum);
        jobInfoBuilder.periodTimeInMs(jobInfo.periodTimeInMs);
        jobInfoBuilder.lastExecTimeInMs(jobInfo.lastExecTimeInMs);
        try {
            TableIf table = StatisticsUtil.findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
            Map<String, Set<Long>> colToPartitions = validateAndGetPartitions(table,
                    jobInfo.colToPartitions.keySet(), jobInfo.analysisType, jobInfo.analysisMode);
            jobInfoBuilder.colToPartitions(colToPartitions);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return jobInfoBuilder.build();
    }

    private void persistAnalysisJob(AnalysisJobInfo jobInfo) throws DdlException {
        if (jobInfo.scheduleType == ScheduleType.PERIOD && jobInfo.lastExecTimeInMs > 0) {
            return;
        }
        try {
            StatisticsRepository.persistAnalysisJob(jobInfo);
        } catch (Throwable t) {
            throw new DdlException(t.getMessage(), t);
        }
    }

    private void createTaskForMVIdx(AnalysisJobInfo jobInfo, Map<Long, BaseAnalysisTask> analysisTasks,
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
                AnalysisTaskInfo.Builder indexTaskInfoBuilder = new AnalysisTaskInfo.Builder(jobInfo);
                AnalysisTaskInfo analysisTaskInfo = indexTaskInfoBuilder
                        .indexId(indexId)
                        .taskId(taskId)
                        .build();
                analysisTasks.put(taskId, createTask(analysisTaskInfo));
                if (isSync && !ConnectContext.get().getSessionVariable().enableSaveStatisticsSyncJob) {
                    return;
                }
                try {
                    StatisticsRepository.persistAnalysisTask(analysisTaskInfo);
                } catch (Exception e) {
                    throw new DdlException("Failed to create analysis task", e);
                }
            }
        } finally {
            olapTable.readUnlock();
        }
    }

    private void createTaskForEachColumns(AnalysisJobInfo jobInfo, Map<Long, BaseAnalysisTask> analysisTasks,
            boolean isSync) throws DdlException {
        TableIf table;
        try {
            table = StatisticsUtil.findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
        } catch (Throwable e) {
            LOG.warn(e.getMessage());
            return;
        }

        Map<Long, String> partIdToName = StatisticsUtil.getPartitionIdToName(table);
        Map<String, Set<Long>> columnToPartitions = jobInfo.colToPartitions;

        for (Entry<String, Set<Long>> entry : columnToPartitions.entrySet()) {
            long indexId = -1;
            long taskId = Env.getCurrentEnv().getNextId();
            String colName = entry.getKey();
            AnalysisTaskInfo.Builder colTaskInfoBuilder = new AnalysisTaskInfo.Builder(jobInfo);
            if (jobInfo.analysisType != AnalysisInfo.AnalysisType.HISTOGRAM) {
                colTaskInfoBuilder.analysisType(AnalysisInfo.AnalysisType.COLUMN);
                Set<String> partitions = entry.getValue().stream()
                        .map(partIdToName::get).collect(Collectors.toSet());
                colTaskInfoBuilder.partitions(partitions);
            }
            AnalysisTaskInfo analysisTaskInfo = colTaskInfoBuilder
                    .colName(colName)
                    .indexId(indexId)
                    .taskId(taskId)
                    .build();
            analysisTasks.put(taskId, createTask(analysisTaskInfo));
            if (isSync && !ConnectContext.get().getSessionVariable().enableSaveStatisticsSyncJob) {
                continue;
            }
            try {
                StatisticsRepository.persistAnalysisTask(analysisTaskInfo);
            } catch (Exception e) {
                throw new DdlException("Failed to create analysis task", e);
            }
        }
    }

    private void createTaskForExternalTable(AnalysisJobInfo jobInfo,
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
        AnalysisTaskInfo.Builder colTaskInfoBuilder = new AnalysisTaskInfo.Builder(jobInfo);
        long taskId = Env.getCurrentEnv().getNextId();
        AnalysisTaskInfo analysisTaskInfo = colTaskInfoBuilder
                .indexId(-1L)
                .taskId(taskId)
                .externalTableLevelTask(true).build();
        analysisTasks.put(taskId, createTask(analysisTaskInfo));
        if (isSync && !ConnectContext.get().getSessionVariable().enableSaveStatisticsSyncJob) {
            return;
        }
        try {
            StatisticsRepository.persistAnalysisTask(analysisTaskInfo);
        } catch (Exception e) {
            throw new DdlException("Failed to create analysis task", e);
        }
    }

    public void updateTaskStatus(AnalysisTaskInfo info, AnalysisState jobState, String message, long time) {
        if (analysisJobIdToTaskMap.get(info.jobId) == null) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("jobState", jobState.toString());
        params.put("message", StringUtils.isNotEmpty(message) ? String.format(", message = '%s'", message) : "");
        params.put("updateExecTime", time == -1 ? "" : ", last_exec_time_in_ms=" + time);
        params.put("jobId", String.valueOf(info.jobId));
        params.put("taskIdExpr", "task_id = " + info.taskId);
        try {
            StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(UPDATE_JOB_STATE_SQL_TEMPLATE));
        } catch (Exception e) {
            LOG.warn(String.format("Failed to update state for task: %d, %d", info.jobId, info.taskId), e);
        } finally {
            info.state = jobState;
            if (analysisJobIdToTaskMap.get(info.jobId).values()
                    .stream().allMatch(t -> t.info.state != null
                            && t.info.state != AnalysisState.PENDING && t.info.state != AnalysisState.RUNNING)) {
                analysisJobIdToTaskMap.remove(info.jobId);
                params.put("taskIdExpr", "task_id IS NULL");
                try {
                    StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(UPDATE_JOB_STATE_SQL_TEMPLATE));
                } catch (Exception e) {
                    LOG.warn(String.format("Failed to update state for job: %s", info.jobId), e);
                }
            }
        }
    }

    private void updateTableStats(AnalysisJobInfo jobInfo) throws Throwable {
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
    private Map<String, String> buildTableStatsParams(AnalysisJobInfo jobInfo) throws Throwable {
        CatalogIf catalog = StatisticsUtil.findCatalog(jobInfo.catalogName);
        DatabaseIf db = StatisticsUtil.findDatabase(jobInfo.catalogName, jobInfo.dbName);
        TableIf tbl = StatisticsUtil.findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
        String indexId = jobInfo.indexId == null ? "-1" : String.valueOf(jobInfo.indexId);
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

    public List<List<Comparable>> showAnalysisJob(ShowAnalyzeStmt stmt) throws DdlException {
        String whereClause = stmt.getWhereClause();
        long limit = stmt.getLimit();
        String executeSql = SHOW_JOB_STATE_SQL_TEMPLATE
                + (whereClause.isEmpty() ? "" : " WHERE " + whereClause)
                + (limit == -1L ? "" : " LIMIT " + limit);

        List<List<Comparable>> results = Lists.newArrayList();
        ImmutableList<String> titleNames = stmt.getTitleNames();
        List<ResultRow> resultRows = StatisticsUtil.execStatisticQuery(executeSql);

        for (ResultRow resultRow : resultRows) {
            List<Comparable> result = Lists.newArrayList();
            for (String column : titleNames) {
                String value = resultRow.getColumnValue(column);
                if (LAST_EXEC_TIME_IN_MS.equals(column)) {
                    long timeMillis = Long.parseLong(value);
                    value = TimeUtils.DATETIME_FORMAT.format(
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(timeMillis), ZoneId.systemDefault()));
                }
                result.add(value);
            }
            results.add(result);
        }

        return results;
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
        Map<Long, BaseAnalysisTask> analysisTaskInfoMap = analysisJobIdToTaskMap.remove(killAnalysisJobStmt.jobId);
        if (analysisTaskInfoMap == null) {
            throw new DdlException("Job not exists or already finished");
        }
        BaseAnalysisTask anyTask = analysisTaskInfoMap.values().stream().findFirst().orElse(null);
        if (anyTask == null) {
            return;
        }
        checkPriv(anyTask);
        for (BaseAnalysisTask taskInfo : analysisTaskInfoMap.values()) {
            taskInfo.markAsKilled();
        }
        Map<String, String> params = new HashMap<>();
        params.put("jobState", AnalysisState.FAILED.toString());
        params.put("message", ", message = 'Killed by user : " + ConnectContext.get().getQualifiedUser() + "'");
        params.put("updateExecTime", ", last_exec_time_in_ms=" + System.currentTimeMillis());
        params.put("jobId", String.valueOf(killAnalysisJobStmt.jobId));
        params.put("taskIdExpr", "true");
        try {
            StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(UPDATE_JOB_STATE_SQL_TEMPLATE));
        } catch (Exception e) {
            LOG.warn("Failed to update status", e);
        }
    }

    private void checkPriv(BaseAnalysisTask analysisTask) {
        String dbName = analysisTask.db.getFullName();
        String tblName = analysisTask.tbl.getName();
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.SELECT)) {
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

    private BaseAnalysisTask createTask(AnalysisTaskInfo analysisTaskInfo) throws DdlException {
        try {
            TableIf table = StatisticsUtil.findTable(analysisTaskInfo.catalogName,
                    analysisTaskInfo.dbName, analysisTaskInfo.tblName);
            return table.createAnalysisTask(analysisTaskInfo);
        } catch (Throwable t) {
            LOG.warn("Failed to find table", t);
            throw new DdlException("Error when trying to find table", t);
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
            if (ConnectContext.get().getSessionVariable().enableSaveStatisticsSyncJob) {
                Env.getCurrentEnv().getAnalysisManager()
                        .updateTaskStatus(task.info, state, "", System.currentTimeMillis());
            }
        }
    }
}
