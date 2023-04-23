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
import org.apache.doris.analysis.ShowAnalyzeStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisTaskInfo.JobType;
import org.apache.doris.statistics.AnalysisTaskInfo.ScheduleType;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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
            + "SET state = '${jobState}' ${message} ${updateExecTime} WHERE job_id = ${jobId} and task_id=${taskId}";

    private static final String SHOW_JOB_STATE_SQL_TEMPLATE = "SELECT "
            + "job_id, catalog_name, db_name, tbl_name, col_name, job_type, "
            + "analysis_type, message, last_exec_time_in_ms, state, schedule_type "
            + "FROM " + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_JOB_TABLE;

    // The time field that needs to be displayed
    private static final String LAST_EXEC_TIME_IN_MS = "last_exec_time_in_ms";

    private final ConcurrentMap<Long, Map<Long, AnalysisTaskInfo>> analysisJobIdToTaskMap;

    private StatisticsCache statisticsCache;

    private final AnalysisTaskExecutor taskExecutor;

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
        Map<String, Set<String>> columnToPartitions = validateAndGetPartitions(stmt);
        if (columnToPartitions.isEmpty()) {
            // No statistics need to be collected or updated
            return;
        }

        long jobId = Env.getCurrentEnv().getNextId();
        TableIf table = stmt.getTable();
        AnalysisType analysisType = stmt.getAnalysisType();

        AnalysisTaskInfoBuilder taskInfoBuilder = buildCommonTaskInfo(stmt, jobId);
        Map<Long, AnalysisTaskInfo> analysisTaskInfos = new HashMap<>();

        // start build analysis tasks
        createTaskForEachColumns(columnToPartitions, taskInfoBuilder, analysisTaskInfos, analysisType);
        createTaskForMVIdx(table, taskInfoBuilder, analysisTaskInfos, analysisType);
        persistAnalysisJob(taskInfoBuilder);

        if (stmt.isSync()) {
            syncExecute(analysisTaskInfos.values());
            return;
        }

        analysisJobIdToTaskMap.put(jobId, analysisTaskInfos);
        analysisTaskInfos.values().forEach(taskScheduler::schedule);
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
    private Map<String, Set<String>> validateAndGetPartitions(AnalyzeStmt stmt) throws DdlException {
        TableIf table = stmt.getTable();
        long tableId = table.getId();
        Set<String> columnNames = stmt.getColumnNames();
        Set<String> partitionNames = table.getPartitionNames();

        Map<String, Set<String>> columnToPartitions = Maps.newHashMap();
        stmt.getColumnNames().forEach(column -> columnToPartitions
                .computeIfAbsent(column, partitions -> new HashSet<>()).addAll(partitionNames));

        if (stmt.getAnalysisType() == AnalysisType.HISTOGRAM) {
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
            StatisticsRepository.dropStatistics(tableId, invalidPartIds,
                    columnNames, StatisticConstants.STATISTIC_TBL_NAME);
        }

        if (stmt.isIncremental() && stmt.getAnalysisType() == AnalysisType.COLUMN) {
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
        }

        Collection<Set<String>> partitions = columnToPartitions.values();
        boolean isEmptyPartitions = partitions.stream().allMatch(Set::isEmpty);

        if (invalidPartIds.isEmpty() && isEmptyPartitions) {
            // There is no invalid statistics, and there are no partition stats to be collected
            return Collections.emptyMap();
        }

        return columnToPartitions;
    }

    private AnalysisTaskInfoBuilder buildCommonTaskInfo(AnalyzeStmt stmt, long jobId) {
        AnalysisTaskInfoBuilder taskInfoBuilder = new AnalysisTaskInfoBuilder();
        String catalogName = stmt.getCatalogName();
        String db = stmt.getDBName();
        TableName tbl = stmt.getTblName();
        StatisticsUtil.convertTableNameToObjects(tbl);
        String tblName = tbl.getTbl();
        int samplePercent = stmt.getSamplePercent();
        int sampleRows = stmt.getSampleRows();
        AnalysisType analysisType = stmt.getAnalysisType();
        AnalysisMethod analysisMethod = stmt.getAnalysisMethod();

        taskInfoBuilder.setJobId(jobId);
        taskInfoBuilder.setCatalogName(catalogName);
        taskInfoBuilder.setDbName(db);
        taskInfoBuilder.setTblName(tblName);
        taskInfoBuilder.setJobType(JobType.MANUAL);
        taskInfoBuilder.setState(AnalysisState.PENDING);
        taskInfoBuilder.setScheduleType(ScheduleType.ONCE);

        if (analysisMethod == AnalysisMethod.SAMPLE) {
            taskInfoBuilder.setAnalysisMethod(AnalysisMethod.SAMPLE);
            taskInfoBuilder.setSamplePercent(samplePercent);
            taskInfoBuilder.setSampleRows(sampleRows);
        } else {
            taskInfoBuilder.setAnalysisMethod(AnalysisMethod.FULL);
        }

        if (analysisType == AnalysisType.HISTOGRAM) {
            taskInfoBuilder.setAnalysisType(AnalysisType.HISTOGRAM);
            int numBuckets = stmt.getNumBuckets();
            int maxBucketNum = numBuckets > 0 ? numBuckets
                    : StatisticConstants.HISTOGRAM_MAX_BUCKET_NUM;
            taskInfoBuilder.setMaxBucketNum(maxBucketNum);
        } else {
            taskInfoBuilder.setAnalysisType(AnalysisType.COLUMN);
        }

        return taskInfoBuilder;
    }

    private void persistAnalysisJob(AnalysisTaskInfoBuilder taskInfoBuilder) throws DdlException {
        try {
            AnalysisTaskInfoBuilder jobInfoBuilder = taskInfoBuilder.copy();
            AnalysisTaskInfo analysisTaskInfo = jobInfoBuilder.setTaskId(-1).build();
            StatisticsRepository.persistAnalysisTask(analysisTaskInfo);
        } catch (Throwable t) {
            throw new DdlException(t.getMessage(), t);
        }
    }

    private void createTaskForMVIdx(TableIf table, AnalysisTaskInfoBuilder taskInfoBuilder,
            Map<Long, AnalysisTaskInfo> analysisTaskInfos, AnalysisType analysisType) throws DdlException {
        TableType type = table.getType();
        if (analysisType != AnalysisType.INDEX || !type.equals(TableType.OLAP)) {
            // not need to collect statistics for materialized view
            return;
        }

        taskInfoBuilder.setAnalysisType(analysisType);
        OlapTable olapTable = (OlapTable) table;

        try {
            olapTable.readLock();
            for (MaterializedIndexMeta meta : olapTable.getIndexIdToMeta().values()) {
                if (meta.getDefineStmt() == null) {
                    continue;
                }
                AnalysisTaskInfoBuilder indexTaskInfoBuilder = taskInfoBuilder.copy();
                long indexId = meta.getIndexId();
                long taskId = Env.getCurrentEnv().getNextId();
                AnalysisTaskInfo analysisTaskInfo = indexTaskInfoBuilder.setIndexId(indexId)
                        .setTaskId(taskId).build();
                try {
                    StatisticsRepository.persistAnalysisTask(analysisTaskInfo);
                } catch (Exception e) {
                    throw new DdlException("Failed to create analysis task", e);
                }
                analysisTaskInfos.put(taskId, analysisTaskInfo);
            }
        } finally {
            olapTable.readUnlock();
        }
    }

    private void createTaskForEachColumns(Map<String, Set<String>> columnToPartitions,
            AnalysisTaskInfoBuilder taskInfoBuilder,
            Map<Long, AnalysisTaskInfo> analysisTaskInfos, AnalysisType analysisType) throws DdlException {
        for (Entry<String, Set<String>> entry : columnToPartitions.entrySet()) {
            Set<String> partitionNames = entry.getValue();
            if (partitionNames.isEmpty()) {
                continue;
            }
            AnalysisTaskInfoBuilder newTaskInfoBuilder = taskInfoBuilder.copy();
            if (analysisType != AnalysisType.HISTOGRAM) {
                // Histograms do not need to specify partitions
                newTaskInfoBuilder.setPartitionNames(partitionNames);
            }
            long taskId = Env.getCurrentEnv().getNextId();
            long indexId = -1;
            String colName = entry.getKey();
            AnalysisTaskInfo analysisTaskInfo = newTaskInfoBuilder
                    .setTaskId(taskId)
                    .setIndexId(indexId)
                    .setColName(colName)
                    .build();
            try {
                StatisticsRepository.persistAnalysisTask(analysisTaskInfo);
            } catch (Exception e) {
                throw new DdlException("Failed to create analysis task", e);
            }
            analysisTaskInfos.put(taskId, analysisTaskInfo);
        }
    }

    public void updateTaskStatus(AnalysisTaskInfo info, AnalysisState jobState, String message, long time) {
        Map<String, String> params = new HashMap<>();
        params.put("jobState", jobState.toString());
        params.put("message", StringUtils.isNotEmpty(message) ? String.format(", message = '%s'", message) : "");
        params.put("updateExecTime", time == -1 ? "" : ", last_exec_time_in_ms=" + time);
        params.put("jobId", String.valueOf(info.jobId));
        params.put("taskId", String.valueOf(info.taskId));
        try {
            StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(UPDATE_JOB_STATE_SQL_TEMPLATE));
        } catch (Exception e) {
            LOG.warn(String.format("Failed to update state for task: %d, %d", info.jobId, info.taskId), e);
        } finally {
            info.state = jobState;
            if (analysisJobIdToTaskMap.get(info.jobId).values()
                    .stream().allMatch(i -> i.state != null
                            && i.state != AnalysisState.PENDING && i.state != AnalysisState.RUNNING)) {
                analysisJobIdToTaskMap.remove(info.jobId);
                params.put("taskId", String.valueOf(-1));
                try {
                    StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(UPDATE_JOB_STATE_SQL_TEMPLATE));
                } catch (Exception e) {
                    LOG.warn(String.format("Failed to update state for job: %s", info.jobId), e);
                }
            }

        }
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

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (ResultRow resultRow : resultRows) {
            List<Comparable> result = Lists.newArrayList();
            for (String column : titleNames) {
                String value = resultRow.getColumnValue(column);
                if (LAST_EXEC_TIME_IN_MS.equals(column)) {
                    long timeMillis = Long.parseLong(value);
                    value = dateFormat.format(new Date(timeMillis));
                }
                result.add(value);
            }
            results.add(result);
        }

        return results;
    }

    private void syncExecute(Collection<AnalysisTaskInfo> taskInfos) {
        List<String> colNames = new ArrayList<>();
        for (AnalysisTaskInfo info : taskInfos) {
            try {
                TableIf table = StatisticsUtil.findTable(info.catalogName,
                        info.dbName, info.tblName);
                BaseAnalysisTask analysisTask = table.createAnalysisTask(info);
                analysisTask.execute();
            } catch (Throwable t) {
                colNames.add(info.colName);
                LOG.info("Failed to analyze, info: {}", info);
            }
        }
        if (!colNames.isEmpty()) {
            throw new RuntimeException("Failed to analyze following columns: " + String.join(",", colNames));
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

}
