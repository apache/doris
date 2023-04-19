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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    public void createAnalysisJob(AnalyzeStmt analyzeStmt) throws DdlException {
        String catalogName = analyzeStmt.getCatalogName();
        String db = analyzeStmt.getDBName();
        TableName tbl = analyzeStmt.getTblName();
        StatisticsUtil.convertTableNameToObjects(tbl);
        Set<String> colNames = analyzeStmt.getColumnNames();
        Map<Long, AnalysisTaskInfo> analysisTaskInfos = new HashMap<>();
        long jobId = Env.getCurrentEnv().getNextId();
        createTaskForEachColumns(analyzeStmt, catalogName, db, tbl, colNames, analysisTaskInfos, jobId);
        createTaskForMVIdx(analyzeStmt, catalogName, db, tbl, analysisTaskInfos, jobId);
        persistAnalysisJob(catalogName, db, tbl, jobId);

        if (analyzeStmt.isSync()) {
            syncExecute(analysisTaskInfos.values());
            return;
        }

        analysisJobIdToTaskMap.put(jobId, analysisTaskInfos);
        analysisTaskInfos.values().forEach(taskScheduler::schedule);
    }

    private void persistAnalysisJob(String catalogName, String db, TableName tbl,
            long jobId) throws DdlException {
        try {
            AnalysisTaskInfo analysisTaskInfo = new AnalysisTaskInfoBuilder().setJobId(
                            jobId).setTaskId(-1)
                    .setCatalogName(catalogName).setDbName(db)
                    .setTblName(tbl.getTbl())
                    .setJobType(JobType.MANUAL)
                    .setAnalysisMethod(AnalysisMethod.FULL).setAnalysisType(AnalysisType.INDEX)
                    .setScheduleType(ScheduleType.ONCE).build();
            StatisticsRepository.persistAnalysisTask(analysisTaskInfo);
        } catch (Throwable t) {
            throw new DdlException(t.getMessage(), t);
        }
    }

    private void createTaskForMVIdx(AnalyzeStmt analyzeStmt, String catalogName, String db, TableName tbl,
            Map<Long, AnalysisTaskInfo> analysisTaskInfos, long jobId) throws DdlException {
        if (!(analyzeStmt.isWholeTbl && analyzeStmt.getTable().getType().equals(TableType.OLAP))) {
            return;
        }
        OlapTable olapTable = (OlapTable) analyzeStmt.getTable();
        try {
            olapTable.readLock();
            for (MaterializedIndexMeta meta : olapTable.getIndexIdToMeta().values()) {
                if (meta.getDefineStmt() == null) {
                    continue;
                }
                long taskId = Env.getCurrentEnv().getNextId();
                AnalysisTaskInfo analysisTaskInfo = new AnalysisTaskInfoBuilder().setJobId(
                                jobId).setTaskId(taskId)
                        .setCatalogName(catalogName).setDbName(db)
                        .setTblName(tbl.getTbl())
                        .setIndexId(meta.getIndexId()).setJobType(JobType.MANUAL)
                        .setAnalysisMethod(AnalysisMethod.FULL).setAnalysisType(AnalysisType.INDEX)
                        .setScheduleType(ScheduleType.ONCE).build();
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

    private void createTaskForEachColumns(AnalyzeStmt analyzeStmt, String catalogName, String db, TableName tbl,
            Set<String> colNames, Map<Long, AnalysisTaskInfo> analysisTaskInfos,
            long jobId) throws DdlException {
        for (String colName : colNames) {
            long taskId = Env.getCurrentEnv().getNextId();
            AnalysisType analType = analyzeStmt.isHistogram ? AnalysisType.HISTOGRAM : AnalysisType.COLUMN;
            AnalysisTaskInfo analysisTaskInfo = new AnalysisTaskInfoBuilder().setJobId(jobId)
                    .setTaskId(taskId).setCatalogName(catalogName).setDbName(db)
                    .setTblName(tbl.getTbl()).setColName(colName)
                    .setJobType(JobType.MANUAL)
                    .setAnalysisMethod(AnalysisMethod.FULL).setAnalysisType(analType)
                    .setState(AnalysisState.PENDING)
                    .setScheduleType(ScheduleType.ONCE).build();
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
