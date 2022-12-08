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
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisTaskInfo.JobType;
import org.apache.doris.statistics.AnalysisTaskInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AnalysisManager {

    private static final Logger LOG = LogManager.getLogger(AnalysisManager.class);

    private static final String UPDATE_JOB_STATE_SQL_TEMPLATE = "UPDATE "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_JOB_TABLE + " "
            + "SET state = '${jobState}' ${message} ${updateExecTime} WHERE job_id = ${jobId}";

    private final ConcurrentMap<Long, Map<Long, AnalysisTaskInfo>> analysisJobIdToTaskMap;

    public final AnalysisTaskScheduler taskScheduler;

    private StatisticsCache statisticsCache;

    private final AnalysisTaskExecutor taskExecutor;

    public AnalysisManager() {
        analysisJobIdToTaskMap = new ConcurrentHashMap<>();
        this.taskScheduler = new AnalysisTaskScheduler();
        taskExecutor = new AnalysisTaskExecutor(taskScheduler);
        this.statisticsCache = new StatisticsCache();
        taskExecutor.start();
    }

    public void createAnalysisJob(AnalyzeStmt analyzeStmt) {
        String catalogName = analyzeStmt.getCatalogName();
        String db = analyzeStmt.getDBName();
        TableName tbl = analyzeStmt.getTblName();
        StatisticsUtil.convertTableNameToObjects(tbl);
        List<String> colNames = analyzeStmt.getOptColumnNames();
        Map<Long, AnalysisTaskInfo> analysisTaskInfos = new HashMap<>();
        long jobId = Env.getCurrentEnv().getNextId();
        if (colNames != null) {
            for (String colName : colNames) {
                long taskId = Env.getCurrentEnv().getNextId();
                AnalysisTaskInfo analysisTaskInfo = new AnalysisTaskInfoBuilder().setJobId(jobId)
                        .setTaskId(taskId).setCatalogName(catalogName).setDbName(db)
                        .setTblName(tbl.getTbl()).setColName(colName).setJobType(JobType.MANUAL)
                        .setAnalysisMethod(AnalysisMethod.FULL).setAnalysisType(AnalysisType.COLUMN)
                        .setScheduleType(ScheduleType.ONCE).build();
                try {
                    StatisticsRepository.createAnalysisTask(analysisTaskInfo);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create analysis job", e);
                }
                analysisTaskInfos.put(taskId, analysisTaskInfo);
            }
        }
        if (analyzeStmt.wholeTbl && analyzeStmt.getTable().getType().equals(TableType.OLAP)) {
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
                            .setTblName(tbl.getTbl()).setIndexId(meta.getIndexId()).setJobType(JobType.MANUAL)
                            .setAnalysisMethod(AnalysisMethod.FULL).setAnalysisType(AnalysisType.INDEX)
                            .setScheduleType(ScheduleType.ONCE).build();
                    try {
                        StatisticsRepository.createAnalysisTask(analysisTaskInfo);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to create analysis job", e);
                    }
                    analysisTaskInfos.put(taskId, analysisTaskInfo);
                }
            } finally {
                olapTable.readUnlock();
            }
        }
        analysisJobIdToTaskMap.put(jobId, analysisTaskInfos);
        analysisTaskInfos.values().forEach(taskScheduler::schedule);
    }

    public void updateTaskStatus(AnalysisTaskInfo info, AnalysisState jobState, String message, long time) {
        Map<String, String> params = new HashMap<>();
        params.put("jobState", jobState.toString());
        params.put("message", StringUtils.isNotEmpty(message) ? String.format(", message = '%s'", message) : "");
        params.put("updateExecTime", time == -1 ? "" : ", last_exec_time_in_ms=" + time);
        params.put("jobId", String.valueOf(info.jobId));
        try {
            StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(UPDATE_JOB_STATE_SQL_TEMPLATE));
        } catch (Exception e) {
            LOG.warn(String.format("Failed to update state for job: %s", info.jobId), e);
        } finally {
            info.state = jobState;
            if (analysisJobIdToTaskMap.get(info.jobId).values()
                    .stream().allMatch(i -> i.state != AnalysisState.PENDING && i.state != AnalysisState.RUNNING)) {
                analysisJobIdToTaskMap.remove(info.jobId);
            }

        }
    }

    public StatisticsCache getStatisticsCache() {
        return statisticsCache;
    }
}
