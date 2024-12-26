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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsAutoCollector extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoCollector.class);

    public static final int JOB_QUEUE_LIMIT = 100;
    private final BlockingQueue<TableIf> highPriorityJobs = new ArrayBlockingQueue<>(JOB_QUEUE_LIMIT);
    private final BlockingQueue<TableIf> lowPriorityJobs = new ArrayBlockingQueue<>(JOB_QUEUE_LIMIT);

    protected final AnalysisTaskExecutor analysisTaskExecutor;

    public StatisticsAutoCollector() {
        super("Automatic Analyzer", TimeUnit.SECONDS.toMillis(10));
        this.analysisTaskExecutor = new AnalysisTaskExecutor(Config.auto_analyze_simultaneously_running_task_num,
                StatisticConstants.TASK_QUEUE_CAP, "Auto Analysis Job Executor");
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!StatisticsUtil.statsTblAvailable()) {
            LOG.info("Stats table not available, skip");
            return;
        }
        if (Env.isCheckpointThread()) {
            return;
        }
        try {
            collect();
        } catch (DdlException | ExecutionException | InterruptedException e) {
            LOG.warn("One auto analyze job failed. ", e);
        }
    }

    protected void collect() throws DdlException, ExecutionException, InterruptedException {
        if (!StatisticsUtil.canCollect()) {
            return;
        }
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        while (true) {
            Pair<TableIf, JobPriority> jobPair = fetchOneJob();
            TableIf table = jobPair.first;
            if (table == null) {
                return;
            }
            TableStatsMeta tblStats = analysisManager.findTableStatsStatus(table.getId());
            if (table.needReAnalyzeTable(tblStats) || StatisticsUtil.tableNotAnalyzedForTooLong(table, tblStats)) {
                processOneJob(table, jobPair.second);
            }
        }
    }

    protected Pair<TableIf, JobPriority> fetchOneJob() {
        TableIf table = null;
        JobPriority priority = null;
        try {
            table = highPriorityJobs.poll(1, TimeUnit.SECONDS);
            priority = JobPriority.HIGH;
        } catch (InterruptedException e) {
            LOG.debug(e);
        }
        if (table == null) {
            try {
                table = lowPriorityJobs.poll(1, TimeUnit.SECONDS);
                priority = JobPriority.LOW;
            } catch (InterruptedException e) {
                LOG.debug(e);
            }
        }
        if (table == null) {
            LOG.debug("Job queues are all empty.");
        }
        return Pair.of(table, priority);
    }

    protected void processOneJob(TableIf table, JobPriority priority)
                throws DdlException, ExecutionException, InterruptedException {
        AnalysisMethod analysisMethod =
                table.getDataSize(true) >= StatisticsUtil.getHugeTableLowerBoundSizeInBytes()
                        ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
        if (!tableRowCountReported(table, analysisMethod)) {
            return;
        }
        // We don't auto analyze empty table to avoid all 0 stats.
        // Because all 0 is more dangerous than unknown stats when row count report is delayed.
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = manager.findTableStatsStatus(table.getId());
        long rowCount = table.getRowCount();
        if (rowCount <= 0) {
            LOG.info("Table {} is empty, remove its old stats and skip auto analyze it.", table.getName());
            // Remove the table's old stats if exists.
            if (tableStatsStatus != null && !tableStatsStatus.isColumnsStatsEmpty()) {
                manager.dropStats(table);
            }
            return;
        }
        List<Pair<String, String>> needRunColumns = table.getColumnIndexPairs(
                table.getSchemaAllIndexes(false)
                        .stream()
                        .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                        .map(Column::getName)
                        .collect(Collectors.toSet()));
        if (needRunColumns == null || needRunColumns.isEmpty()) {
            return;
        }
        StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
        for (Pair<String, String> pair : needRunColumns) {
            stringJoiner.add(pair.toString());
        }
        AnalysisInfo jobInfo = createAnalysisInfo(table, analysisMethod, rowCount,
                    stringJoiner.toString(), needRunColumns, priority);
        executeSystemAnalysisJob(jobInfo);
    }

    protected boolean tableRowCountReported(TableIf table, AnalysisMethod analysisMethod) {
        if (table instanceof OlapTable && analysisMethod.equals(AnalysisMethod.SAMPLE)) {
            OlapTable ot = (OlapTable) table;
            if (ot.getRowCountForIndex(ot.getBaseIndexId(), true) == TableIf.UNKNOWN_ROW_COUNT) {
                LOG.info("Table {} row count is not fully reported, skip auto analyzing this time.", ot.getName());
                return false;
            }
        }
        return true;
    }

    protected AnalysisInfo createAnalysisInfo(TableIf table, AnalysisMethod analysisMethod, long rowCount,
            String colNames, List<Pair<String, String>> needRunColumns, JobPriority priority) {
        AnalysisInfo jobInfo = new AnalysisInfoBuilder()
                .setJobId(Env.getCurrentEnv().getNextId())
                .setCatalogId(table.getDatabase().getCatalog().getId())
                .setDBId(table.getDatabase().getId())
                .setTblId(table.getId())
                .setAnalysisType(AnalysisInfo.AnalysisType.FUNDAMENTALS)
                .setAnalysisMode(AnalysisInfo.AnalysisMode.INCREMENTAL)
                .setAnalysisMethod(analysisMethod)
                .setSampleRows(analysisMethod.equals(AnalysisMethod.SAMPLE)
                        ? StatisticsUtil.getHugeTableSampleRows() : -1)
                .setScheduleType(ScheduleType.AUTOMATIC)
                .setState(AnalysisState.PENDING)
                .setTaskIds(new ArrayList<>())
                .setLastExecTimeInMs(System.currentTimeMillis())
                .setJobType(JobType.SYSTEM)
                .setTblUpdateTime(table.getUpdateTime())
                .setEmptyJob(table instanceof OlapTable && table.getRowCount() == 0
                        && analysisMethod.equals(AnalysisMethod.SAMPLE))
                .setRowCount(rowCount)
                .setColName(colNames)
                .setJobColumns(needRunColumns)
                .setPriority(priority)
                .setTableVersion(table instanceof OlapTable ? ((OlapTable) table).getVisibleVersion() : 0)
                .build();
        return jobInfo;
    }

    // Analysis job created by the system
    protected void executeSystemAnalysisJob(AnalysisInfo jobInfo)
            throws DdlException, ExecutionException, InterruptedException {
        if (jobInfo.jobColumns.isEmpty()) {
            // No statistics need to be collected or updated
            return;
        }
        Map<Long, BaseAnalysisTask> analysisTasks = new HashMap<>();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        analysisManager.createTaskForEachColumns(jobInfo, analysisTasks, false);
        if (StatisticsUtil.isExternalTable(jobInfo.catalogId, jobInfo.dbId, jobInfo.tblId)) {
            analysisManager.createTableLevelTaskForExternalTable(jobInfo, analysisTasks, false);
        }
        Env.getCurrentEnv().getAnalysisManager().constructJob(jobInfo, analysisTasks.values());
        Env.getCurrentEnv().getAnalysisManager().registerSysJob(jobInfo, analysisTasks);
        Future<?>[] futures = new Future[analysisTasks.values().size()];
        int i = 0;
        for (BaseAnalysisTask task : analysisTasks.values()) {
            futures[i++] = analysisTaskExecutor.submitTask(task);
        }
        for (Future future : futures) {
            future.get();
        }
    }

    public void appendToHighPriorityJobs(TableIf table) throws InterruptedException {
        highPriorityJobs.put(table);
    }

    public boolean appendToLowPriorityJobs(TableIf table) {
        return lowPriorityJobs.offer(table);
    }
}
