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

    public static final int JOB_QUEUE_LIMIT = 10;
    private final BlockingQueue<TableIf> highPriorityJobs = new ArrayBlockingQueue<>(JOB_QUEUE_LIMIT);
    private final BlockingQueue<TableIf> lowPriorityJobs = new ArrayBlockingQueue<>(JOB_QUEUE_LIMIT);

    protected final AnalysisTaskExecutor analysisTaskExecutor;

    public StatisticsAutoCollector() {
        super("Automatic Analyzer", TimeUnit.SECONDS.toMillis(1));
        this.analysisTaskExecutor = new AnalysisTaskExecutor(Config.auto_analyze_simultaneously_running_task_num,
                StatisticConstants.TASK_QUEUE_CAP);
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
        TableIf table = null;
        try {
            table = highPriorityJobs.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (table == null) {
            try {
                table = lowPriorityJobs.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (table == null) {
            LOG.debug("Job queues are all empty.");
            return;
        }
        processOneJob(table);
    }

    protected void processOneJob(TableIf table) throws DdlException, ExecutionException, InterruptedException {
        AnalysisManager analysisManager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tblStats = analysisManager.findTableStatsStatus(table.getId());
        if (!table.needReAnalyzeTable(tblStats) && !StatisticsUtil.tableNotAnalyzedForTooLong(tblStats)) {
            LOG.info("Table {} doesn't need to analyze, skip it.", table.getName());
            return;
        }

        List<Pair<String, String>> needRunColumns = table.getColumnIndexPairs(table.getSchemaAllIndexes(false)
                .stream().map(Column::getName).collect(Collectors.toSet()));
        if (needRunColumns == null || needRunColumns.isEmpty()) {
            return;
        }
        StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
        for (Pair<String, String> pair : needRunColumns) {
            stringJoiner.add(pair.toString());
        }

        AnalysisMethod analysisMethod =
                table.getDataSize(true) >= StatisticsUtil.getHugeTableLowerBoundSizeInBytes()
                        ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
        long rowCount = StatisticsUtil.isEmptyTable(table, analysisMethod) ? 0 : table.getRowCount();
        AnalysisInfo jobInfo = new AnalysisInfoBuilder()
                .setJobId(Env.getCurrentEnv().getNextId())
                .setCatalogId(table.getDatabase().getCatalog().getId())
                .setDBId(table.getDatabase().getId())
                .setTblId(table.getId())
                .setColName(null)
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
                .setColName(stringJoiner.toString())
                .setJobColumns(needRunColumns)
                .build();
        executeSystemAnalysisJob(jobInfo);
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

    public void appendToLowPriorityJobs(TableIf table) throws InterruptedException {
        lowPriorityJobs.put(table);
    }
}
