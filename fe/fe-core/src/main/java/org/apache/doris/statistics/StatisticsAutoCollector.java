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

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsAutoCollector extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoCollector.class);

    protected final AnalysisTaskExecutor analysisTaskExecutor;
    // Waited flag. Wait once when FE started for TabletStatMgr has received BE report at least once.
    // This couldn't guarantee getRowCount will return up-to-date value,
    // but could reduce the chance to get wrong row count. e.g. 0 after FE restart.
    private boolean waited = false;

    public StatisticsAutoCollector() {
        super("Automatic Analyzer", TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes));
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
        if (waited) {
            collect();
        } else {
            try {
                Thread.sleep((long) Config.tablet_stat_update_interval_second * 1000 * 2);
                waited = true;
            } catch (InterruptedException e) {
                LOG.info("Wait Sleep interrupted.", e);
            }
        }
    }

    protected void collect() {
        while (canCollect()) {
            Pair<Entry<TableName, Set<Pair<String, String>>>, JobPriority> job = getJob();
            if (job == null) {
                // No more job to process, break and sleep.
                break;
            }
            try {
                TableName tblName = job.first.getKey();
                TableIf table = StatisticsUtil.findTable(tblName.getCtl(), tblName.getDb(), tblName.getTbl());
                if (!supportAutoAnalyze(table)) {
                    continue;
                }
                processOneJob(table, job.first.getValue(), job.second);
            } catch (Exception e) {
                LOG.warn("Failed to analyze table {} with columns [{}]", job.first.getKey().getTbl(),
                        job.first.getValue().stream().map(Pair::toString).collect(Collectors.joining(",")), e);
            }
        }
    }

    protected boolean canCollect() {
        return StatisticsUtil.enableAutoAnalyze()
            && StatisticsUtil.inAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()));
    }

    protected Pair<Entry<TableName, Set<Pair<String, String>>>, JobPriority> getJob() {
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        Optional<Entry<TableName, Set<Pair<String, String>>>> job = fetchJobFromMap(manager.highPriorityJobs);
        if (job.isPresent()) {
            return Pair.of(job.get(), JobPriority.HIGH);
        }
        job = fetchJobFromMap(manager.midPriorityJobs);
        if (job.isPresent()) {
            return Pair.of(job.get(), JobPriority.MID);
        }
        job = fetchJobFromMap(manager.lowPriorityJobs);
        return job.map(entry -> Pair.of(entry, JobPriority.LOW)).orElse(null);
    }

    protected Optional<Map.Entry<TableName, Set<Pair<String, String>>>> fetchJobFromMap(
            Map<TableName, Set<Pair<String, String>>> jobMap) {
        synchronized (jobMap) {
            Optional<Map.Entry<TableName, Set<Pair<String, String>>>> first = jobMap.entrySet().stream().findFirst();
            first.ifPresent(entry -> jobMap.remove(entry.getKey()));
            return first;
        }
    }

    protected void processOneJob(TableIf table, Set<Pair<String, String>> columns,
            JobPriority priority) throws DdlException {
        // appendMvColumn(table, columns);
        appendPartitionColumns(table, columns);
        columns = columns.stream().filter(c -> StatisticsUtil.needAnalyzeColumn(table, c)).collect(Collectors.toSet());
        if (columns.isEmpty()) {
            return;
        }
        AnalysisInfo analyzeJob = createAnalyzeJobForTbl(table, columns, priority);
        LOG.debug("Auto analyze job : {}", analyzeJob.toString());
        try {
            executeSystemAnalysisJob(analyzeJob);
        } catch (Exception e) {
            StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
            for (Pair<String, String> pair : columns) {
                stringJoiner.add(pair.toString());
            }
            LOG.warn("Fail to auto analyze table {}, columns [{}]", table.getName(), stringJoiner.toString());
        }
    }

    protected void appendPartitionColumns(TableIf table, Set<Pair<String, String>> columns) throws DdlException {
        if (!(table instanceof OlapTable)) {
            return;
        }
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = manager.findTableStatsStatus(table.getId());
        if (tableStatsStatus != null && tableStatsStatus.partitionChanged.get()) {
            OlapTable olapTable = (OlapTable) table;
            columns.addAll(olapTable.getColumnIndexPairs(olapTable.getPartitionColumnNames()));
        }
    }

    protected void appendMvColumn(TableIf table, Set<String> columns) {
        if (!(table instanceof OlapTable)) {
            return;
        }
        OlapTable olapTable = (OlapTable) table;
        Set<String> mvColumns = olapTable.getMvColumns(false).stream().map(Column::getName).collect(Collectors.toSet());
        columns.addAll(mvColumns);
    }

    protected boolean supportAutoAnalyze(TableIf tableIf) {
        if (tableIf == null) {
            return false;
        }
        return tableIf instanceof OlapTable
                || tableIf instanceof HMSExternalTable
                && ((HMSExternalTable) tableIf).getDlaType().equals(HMSExternalTable.DLAType.HIVE);
    }

    protected AnalysisInfo createAnalyzeJobForTbl(
            TableIf table, Set<Pair<String, String>> jobColumns, JobPriority priority) {
        AnalysisMethod analysisMethod = table.getDataSize(true) >= StatisticsUtil.getHugeTableLowerBoundSizeInBytes()
                ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
        if (StatisticsUtil.enablePartitionAnalyze() && table.isPartitionedTable()) {
            analysisMethod = AnalysisMethod.FULL;
        }
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = manager.findTableStatsStatus(table.getId());
        long rowCount = StatisticsUtil.isEmptyTable(table, analysisMethod) ? 0 :
                (table.getRowCount() <= 0 ? table.fetchRowCount() : table.getRowCount());
        StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
        for (Pair<String, String> pair : jobColumns) {
            stringJoiner.add(pair.toString());
        }
        return new AnalysisInfoBuilder()
                .setJobId(Env.getCurrentEnv().getNextId())
                .setCatalogId(table.getDatabase().getCatalog().getId())
                .setDBId(table.getDatabase().getId())
                .setTblId(table.getId())
                .setColName(stringJoiner.toString())
                .setJobColumns(jobColumns)
                .setAnalysisType(AnalysisInfo.AnalysisType.FUNDAMENTALS)
                .setAnalysisMethod(analysisMethod)
                .setPartitionNames(Collections.emptySet())
                .setSampleRows(analysisMethod.equals(AnalysisMethod.SAMPLE)
                    ? StatisticsUtil.getHugeTableSampleRows() : -1)
                .setScheduleType(ScheduleType.AUTOMATIC)
                .setState(AnalysisState.PENDING)
                .setTaskIds(new ArrayList<>())
                .setLastExecTimeInMs(System.currentTimeMillis())
                .setJobType(JobType.SYSTEM)
                .setTblUpdateTime(System.currentTimeMillis())
                .setRowCount(rowCount)
                .setUpdateRows(tableStatsStatus == null ? 0 : tableStatsStatus.updatedRows.get())
                .setPriority(priority)
                .setPartitionUpdateRows(tableStatsStatus == null ? null : tableStatsStatus.partitionUpdateRows)
                .setEnablePartition(StatisticsUtil.enablePartitionAnalyze())
                .build();
    }

    // Analysis job created by the system
    @VisibleForTesting
    protected void executeSystemAnalysisJob(AnalysisInfo jobInfo)
            throws DdlException, ExecutionException, InterruptedException {
        Map<Long, BaseAnalysisTask> analysisTasks = new HashMap<>();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        analysisManager.createTaskForEachColumns(jobInfo, analysisTasks, false);
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
}
