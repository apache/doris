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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsAutoCollector extends StatisticsCollector {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoCollector.class);

    public StatisticsAutoCollector() {
        super("Automatic Analyzer",
                TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes),
                new AnalysisTaskExecutor(Config.auto_analyze_simultaneously_running_task_num,
                        StatisticConstants.TASK_QUEUE_CAP));
    }

    @Override
    protected void collect() {
        while (canCollect()) {
            Pair<Entry<TableName, Set<String>>, JobPriority> job = getJob();
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
                Set<String> columns = job.first.getValue().stream().collect(Collectors.toSet());
                processOneJob(table, columns, job.second);
            } catch (Exception e) {
                LOG.warn("Failed to analyze table {} with columns [{}]", job.first.getKey().getTbl(),
                        job.first.getValue().stream().collect(Collectors.joining(",")), e);
            }
        }
    }

    protected boolean canCollect() {
        return StatisticsUtil.enableAutoAnalyze()
            && StatisticsUtil.inAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()));
    }

    protected Pair<Entry<TableName, Set<String>>, JobPriority> getJob() {
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        Optional<Entry<TableName, Set<String>>> job = fetchJobFromMap(manager.highPriorityJobs);
        if (job.isPresent()) {
            return Pair.of(job.get(), JobPriority.HIGH);
        }
        job = fetchJobFromMap(manager.midPriorityJobs);
        if (job.isPresent()) {
            return Pair.of(job.get(), JobPriority.MID);
        }
        job = fetchJobFromMap(manager.lowPriorityJobs);
        return job.isPresent() ? Pair.of(job.get(), JobPriority.LOW) : null;
    }

    protected Optional<Map.Entry<TableName, Set<String>>> fetchJobFromMap(Map<TableName, Set<String>> jobMap) {
        synchronized (jobMap) {
            Optional<Map.Entry<TableName, Set<String>>> first = jobMap.entrySet().stream().findFirst();
            first.ifPresent(entry -> jobMap.remove(entry.getKey()));
            return first;
        }
    }

    protected void processOneJob(TableIf table, Set<String> columns, JobPriority priority) throws DdlException {
        appendPartitionColumns(table, columns);
        if (columns.isEmpty()) {
            return;
        }
        AnalysisInfo analyzeJob = createAnalyzeJobForTbl(table, columns, priority);
        LOG.info("Analyze job : {}", analyzeJob.toString());
        createSystemAnalysisJob(analyzeJob);
    }

    protected void appendPartitionColumns(TableIf table, Set<String> columns) {
        if (!(table instanceof OlapTable)) {
            return;
        }
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = manager.findTableStatsStatus(table.getId());
        if (tableStatsStatus != null && tableStatsStatus.newPartitionLoaded.get()) {
            OlapTable olapTable = (OlapTable) table;
            columns.addAll(olapTable.getPartitionNames());
        }
    }

    protected boolean supportAutoAnalyze(TableIf tableIf) {
        if (tableIf == null) {
            return false;
        }
        return tableIf instanceof OlapTable
                || tableIf instanceof HMSExternalTable
                && ((HMSExternalTable) tableIf).getDlaType().equals(HMSExternalTable.DLAType.HIVE);
    }

    protected AnalysisInfo createAnalyzeJobForTbl(TableIf table, Set<String> columns, JobPriority priority) {
        AnalysisMethod analysisMethod = table.getDataSize(true) >= StatisticsUtil.getHugeTableLowerBoundSizeInBytes()
                ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = manager.findTableStatsStatus(table.getId());
        long rowCount = table.getRowCount();
        Map<String, Set<String>> colToPartitions = new HashMap<>();
        Set<String> dummyPartition = new HashSet<>();
        dummyPartition.add("dummy partition");
        columns.stream().forEach(c -> colToPartitions.put(c, dummyPartition));
        return new AnalysisInfoBuilder()
                .setJobId(Env.getCurrentEnv().getNextId())
                .setCatalogId(table.getDatabase().getCatalog().getId())
                .setDBId(table.getDatabase().getId())
                .setTblId(table.getId())
                .setColName(columns.stream().collect(Collectors.joining(",")))
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
                .setRowCount(rowCount)
                .setUpdateRows(tableStatsStatus == null ? 0 : tableStatsStatus.updatedRows.get())
                .setColToPartitions(colToPartitions)
                .setPriority(priority)
                .build();
    }
}
