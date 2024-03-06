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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
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
            Map.Entry<TableIf, Set<String>> job = getJob();
            if (job == null) {
                // No more job to process, break and sleep.
                break;
            }
            try {
                TableIf table = job.getKey();
                if (!supportAutoAnalyze(table)) {
                    continue;
                }
                Set<String> columns = job.getValue()
                        .stream()
                        .filter(c -> {
                            boolean needAnalyzeColumn = needAnalyzeColumn(table, c);
                            LOG.info("Need analyze column " + c + " ? " + needAnalyzeColumn);
                            return needAnalyzeColumn;
                        })
                        .collect(Collectors.toSet());
                processOneJob(table, columns);
            } catch (Exception e) {
                LOG.warn("Failed to analyze table {} with columns [{}]",
                        job.getKey().getName(), job.getValue().stream().collect(Collectors.joining(",")), e);
            }
        }
    }

    protected boolean canCollect() {
        return StatisticsUtil.enableAutoAnalyze()
            && StatisticsUtil.inAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()));
    }

    protected Map.Entry<TableIf, Set<String>> getJob() {
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        Optional<Map.Entry<TableIf, Set<String>>> job = fetchJobFromMap(manager.highPriorityJobs);
        if (job.isPresent()) {
            return job.get();
        }
        job = fetchJobFromMap(manager.midPriorityJobs);
        if (job.isPresent()) {
            return job.get();
        }
        job = fetchJobFromMap(manager.lowPriorityJobs);
        return job.isPresent() ? job.get() : null;
    }

    protected Optional<Map.Entry<TableIf, Set<String>>> fetchJobFromMap(Map<TableIf, Set<String>> jobMap) {
        synchronized (jobMap) {
            Optional<Map.Entry<TableIf, Set<String>>> first = jobMap.entrySet().stream().findFirst();
            first.ifPresent(entry -> jobMap.remove(entry.getKey()));
            return first;
        }
    }

    protected void processOneJob(TableIf table, Set<String> columns) throws DdlException {
        appendPartitionColumns(table, columns);
        if (columns.isEmpty()) {
            return;
        }
        AnalysisInfo analyzeJob = createAnalyzeJobForTbl(table, columns);
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

    // TODO: Need refactor, hard to understand now.
    protected boolean needAnalyzeColumn(TableIf table, String column) {
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = manager.findTableStatsStatus(table.getId());
        if (tableStatsStatus == null) {
            return true;
        }
        if (tableStatsStatus.userInjected) {
            return false;
        }
        ColStatsMeta columnStatsMeta = tableStatsStatus.findColumnStatsMeta(column);
        if (columnStatsMeta == null) {
            return true;
        }
        if (table instanceof OlapTable) {
            long currentUpdatedRows = tableStatsStatus.updatedRows.get();
            long lastAnalyzeUpdateRows = columnStatsMeta.updatedRows;
            if (lastAnalyzeUpdateRows == 0 && currentUpdatedRows > 0) {
                return true;
            }
            if (lastAnalyzeUpdateRows > currentUpdatedRows) {
                // Shouldn't happen. Just in case.
                return true;
            }
            OlapTable olapTable = (OlapTable) table;
            long currentRowCount = olapTable.getRowCount();
            long lastAnalyzeRowCount = columnStatsMeta.rowCount;
            if (tableStatsStatus.newPartitionLoaded.get() && olapTable.isPartitionColumn(column)) {
                return true;
            }
            if (lastAnalyzeRowCount == 0 && currentRowCount > 0) {
                return true;
            }
            if (currentUpdatedRows == lastAnalyzeUpdateRows) {
                return false;
            }
            double healthValue = ((double) (currentUpdatedRows - lastAnalyzeUpdateRows)
                    / (double) currentUpdatedRows) * 100.0;
            LOG.info("Column " + column + " update rows health value is " + healthValue);
            if (healthValue < StatisticsUtil.getTableStatsHealthThreshold()) {
                return true;
            }
            if (currentRowCount == 0 && lastAnalyzeRowCount != 0) {
                return true;
            }
            if (currentRowCount == 0 && lastAnalyzeRowCount == 0) {
                return false;
            }
            healthValue = ((double) (currentRowCount - lastAnalyzeRowCount) / (double) currentRowCount) * 100.0;
            return healthValue < StatisticsUtil.getTableStatsHealthThreshold();
        } else {
            if (!(table instanceof HMSExternalTable)) {
                return false;
            }
            HMSExternalTable hmsTable = (HMSExternalTable) table;
            if (!hmsTable.getDlaType().equals(DLAType.HIVE)) {
                return false;
            }
            return System.currentTimeMillis()
                    - tableStatsStatus.updatedTime > StatisticsUtil.getExternalTableAutoAnalyzeIntervalInMillis();
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

    protected AnalysisInfo createAnalyzeJobForTbl(TableIf table, Set<String> columns) {
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
                .build();
    }
}
