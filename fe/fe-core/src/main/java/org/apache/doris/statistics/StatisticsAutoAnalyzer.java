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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsAutoAnalyzer extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoAnalyzer.class);

    private AnalysisTaskExecutor analysisTaskExecutor;

    public StatisticsAutoAnalyzer() {
        super("Automatic Analyzer",
                TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes) / 2);
        analysisTaskExecutor = new AnalysisTaskExecutor(Config.full_auto_analyze_simultaneously_running_task_num);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!StatisticsUtil.statsTblAvailable()) {
            return;
        }
        if (!checkAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()))) {
            return;
        }

        if (!analysisTaskExecutor.idle()) {
            return;
        }

        analyzePeriodically();
        if (!Config.enable_full_auto_analyze) {
            analyzeAutomatically();
        } else {
            analyzeAll();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void analyzeAll() {
        Set<CatalogIf> catalogs = Env.getCurrentEnv().getCatalogMgr().getCopyOfCatalog();
        for (CatalogIf ctl : catalogs) {

            Collection<DatabaseIf> dbs = ctl.getAllDbs();
            for (DatabaseIf<TableIf> databaseIf : dbs) {
                if (StatisticConstants.STATISTICS_DB_BLACK_LIST.contains(databaseIf.getFullName())) {
                    continue;
                }
                AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
                List<AnalysisInfo> analysisInfos = constructAnalysisInfo(databaseIf);
                for (AnalysisInfo analysisInfo : analysisInfos) {
                    analysisInfo = getReAnalyzeRequiredPart(analysisInfo);
                    if (analysisInfo == null) {
                        continue;
                    }
                    try {
                        analysisManager.createSystemAnalysisJob(analysisInfo, analysisTaskExecutor);
                    } catch (Exception e) {
                        LOG.warn("Failed to create analysis job", e);
                    }
                }
            }
        }
    }

    private List<AnalysisInfo> constructAnalysisInfo(DatabaseIf<TableIf> db) {
        List<AnalysisInfo> analysisInfos = new ArrayList<>();
        for (TableIf table : db.getTables()) {
            if (table instanceof View) {
                continue;
            }
            TableName tableName = new TableName(db.getCatalog().getName(), db.getFullName(),
                    table.getName());
            AnalysisInfo jobInfo = new AnalysisInfoBuilder()
                    .setJobId(Env.getCurrentEnv().getNextId())
                    .setCatalogName(db.getCatalog().getName())
                    .setDbName(db.getFullName())
                    .setTblName(tableName.getTbl())
                    .setColName(
                        table.getBaseSchema().stream().filter(c -> !StatisticsUtil.isUnsupportedType(c.getType())).map(
                            Column::getName).collect(Collectors.joining(","))
                    )
                    .setAnalysisType(AnalysisInfo.AnalysisType.FUNDAMENTALS)
                    .setAnalysisMode(AnalysisInfo.AnalysisMode.INCREMENTAL)
                    .setAnalysisMethod(AnalysisInfo.AnalysisMethod.FULL)
                    .setScheduleType(AnalysisInfo.ScheduleType.ONCE)
                    .setJobType(JobType.SYSTEM).build();
            analysisInfos.add(jobInfo);
        }
        return analysisInfos;
    }

    private void analyzePeriodically() {
        try {
            AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
            List<AnalysisInfo> jobInfos = analysisManager.findPeriodicJobs();
            for (AnalysisInfo jobInfo : jobInfos) {
                jobInfo = new AnalysisInfoBuilder(jobInfo).setJobType(JobType.SYSTEM).build();
                analysisManager.createSystemAnalysisJob(jobInfo, analysisTaskExecutor);
            }
        } catch (DdlException e) {
            LOG.warn("Failed to periodically analyze the statistics." + e);
        }
    }

    private void analyzeAutomatically() {
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        List<AnalysisInfo> jobInfos = analysisManager.findAutomaticAnalysisJobs();
        for (AnalysisInfo jobInfo : jobInfos) {
            AnalysisInfo checkedJobInfo = null;
            try {
                checkedJobInfo = getReAnalyzeRequiredPart(jobInfo);
                if (checkedJobInfo != null) {
                    analysisManager.createSystemAnalysisJob(checkedJobInfo, analysisTaskExecutor);
                }
            } catch (Throwable t) {
                LOG.warn("Failed to create analyze job: {}", checkedJobInfo, t);
            }

        }
    }

    /**
     * Check if automatic analysis of statistics is required.
     * <p>
     * Step1: check the health of the table, if the health is good,
     * there is no need to re-analyze, or check partition
     * <p>
     * Step2: check the partition update time, if the partition is not updated
     * after the statistics is analyzed, there is no need to re-analyze
     * <p>
     * Step3: if the partition is updated after the statistics is analyzed,
     * check the health of the partition, if the health is good, there is no need to re-analyze
     * - Step3.1: check the analyzed partition statistics
     * - Step3.2: Check for new partitions for which statistics were not analyzed
     * <p>
     * TODO new columns is not currently supported to analyze automatically
     *
     * @param jobInfo analysis job info
     * @return new job info after check
     * @throws Throwable failed to check
     */
    private AnalysisInfo getReAnalyzeRequiredPart(AnalysisInfo jobInfo) {
        long lastExecTimeInMs = jobInfo.lastExecTimeInMs;
        TableIf table = StatisticsUtil
                .findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
        TableStatistic tblStats = null;
        try {
            tblStats = StatisticsRepository.fetchTableLevelStats(table.getId());
        } catch (Throwable t) {
            LOG.warn("Failed to fetch table stats", t);
            return null;
        }

        if (!(needReanalyzeTable(table, tblStats) || tblStats == TableStatistic.UNKNOWN)) {
            return null;
        }

        Set<String> needRunPartitions = table.getPartitionNames().stream()
                .map(table::getPartition)
                .filter(Partition::hasData)
                .filter(partition ->
                    partition.getVisibleVersionTime() >= lastExecTimeInMs).map(Partition::getName)
                .collect(Collectors.toSet());

        if (needRunPartitions.isEmpty()) {
            return null;
        }

        return getAnalysisJobInfo(jobInfo, table, needRunPartitions);
    }

    private boolean needReanalyzeTable(TableIf table, TableStatistic tblStats) {
        long rowCount = table.getRowCount();
        long updateRows = Math.abs(rowCount - tblStats.rowCount);
        int tblHealth = StatisticsUtil.getTableHealth(rowCount, updateRows);
        return tblHealth < StatisticConstants.TABLE_STATS_HEALTH_THRESHOLD;
    }

    private AnalysisInfo getAnalysisJobInfo(AnalysisInfo jobInfo, TableIf table,
            Set<String> needRunPartitions) {
        Map<String, Set<String>> newColToPartitions = Maps.newHashMap();
        Map<String, Set<String>> colToPartitions = jobInfo.colToPartitions;
        if (colToPartitions == null) {
            for (Column c : table.getColumns()) {
                newColToPartitions.put(c.getName(), needRunPartitions);
            }
        } else {
            colToPartitions.keySet().forEach(colName -> {
                Column column = table.getColumn(colName);
                if (column != null) {
                    newColToPartitions.put(colName, needRunPartitions);
                }
            });
        }
        return new AnalysisInfoBuilder(jobInfo)
                .setColToPartitions(newColToPartitions).build();
    }

    private boolean checkAnalyzeTime(LocalTime now) {
        try {
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
            LocalTime start = LocalTime.parse(Config.full_auto_analyze_start_time, timeFormatter);
            LocalTime end = LocalTime.parse(Config.full_auto_analyze_end_time, timeFormatter);

            if (start.isAfter(end) && (now.isAfter(start) || now.isBefore(end))) {
                return true;
            } else {
                return now.isAfter(start) && now.isBefore(end);
            }
        } catch (DateTimeParseException e) {
            LOG.warn("Parse analyze start/end time format fail", e);
            return true;
        }
    }
}
