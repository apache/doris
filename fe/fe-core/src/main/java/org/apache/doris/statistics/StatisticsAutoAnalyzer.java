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

import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsAutoAnalyzer extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoAnalyzer.class);

    public StatisticsAutoAnalyzer() {
        super("Automatic Analyzer", TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes));
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!StatisticsUtil.statsTblAvailable()) {
            return;
        }
        if (Config.enable_auto_collect_statistics) {
            analyzePeriodically();
            analyzeAutomatically();
        }
    }

    public void autoAnalyzeStats(DdlStmt ddlStmt) {
        // TODO Monitor some DDL statements, and then trigger automatic analysis tasks
    }

    private void analyzePeriodically() {
        try {
            AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
            List<AnalysisInfo> jobInfos = analysisManager.findPeriodicJobs();
            for (AnalysisInfo jobInfo : jobInfos) {
                jobInfo = new AnalysisInfoBuilder(jobInfo).setJobType(JobType.SYSTEM).build();
                analysisManager.createAnalysisJob(jobInfo);
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
                checkedJobInfo = checkAutomaticJobInfo(jobInfo);
                if (checkedJobInfo != null) {
                    analysisManager.createAnalysisJob(checkedJobInfo);
                }
            } catch (Throwable t) {
                LOG.warn("Failed to create analyze job: {}", checkedJobInfo);
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
    private AnalysisInfo checkAutomaticJobInfo(AnalysisInfo jobInfo) throws Throwable {
        long lastExecTimeInMs = jobInfo.lastExecTimeInMs;
        TableIf table = StatisticsUtil
                .findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
        TableStatistic tblStats = StatisticsRepository.fetchTableLevelStats(table.getId());

        if (tblStats == TableStatistic.UNKNOWN) {
            LOG.warn("Failed to automatically analyze statistics, "
                    + "no corresponding table statistics for job: {}", jobInfo.toString());
            throw new DdlException("No corresponding table statistics for automatic job.");
        }

        if (!needReanalyzeTable(table, tblStats)) {
            return null;
        }

        Set<String> needRunPartitions = new HashSet<>();
        Set<String> statsPartitions = jobInfo.colToPartitions.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        checkAnalyzedPartitions(table, statsPartitions, needRunPartitions, lastExecTimeInMs);
        checkNewPartitions(table, needRunPartitions, lastExecTimeInMs);

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

    private void checkAnalyzedPartitions(TableIf table, Set<String> statsPartitions,
            Set<String> needRunPartitions, long lastExecTimeInMs) throws DdlException {
        for (String statsPartition : statsPartitions) {
            Partition partition = table.getPartition(statsPartition);
            if (partition == null) {
                // Partition that has been deleted also need to
                // be reanalyzed (delete partition statistics later)
                needRunPartitions.add(statsPartition);
                continue;
            }
            TableStatistic partitionStats = StatisticsRepository
                    .fetchTableLevelOfPartStats(partition.getId());
            if (partitionStats == TableStatistic.UNKNOWN) {
                continue;
            }
            if (needReanalyzePartition(lastExecTimeInMs, partition, partitionStats)) {
                needRunPartitions.add(partition.getName());
            }
        }
    }

    private boolean needReanalyzePartition(long lastExecTimeInMs, Partition partition, TableStatistic partStats) {
        long partUpdateTime = partition.getVisibleVersionTime();
        if (partUpdateTime < lastExecTimeInMs) {
            return false;
        }
        long pRowCount = partition.getBaseIndex().getRowCount();
        long pUpdateRows = Math.abs(pRowCount - partStats.rowCount);
        int partHealth = StatisticsUtil.getTableHealth(pRowCount, pUpdateRows);
        return partHealth < StatisticConstants.TABLE_STATS_HEALTH_THRESHOLD;
    }

    private void checkNewPartitions(TableIf table, Set<String> needRunPartitions, long lastExecTimeInMs) {
        Set<String> partitionNames = table.getPartitionNames();
        partitionNames.removeAll(needRunPartitions);
        needRunPartitions.addAll(
                partitionNames.stream()
                        .map(table::getPartition)
                        .filter(partition -> partition.getVisibleVersionTime() >= lastExecTimeInMs)
                        .map(Partition::getName)
                        .collect(Collectors.toSet())
        );
    }

    private AnalysisInfo getAnalysisJobInfo(AnalysisInfo jobInfo, TableIf table,
            Set<String> needRunPartitions) {
        Map<String, Set<String>> newColToPartitions = Maps.newHashMap();
        Map<String, Set<String>> colToPartitions = jobInfo.colToPartitions;
        colToPartitions.keySet().forEach(colName -> {
            Column column = table.getColumn(colName);
            if (column != null) {
                newColToPartitions.put(colName, needRunPartitions);
            }
        });
        return new AnalysisInfoBuilder(jobInfo)
                .setColToPartitions(newColToPartitions).build();
    }
}
