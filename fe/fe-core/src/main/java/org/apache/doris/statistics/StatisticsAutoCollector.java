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
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
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
        if (!StatisticsUtil.inAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()))) {
            return;
        }
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        while (true) {
            try {
                TableIf table;
                Set<String> columns;
                Entry<TableIf, Set<String>> job
                        = analysisManager.getFirstJobInQueue(analysisManager.highPriorityJobs);
                AnalyzePriority priority = AnalyzePriority.HIGH;
                if (job == null) {
                    job = analysisManager.getFirstJobInQueue(analysisManager.normalPriorityJobs);
                    priority = AnalyzePriority.NORMAL;
                }
                if (job == null) {
                    job = analysisManager.getFirstJobInQueue(analysisManager.lowPriorityJobs);
                    priority = AnalyzePriority.LOW;
                }
                // No job in any job queues. Go sleep.
                if (job == null) {
                    break;
                }
                table = job.getKey();
                columns = job.getValue();
                if (skip(table)) {
                    continue;
                }
                AnalysisInfo analysisJob = createAnalyzeJobForTbl(analysisManager, table, columns, priority);
                if (analysisJob != null) {
                    createSystemAnalysisJob(analysisJob);
                }
            } catch (Throwable t) {
                LOG.warn(t.getMessage(), t);
            }
        }
    }

    // return true if skip auto analyze this time.
    protected boolean skip(TableIf table) {
        if (!(table instanceof OlapTable || table instanceof HMSExternalTable)) {
            return true;
        }
        // For now, only support Hive HMS table auto collection.
        if (table instanceof HMSExternalTable
                && !((HMSExternalTable) table).getDlaType().equals(HMSExternalTable.DLAType.HIVE)) {
            return true;
        }
        return false;
        //    if (table.getDataSize(true) < StatisticsUtil.getHugeTableLowerBoundSizeInBytes() * 5) {
        //        return false;
        //    }
        //    TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(table.getId());
        //    // means it's never got analyzed
        //    if (tableStats == null) {
        //        return false;
        //    }
        //    return System.currentTimeMillis()
        //            - tableStats.updatedTime < StatisticsUtil.getHugeTableAutoAnalyzeIntervalInMillis();
    }

    protected AnalysisInfo createAnalyzeJobForTbl(AnalysisManager analysisManager, TableIf table,
                                                  Set<String> columns, AnalyzePriority priority) {
        if (priority.equals(AnalyzePriority.LOW)
                && table.getBaseSchema().size() > StatisticsUtil.getAutoAnalyzeTableWidthThreshold()) {
            // Skip low priority wide table.
            return null;
        }
        if (columns == null) {
            columns = table.getColumns()
                .stream()
                .map(Column::getName)
                .filter(c -> !StatisticsUtil.isUnsupportedType(table.getColumn(c).getType()))
                .collect(Collectors.toSet());
        }
        TableStatsMeta tableStats = analysisManager.findTableStatsStatus(table.getId());
        long updatedRows = tableStats.updatedRows.get();
        long rowCount = table.getRowCount();
        String columnNames = columns
                .stream()
                .filter(c -> !StatisticsUtil.isUnsupportedType(table.getColumn(c).getType()))
                .filter(c -> needAnalyzeColumn(tableStats, rowCount, updatedRows, c, table))
                .collect(Collectors.joining(","));
        if (columnNames == null || columnNames.isEmpty()) {
            return null;
        }

        AnalysisMethod analysisMethod = table.getDataSize(true)
                > StatisticsUtil.getHugeTableLowerBoundSizeInBytes()
                ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
        AnalysisInfo jobInfo = new AnalysisInfoBuilder()
                .setJobId(Env.getCurrentEnv().getNextId())
                .setCatalogId(table.getDatabase().getCatalog().getId())
                .setDBId(table.getDatabase().getId())
                .setTblId(table.getId())
                .setColName(columnNames)
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
                .setAnalyzePriority(priority)
                .setCurrentUpdatedRows(updatedRows)
                .build();
        return jobInfo;
    }

    protected boolean needAnalyzeColumn(TableStatsMeta tblStats, long rowCount,
                                        long updatedRows, String column, TableIf table) {
        if (tblStats == null) {
            return true;
        }
        ColStatsMeta columnStatsMeta = tblStats.findColumnStatsMeta(column);
        if (columnStatsMeta == null) {
            return true;
        }
        if (table instanceof ExternalTable) {
            return System.currentTimeMillis()
                - columnStatsMeta.updatedTime > StatisticsUtil.getExternalTableAutoAnalyzeIntervalInMillis();
        } else if (table instanceof OlapTable) {
            int tblHealth = StatisticsUtil.getTableHealth(rowCount, updatedRows - columnStatsMeta.lastUpdatedRows);
            return tblHealth < StatisticsUtil.getTableStatsHealthThreshold();
        } else {
            return true;
        }
    }

    @VisibleForTesting
    protected AnalysisInfo getReAnalyzeRequiredPart(AnalysisInfo jobInfo) {
        TableIf table = StatisticsUtil
                .findTable(jobInfo.catalogId, jobInfo.dbId, jobInfo.tblId);

        Map<String, Set<String>> needRunPartitions = table.findReAnalyzeNeededPartitions();
        if (needRunPartitions.isEmpty()) {
            return null;
        }
        return new AnalysisInfoBuilder(jobInfo).setColToPartitions(needRunPartitions).build();
    }
}
