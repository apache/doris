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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Maps;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsAutoCollector extends StatisticsCollector {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoCollector.class);

    public StatisticsAutoCollector() {
        super("Automatic Analyzer",
                TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes),
                new AnalysisTaskExecutor(Config.full_auto_analyze_simultaneously_running_task_num));
    }

    @Override
    protected void collect() {
        if (!StatisticsUtil.inAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()))) {
            analysisTaskExecutor.clear();
            return;
        }
        if (StatisticsUtil.enableAutoAnalyze()) {
            analyzeAll();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void analyzeAll() {
        Set<CatalogIf> catalogs = Env.getCurrentEnv().getCatalogMgr().getCopyOfCatalog();
        for (CatalogIf ctl : catalogs) {
            if (!ctl.enableAutoAnalyze()) {
                continue;
            }
            Collection<DatabaseIf> dbs = ctl.getAllDbs();
            for (DatabaseIf<TableIf> databaseIf : dbs) {
                if (StatisticConstants.SYSTEM_DBS.contains(databaseIf.getFullName())) {
                    continue;
                }
                analyzeDb(databaseIf);
            }
        }
    }

    public void analyzeDb(DatabaseIf<TableIf> databaseIf) {
        List<AnalysisInfo> analysisInfos = constructAnalysisInfo(databaseIf);
        for (AnalysisInfo analysisInfo : analysisInfos) {
            analysisInfo = getReAnalyzeRequiredPart(analysisInfo);
            if (analysisInfo == null) {
                continue;
            }
            try {
                createSystemAnalysisJob(analysisInfo);
            } catch (Exception e) {
                LOG.warn("Failed to create analysis job", e);
            }
        }
    }

    protected List<AnalysisInfo> constructAnalysisInfo(DatabaseIf<? extends TableIf> db) {
        List<AnalysisInfo> analysisInfos = new ArrayList<>();
        for (TableIf table : db.getTables()) {
            if (skip(table)) {
                continue;
            }
            createAnalyzeJobForTbl(db, analysisInfos, table);
        }
        return analysisInfos;
    }

    // return true if skip auto analyze this time.
    protected boolean skip(TableIf table) {
        if (!(table instanceof OlapTable || table instanceof ExternalTable)) {
            return true;
        }
        if (table.getDataSize(true) < Config.huge_table_lower_bound_size_in_bytes) {
            return false;
        }
        TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(table.getId());
        return System.currentTimeMillis() - tableStats.updatedTime < Config.huge_table_auto_analyze_interval_in_millis;
    }

    protected void createAnalyzeJobForTbl(DatabaseIf<? extends TableIf> db,
            List<AnalysisInfo> analysisInfos, TableIf table) {
        AnalysisMethod analysisMethod = table.getDataSize(true) > Config.huge_table_lower_bound_size_in_bytes
                ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
        TableName tableName = new TableName(db.getCatalog().getName(), db.getFullName(),
                table.getName());
        AnalysisInfo jobInfo = new AnalysisInfoBuilder()
                .setJobId(Env.getCurrentEnv().getNextId())
                .setCatalogName(db.getCatalog().getName())
                .setDbName(db.getFullName())
                .setTblName(tableName.getTbl())
                .setColName(
                        table.getBaseSchema().stream().filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                                .map(
                                        Column::getName).collect(Collectors.joining(","))
                )
                .setAnalysisType(AnalysisInfo.AnalysisType.FUNDAMENTALS)
                .setAnalysisMode(AnalysisInfo.AnalysisMode.INCREMENTAL)
                .setAnalysisMethod(analysisMethod)
                .setSampleRows(Config.huge_table_default_sample_rows)
                .setScheduleType(ScheduleType.AUTOMATIC)
                .setState(AnalysisState.PENDING)
                .setTaskIds(new ArrayList<>())
                .setLastExecTimeInMs(System.currentTimeMillis())
                .setJobType(JobType.SYSTEM).build();
        analysisInfos.add(jobInfo);
    }

    @VisibleForTesting
    protected AnalysisInfo getReAnalyzeRequiredPart(AnalysisInfo jobInfo) {
        TableIf table = StatisticsUtil
                .findTable(jobInfo.catalogName, jobInfo.dbName, jobInfo.tblName);
        AnalysisManager analysisManager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tblStats = analysisManager.findTableStatsStatus(table.getId());

        if (!table.needReAnalyzeTable(tblStats)) {
            return null;
        }

        Map<String, Set<String>> needRunPartitions = table.findReAnalyzeNeededPartitions();

        if (needRunPartitions.isEmpty()) {
            return null;
        }

        return new AnalysisInfoBuilder(jobInfo).setColToPartitions(needRunPartitions).build();
    }

    @VisibleForTesting
    protected AnalysisInfo getAnalysisJobInfo(AnalysisInfo jobInfo, TableIf table,
            Set<String> needRunPartitions) {
        Map<String, Set<String>> newColToPartitions = Maps.newHashMap();
        Map<String, Set<String>> colToPartitions = jobInfo.colToPartitions;
        if (colToPartitions == null) {
            for (Column c : table.getColumns()) {
                if (StatisticsUtil.isUnsupportedType(c.getType())) {
                    continue;
                }
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
}
