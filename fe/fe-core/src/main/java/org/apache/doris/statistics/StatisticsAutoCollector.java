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

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.VariableExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
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

public class StatisticsAutoCollector extends StatisticsCollector {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoCollector.class);

    public StatisticsAutoCollector() {
        super("Automatic Analyzer",
                TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes),
                new AnalysisTaskExecutor(Config.full_auto_analyze_simultaneously_running_task_num));
    }

    @Override
    protected void collect() {
        if (!checkAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()))) {
            return;
        }
        if (Config.enable_full_auto_analyze) {
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
                if (StatisticConstants.STATISTICS_DB_BLACK_LIST.contains(databaseIf.getFullName())) {
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
        if (table.getDataSize() < Config.huge_table_lower_bound_size_in_bytes) {
            return false;
        }
        TableStats tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(table.getId());
        return System.currentTimeMillis() - tableStats.updatedTime < Config.huge_table_auto_analyze_interval_in_millis;
    }

    protected void createAnalyzeJobForTbl(DatabaseIf<? extends TableIf> db,
            List<AnalysisInfo> analysisInfos, TableIf table) {
        AnalysisMethod analysisMethod = table.getDataSize() > Config.huge_table_lower_bound_size_in_bytes
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
                .setSamplePercent(Config.huge_table_default_sample_rows)
                .setScheduleType(AnalysisInfo.ScheduleType.ONCE)
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
        TableStats tblStats = analysisManager.findTableStatsStatus(table.getId());

        if (!(tblStats == null || table.needReAnalyzeTable(tblStats))) {
            return null;
        }

        Map<String, Set<String>> needRunPartitions = table.findReAnalyzeNeededPartitions();

        if (needRunPartitions.isEmpty()) {
            return null;
        }

        return new AnalysisInfoBuilder(jobInfo).setColToPartitions(needRunPartitions).build();
    }


    private boolean checkAnalyzeTime(LocalTime now) {
        try {
            Pair<LocalTime, LocalTime> range = findRangeFromGlobalSessionVar();
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
            LocalTime start = range == null
                    ? LocalTime.parse(Config.full_auto_analyze_start_time, timeFormatter) : range.first;
            LocalTime end = range == null
                    ? LocalTime.parse(Config.full_auto_analyze_end_time, timeFormatter) : range.second;

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

    private Pair<LocalTime, LocalTime> findRangeFromGlobalSessionVar() {
        try {
            String startTime =
                    findRangeFromGlobalSessionVar(SessionVariable.FULL_AUTO_ANALYZE_START_TIME)
                            .fullAutoAnalyzeStartTime;
            if (StringUtils.isEmpty(startTime)) {
                return null;
            }
            String endTime = findRangeFromGlobalSessionVar(SessionVariable.FULL_AUTO_ANALYZE_END_TIME)
                    .fullAutoAnalyzeEndTime;
            if (StringUtils.isEmpty(startTime)) {
                return null;
            }
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
            return Pair.of(LocalTime.parse(startTime, timeFormatter), LocalTime.parse(endTime, timeFormatter));
        } catch (Exception e) {
            return null;
        }
    }

    private SessionVariable findRangeFromGlobalSessionVar(String varName) throws Exception {
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        VariableExpr variableExpr = new VariableExpr(varName, SetType.GLOBAL);
        VariableMgr.getValue(sessionVariable, variableExpr);
        return sessionVariable;
    }
}
