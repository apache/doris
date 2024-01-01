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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
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
                new AnalysisTaskExecutor(Config.auto_analyze_simultaneously_running_task_num,
                        StatisticConstants.TASK_QUEUE_CAP));
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
        List<CatalogIf> catalogs = getCatalogsInOrder();
        for (CatalogIf ctl : catalogs) {
            if (!ctl.enableAutoAnalyze()) {
                continue;
            }
            List<DatabaseIf> dbs = getDatabasesInOrder(ctl);
            for (DatabaseIf<TableIf> databaseIf : dbs) {
                if (StatisticConstants.SYSTEM_DBS.contains(databaseIf.getFullName())) {
                    continue;
                }
                try {
                    analyzeDb(databaseIf);
                } catch (Throwable t) {
                    LOG.warn("Failed to analyze database {}.{}", ctl.getName(), databaseIf.getFullName(), t);
                    continue;
                }
            }
        }
    }

    public List<CatalogIf> getCatalogsInOrder() {
        return Env.getCurrentEnv().getCatalogMgr().getCopyOfCatalog().stream()
            .sorted((c1, c2) -> (int) (c1.getId() - c2.getId())).collect(Collectors.toList());
    }

    public List<DatabaseIf<? extends TableIf>> getDatabasesInOrder(CatalogIf<DatabaseIf> catalog) {
        return catalog.getAllDbs().stream()
            .sorted((d1, d2) -> (int) (d1.getId() - d2.getId())).collect(Collectors.toList());
    }

    public List<TableIf> getTablesInOrder(DatabaseIf<? extends TableIf> db) {
        return db.getTables().stream()
            .sorted((t1, t2) -> (int) (t1.getId() - t2.getId())).collect(Collectors.toList());
    }

    public void analyzeDb(DatabaseIf<TableIf> databaseIf) throws DdlException {
        List<AnalysisInfo> analysisInfos = constructAnalysisInfo(databaseIf);
        for (AnalysisInfo analysisInfo : analysisInfos) {
            try {
                analysisInfo = getReAnalyzeRequiredPart(analysisInfo);
                if (analysisInfo == null) {
                    continue;
                }
                createSystemAnalysisJob(analysisInfo);
            } catch (Throwable t) {
                analysisInfo.message = t.getMessage();
                LOG.warn("Failed to auto analyze table {}.{}, reason {}",
                        databaseIf.getFullName(), analysisInfo.tblId, analysisInfo.message, t);
                continue;
            }
        }
    }

    protected List<AnalysisInfo> constructAnalysisInfo(DatabaseIf<? extends TableIf> db) {
        List<AnalysisInfo> analysisInfos = new ArrayList<>();
        for (TableIf table : getTablesInOrder(db)) {
            try {
                if (skip(table)) {
                    continue;
                }
                createAnalyzeJobForTbl(db, analysisInfos, table);
            } catch (Throwable t) {
                LOG.warn("Failed to analyze table {}.{}.{}",
                        db.getCatalog().getName(), db.getFullName(), table.getName(), t);
                continue;
            }
        }
        return analysisInfos;
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
        if (table.getDataSize(true) < StatisticsUtil.getHugeTableLowerBoundSizeInBytes() * 5) {
            return false;
        }
        TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(table.getId());
        // means it's never got analyzed or new partition loaded data.
        if (tableStats == null || tableStats.newPartitionLoaded.get()) {
            return false;
        }
        return System.currentTimeMillis()
                - tableStats.updatedTime < StatisticsUtil.getHugeTableAutoAnalyzeIntervalInMillis();
    }

    protected void createAnalyzeJobForTbl(DatabaseIf<? extends TableIf> db,
            List<AnalysisInfo> analysisInfos, TableIf table) {
        AnalysisMethod analysisMethod = table.getDataSize(true) >= StatisticsUtil.getHugeTableLowerBoundSizeInBytes()
                ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
        AnalysisInfo jobInfo = new AnalysisInfoBuilder()
                .setJobId(Env.getCurrentEnv().getNextId())
                .setCatalogId(db.getCatalog().getId())
                .setDBId(db.getId())
                .setTblId(table.getId())
                .setColName(
                        table.getBaseSchema().stream().filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                                .map(Column::getName).collect(Collectors.joining(","))
                )
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
                .setEmptyJob(table instanceof OlapTable && table.getRowCount() == 0)
                .build();
        analysisInfos.add(jobInfo);
    }

    @VisibleForTesting
    protected AnalysisInfo getReAnalyzeRequiredPart(AnalysisInfo jobInfo) {
        TableIf table = StatisticsUtil.findTable(jobInfo.catalogId, jobInfo.dbId, jobInfo.tblId);
        // Skip tables that are too width.
        if (table.getBaseSchema().size() > StatisticsUtil.getAutoAnalyzeTableWidthThreshold()) {
            return null;
        }

        AnalysisManager analysisManager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tblStats = analysisManager.findTableStatsStatus(table.getId());

        Map<String, Set<String>> needRunPartitions = null;
        String colNames = jobInfo.colName;
        if (table.needReAnalyzeTable(tblStats)) {
            needRunPartitions = table.findReAnalyzeNeededPartitions();
        } else if (table instanceof OlapTable && tblStats.newPartitionLoaded.get()) {
            OlapTable olapTable = (OlapTable) table;
            needRunPartitions = new HashMap<>();
            Set<String> partitionColumnNames = olapTable.getPartitionInfo().getPartitionColumns().stream()
                    .map(Column::getName).collect(Collectors.toSet());
            colNames = partitionColumnNames.stream().collect(Collectors.joining(","));
            Set<String> partitionNames = olapTable.getAllPartitions().stream()
                    .map(Partition::getName).collect(Collectors.toSet());
            for (String column : partitionColumnNames) {
                needRunPartitions.put(column, partitionNames);
            }
        }

        if (needRunPartitions == null || needRunPartitions.isEmpty()) {
            return null;
        }
        return new AnalysisInfoBuilder(jobInfo).setColName(colNames).setColToPartitions(needRunPartitions).build();
    }
}
