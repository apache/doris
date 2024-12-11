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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsJobAppender extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsJobAppender.class);

    public StatisticsJobAppender(String name) {
        super(name, TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes));
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
        if (Env.getCurrentEnv().getStatisticsAutoCollector() == null) {
            LOG.info("Statistics auto collector not ready, skip");
            return;
        }
        if (Env.isCheckpointThread()) {
            return;
        }
        if (!Env.getCurrentEnv().isReady()) {
            return;
        }
        if (!StatisticsUtil.canCollect()) {
            LOG.debug("Auto analyze not enabled or not in analyze time range.");
            return;
        }
        traverseAllTables();
    }

    protected void traverseAllTables() {
        List<CatalogIf> catalogs = getCatalogsInOrder();
        AnalysisManager analysisManager = Env.getServingEnv().getAnalysisManager();
        StatisticsAutoCollector autoCollector = Env.getCurrentEnv().getStatisticsAutoCollector();
        for (CatalogIf ctl : catalogs) {
            if (!StatisticsUtil.canCollect()) {
                break;
            }
            if (!ctl.enableAutoAnalyze()) {
                continue;
            }
            List<DatabaseIf> dbs = getDatabasesInOrder(ctl);
            for (DatabaseIf<TableIf> db : dbs) {
                if (!StatisticsUtil.canCollect()) {
                    break;
                }
                if (StatisticConstants.SYSTEM_DBS.contains(db.getFullName())) {
                    continue;
                }
                for (TableIf table : getTablesInOrder(db)) {
                    try {
                        if (skip(table)) {
                            continue;
                        }
                        TableStatsMeta tblStats = analysisManager.findTableStatsStatus(table.getId());
                        if (table.needReAnalyzeTable(tblStats)) {
                            autoCollector.appendToHighPriorityJobs(table);
                        } else if (StatisticsUtil.tableNotAnalyzedForTooLong(table, tblStats)) {
                            autoCollector.appendToLowPriorityJobs(table);
                        }
                    } catch (Throwable t) {
                        LOG.warn("Failed to analyze table {}.{}.{}",
                                ctl.getName(), db.getFullName(), table.getName(), t);
                    }
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
        // Skip wide table.
        if (table.getBaseSchema().size() > StatisticsUtil.getAutoAnalyzeTableWidthThreshold()) {
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
        if (tableStats.userInjected) {
            return true;
        }
        return System.currentTimeMillis()
                - tableStats.lastAnalyzeTime < StatisticsUtil.getHugeTableAutoAnalyzeIntervalInMillis();
    }
}
