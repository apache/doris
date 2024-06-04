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
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Maintenance the internal statistics table.
 * Delete rows that corresponding DB/Table/Column not exists anymore.
 */
public class StatisticsCleaner extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsCleaner.class);

    private OlapTable colStatsTbl;
    private OlapTable partitionColStatsTbl;

    private Map<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>> idToCatalog;
    private Map<Long, DatabaseIf<? extends TableIf>> idToDb;
    private Map<Long, TableIf> idToTbl;

    private Map<Long, MaterializedIndexMeta> idToMVIdx;

    public StatisticsCleaner() {
        super("Statistics Table Cleaner",
                TimeUnit.HOURS.toMillis(StatisticConstants.STATISTIC_CLEAN_INTERVAL_IN_HOURS));
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        clear();
    }

    public synchronized void clear() {
        try {
            if (!init()) {
                return;
            }
            clearStats(colStatsTbl, true);
            clearStats(partitionColStatsTbl, false);
        } finally {
            colStatsTbl = null;
            idToCatalog = null;
            idToDb = null;
            idToTbl = null;
            idToMVIdx = null;
        }
    }

    private void clearStats(OlapTable statsTbl, boolean isTableColumnStats) {
        ExpiredStats expiredStats;
        long offset = 0;
        do {
            expiredStats = new ExpiredStats();
            offset = findExpiredStats(statsTbl, expiredStats, offset, isTableColumnStats);
            deleteExpiredStats(expiredStats, statsTbl.getName(), isTableColumnStats);
        } while (!expiredStats.isEmpty());
    }

    private boolean init() {
        try {
            String dbName = FeConstants.INTERNAL_DB_NAME;
            colStatsTbl = (OlapTable) StatisticsUtil.findTable(InternalCatalog.INTERNAL_CATALOG_NAME,
                dbName, StatisticConstants.TABLE_STATISTIC_TBL_NAME);
            partitionColStatsTbl = (OlapTable) StatisticsUtil.findTable(InternalCatalog.INTERNAL_CATALOG_NAME,
                dbName, StatisticConstants.PARTITION_STATISTIC_TBL_NAME);
        } catch (Throwable t) {
            LOG.warn("Failed to init stats cleaner", t);
            return false;
        }

        idToCatalog = Env.getCurrentEnv().getCatalogMgr().getIdToCatalog();
        idToDb = constructDbMap();
        idToTbl = constructTblMap();
        idToMVIdx = constructIdxMap();
        return true;
    }

    private Map<Long, DatabaseIf<? extends TableIf>> constructDbMap() {
        Map<Long, DatabaseIf<? extends TableIf>> idToDb = Maps.newHashMap();
        for (CatalogIf<? extends DatabaseIf<? extends TableIf>> ctl : idToCatalog.values()) {
            for (DatabaseIf<? extends TableIf> db : ctl.getAllDbs()) {
                idToDb.put(db.getId(), db);
            }
        }
        return idToDb;
    }

    private Map<Long, TableIf> constructTblMap() {
        Map<Long, TableIf> idToTbl = new HashMap<>();
        for (DatabaseIf<? extends TableIf> db : idToDb.values()) {
            for (TableIf tbl : db.getTables()) {
                idToTbl.put(tbl.getId(), tbl);
            }
        }
        return idToTbl;
    }

    private Map<Long, MaterializedIndexMeta> constructIdxMap() {
        Map<Long, MaterializedIndexMeta> idToMVIdx = new HashMap<>();
        for (TableIf t : idToTbl.values()) {
            if (t instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) t;
                olapTable.getCopyOfIndexIdToMeta()
                        .entrySet()
                        .stream()
                        .filter(idx -> idx.getValue().getDefineStmt() != null)
                        .forEach(e -> idToMVIdx.put(e.getKey(), e.getValue()));
            }
        }
        return idToMVIdx;
    }

    private void deleteExpiredStats(ExpiredStats expiredStats, String tblName, boolean isTableColumnStats) {
        doDelete("catalog_id", expiredStats.expiredCatalog.stream()
                        .map(String::valueOf).collect(Collectors.toList()),
                FeConstants.INTERNAL_DB_NAME + "." + tblName);
        doDelete("db_id", expiredStats.expiredDatabase.stream()
                        .map(String::valueOf).collect(Collectors.toList()),
                FeConstants.INTERNAL_DB_NAME + "." + tblName);
        doDelete("tbl_id", expiredStats.expiredTable.stream()
                        .map(String::valueOf).collect(Collectors.toList()),
                FeConstants.INTERNAL_DB_NAME + "." + tblName);
        doDelete("idx_id", expiredStats.expiredIdxId.stream()
                        .map(String::valueOf).collect(Collectors.toList()),
                FeConstants.INTERNAL_DB_NAME + "." + tblName);
        // Partition level column stats doesn't need to do the following deletion.
        // 1. For invalid part id, do it in next analyze
        // 2. Partition stats table doesn't have id column.
        if (isTableColumnStats) {
            doDelete("part_id", expiredStats.expiredPartitionId.stream()
                    .map(String::valueOf).collect(Collectors.toList()),
                    FeConstants.INTERNAL_DB_NAME + "." + tblName);
            doDelete("id", expiredStats.ids.stream()
                    .map(String::valueOf).collect(Collectors.toList()),
                    FeConstants.INTERNAL_DB_NAME + "." + tblName);
        }
    }

    private void doDelete(String colName, List<String> pred, String tblName) {
        String deleteTemplate = "DELETE FROM " + tblName + " WHERE ${left} IN (${right})";
        if (CollectionUtils.isEmpty(pred)) {
            return;
        }
        String right = pred.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
        Map<String, String> params = new HashMap<>();
        params.put("left", colName);
        params.put("right", right);
        String sql = new StringSubstitutor(params).replace(deleteTemplate);
        try {
            StatisticsUtil.execUpdate(sql);
        } catch (Exception e) {
            LOG.warn("Failed to delete expired stats!", e);
        }
    }

    private long findExpiredStats(OlapTable statsTbl, ExpiredStats expiredStats,
                                  long offset, boolean isTableColumnStats) {
        long pos = offset;
        while (pos < statsTbl.getRowCount() && !expiredStats.isFull()) {
            List<ResultRow> rows = StatisticsRepository.fetchStatsFullName(
                    StatisticConstants.FETCH_LIMIT, pos, isTableColumnStats);
            pos += StatisticConstants.FETCH_LIMIT;
            for (ResultRow r : rows) {
                try {
                    StatsId statsId = new StatsId(r);
                    String id = statsId.id;
                    long catalogId = statsId.catalogId;
                    if (!idToCatalog.containsKey(catalogId)) {
                        expiredStats.expiredCatalog.add(catalogId);
                        continue;
                    }
                    long dbId = statsId.dbId;
                    if (!idToDb.containsKey(dbId)) {
                        expiredStats.expiredDatabase.add(dbId);
                        continue;
                    }
                    long tblId = statsId.tblId;
                    if (!idToTbl.containsKey(tblId)) {
                        expiredStats.expiredTable.add(tblId);
                        continue;
                    }

                    long idxId = statsId.idxId;
                    if (idxId != -1 && !idToMVIdx.containsKey(idxId)) {
                        expiredStats.expiredIdxId.add(idxId);
                        continue;
                    }

                    TableIf t = idToTbl.get(tblId);
                    String colId = statsId.colId;
                    if (!StatisticsUtil.isMvColumn(t, colId) && t.getColumn(colId) == null) {
                        expiredStats.ids.add(id);
                        continue;
                    }
                    if (!(t instanceof OlapTable)) {
                        continue;
                    }
                    // part_id should always be NULL in column_statistics table.
                    String partId = statsId.partId;
                    if (partId == null) {
                        continue;
                    }
                    expiredStats.expiredPartitionId.add(partId);
                } catch (Exception e) {
                    LOG.warn("Error occurred when retrieving expired stats", e);
                }
            }
            this.yieldForOtherTask();
        }
        return pos;
    }

    private static class ExpiredStats {
        Set<Long> expiredCatalog = new HashSet<>();
        Set<Long> expiredDatabase = new HashSet<>();
        Set<Long> expiredTable = new HashSet<>();
        Set<Long> expiredIdxId = new HashSet<>();
        Set<String> expiredPartitionId = new HashSet<>();
        Set<String> ids = new HashSet<>();

        public boolean isFull() {
            return expiredCatalog.size() >= Config.max_allowed_in_element_num_of_delete
                    || expiredDatabase.size() >= Config.max_allowed_in_element_num_of_delete
                    || expiredTable.size() >= Config.max_allowed_in_element_num_of_delete
                    || expiredIdxId.size() >= Config.max_allowed_in_element_num_of_delete
                    || expiredPartitionId.size() >= Config.max_allowed_in_element_num_of_delete
                    || ids.size() >= Config.max_allowed_in_element_num_of_delete;
        }

        public boolean isEmpty() {
            return expiredCatalog.isEmpty()
                    && expiredDatabase.isEmpty()
                    && expiredTable.isEmpty()
                    && expiredIdxId.isEmpty()
                    && expiredPartitionId.isEmpty()
                    && ids.size() < Config.max_allowed_in_element_num_of_delete / 10;
        }
    }

    // Avoid this task takes too much IO.
    private void yieldForOtherTask() {
        try {
            Thread.sleep(StatisticConstants.FETCH_INTERVAL_IN_MS);
        } catch (InterruptedException t) {
            // IGNORE
        }
    }

}
