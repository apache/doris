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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

/**
 * Maintenance the internal statistics table.
 * Delete rows that corresponding DB/Table/Column not exists anymore.
 */
public class StatisticsCleaner extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsCleaner.class);

    private OlapTable colStatsTbl;
    private OlapTable histStatsTbl;

    private Map<Long, CatalogIf> idToCatalog;

    /* Internal DB only */
    private Map<Long, Database> idToDb;

    /* Internal tbl only */
    private Map<Long, Table> idToTbl;

    public StatisticsCleaner() {
        super("Statistics Table Cleaner",
                TimeUnit.HOURS.toMillis(StatisticConstants.STATISTIC_CLEAN_INTERVAL_IN_HOURS));
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!init()) {
            return;
        }
        clear(colStatsTbl);
        clear(histStatsTbl);
    }

    private void clear(OlapTable statsTbl) {
        ExpiredStats expiredStats = null;
        do {
            expiredStats = findExpiredStats(statsTbl);
        } while (!expiredStats.isEmpty());
    }

    private boolean init() {
        try {
            colStatsTbl =
                    (OlapTable) StatisticsUtil
                            .findTable(InternalCatalog.INTERNAL_CATALOG_NAME,
                                    FeConstants.INTERNAL_DB_NAME,
                                    StatisticConstants.STATISTIC_TBL_NAME);
            histStatsTbl =
                    (OlapTable) StatisticsUtil
                            .findTable(InternalCatalog.INTERNAL_CATALOG_NAME,
                                    FeConstants.INTERNAL_DB_NAME,
                                    StatisticConstants.HISTOGRAM_TBL_NAME);
        } catch (Throwable t) {
            LOG.warn("Failed to init stats cleaner", e);
            return false;
        }

        idToCatalog = Env.getCurrentEnv().getCatalogMgr().getIdToCatalog();
        idToDb = Env.getCurrentEnv().getInternalCatalog().getIdToDb();
        idToTbl = constructTblMap();
        return true;
    }

    private Map<Long, Table> constructTblMap() {
        Map<Long, Table> idToTbl = new HashMap<>();
        for (Database db : idToDb.values()) {
            idToTbl.putAll(db.getIdToTable());
        }
        return idToTbl;
    }

    private void deleteExpired(String colName, List<String> constants) {
        // TODO: must promise count of children of predicate is less than the FE limits.

        StringJoiner predicateBuilder = new StringJoiner(",", "(", ")");
        constants.forEach(predicateBuilder::add);
        Map<String, String> map = new HashMap<String, String>() {
            {
                put("colName", colName);
                put("predicate", predicateBuilder.toString());
            }
        };
        StringSubstitutor stringSubstitutor = new StringSubstitutor(map);
        try {
            StatisticsUtil.execUpdate(stringSubstitutor.replace(deleteTemplate));
        } catch (Exception e) {
            LOG.warn("Remove expired statistics failed", e);
        }
    }

    private void doDelete(List<Pair<String/*col name*/, List<String>/*constants*/>> conds) {
        String deleteTemplate = "DELETE FROM " + FeConstants.INTERNAL_DB_NAME
                + "." + StatisticConstants.STATISTIC_TBL_NAME + "WHERE ";
        String inPredicateTemplate = " ${colName} IN ${predicate}";
        for (Pair<String/*col name*/, List<String>/*constants*/> p : conds) {
            String.join("," )
        }
    }

    public ExpiredStats findExpiredStats(OlapTable statsTbl) {
        ExpiredStats expiredStats = new ExpiredStats();
        long rowCount = statsTbl.getRowCount();
        long pos = 0;
        while (pos < rowCount
                && !expiredStats.isFull()) {
            List<ResultRow> rows = StatisticsRepository.fetchStatsFullName(StatisticConstants.FETCH_LIMIT, pos);
            pos += StatisticConstants.FETCH_LIMIT;
            for (ResultRow r : rows) {
                try {
                    long catalogId = Long.parseLong(r.getColumnValue("catalog_id"));
                    if (!idToCatalog.containsKey(catalogId)) {
                        expiredStats.expiredCatalog.add(catalogId);
                    }
                    long dbId = Long.parseLong(r.getColumnValue("db_id"));
                    if (!idToDb.containsKey(dbId)) {
                        expiredStats.expiredDatabase.add(dbId);
                    }
                    long tblId = Long.parseLong(r.getColumnValue("tbl_id"));
                    if (!idToTbl.containsKey(tblId)) {
                        expiredStats.expiredDatabase.add(tblId);
                        continue;
                    }
                    Table t = idToTbl.get(tblId);
                    String colId = r.getColumnValue("col_id");
                    if (t.getColumn(colId) == null) {
                        expiredStats.expiredCol.add(Pair.of(tblId, colId));
                    }
                    if (!(t instanceof OlapTable)) {
                        continue;
                    }
                    OlapTable olapTable = (OlapTable) t;
                    long partId = Long.parseLong(r.getColumnValue("part_id"));
                    if (!olapTable.getPartitionIds().contains(partId)) {
                        expiredStats.expiredParts.add(partId);
                    }
                } catch (Exception e) {
                    // TODO
                }
            }
        }
        return expiredStats;
    }

    private static class ExpiredStats {
        Set<Long> expiredCatalog = new HashSet<>();
        Set<Long> expiredDatabase = new HashSet<>();
        Set<Long> expiredTable = new HashSet<>();
        Set<Long> expiredIdx = new HashSet<>();

        Set<Pair<Long, String>> expiredCol = new HashSet<>();

        Set<Long> expiredParts = new HashSet<>();

        public boolean isFull() {
            return expiredCatalog.size() >= Config.expr_children_limit
                    || expiredDatabase.size() >= Config.expr_children_limit
                    || expiredTable.size() >= Config.expr_children_limit
                    || expiredIdx.size() >= Config.expr_children_limit
                    || expiredCol.size() >= Config.expr_children_limit
                    || expiredParts.size() >= Config.expr_children_limit;
        }

        public boolean isEmpty() {
            return expiredCatalog.isEmpty()
                    && expiredDatabase.isEmpty()
                    && expiredTable.isEmpty()
                    && expiredIdx.isEmpty()
                    && expiredCol.size() < Config.expr_children_limit / 10
                    && expiredParts.size() < Config.expr_children_limit / 10;
        }
    }

}
