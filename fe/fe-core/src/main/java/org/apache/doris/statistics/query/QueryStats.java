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

package org.apache.doris.statistics.query;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.CleanQueryStatsInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QueryStats is used to record the query statistics of each table.
 * The statistics include the number of column/index/table hits in queries, the number of column hit in where clause,
 * The statistics are used to find hot table and columns.
 */
public class QueryStats {
    private static final Logger LOG = LogManager.getLogger(QueryStats.class);

    ConcurrentHashMap<Long, CatalogStats> catalogStats;
    TabletStats tabletStats;

    public QueryStats() {
        catalogStats = new ConcurrentHashMap<>();
        tabletStats = new TabletStats();
    }

    /**
     * Add query statistics columns
     */
    public void addStats(StatsDelta statsDelta) throws AnalysisException {
        long catalogId = statsDelta.getCatalog();
        CatalogStats c = catalogStats.get(catalogId);
        if (c == null) {
            c = new CatalogStats(catalogId);
            CatalogStats old = catalogStats.putIfAbsent(catalogId, c);
            if (old == null) {
                c.addStats(statsDelta);
            } else {
                old.addStats(statsDelta);
            }
        } else {
            c.addStats(statsDelta);
        }
    }

    /**
     * Add tablet statistics
     */
    public void addStats(List<Long> replicaIds) {
        tabletStats.addStats(replicaIds);
    }

    public long getQueryStats(long catalog, long database, long table) {
        CatalogStats c = catalogStats.get(catalog);
        if (c == null) {
            return 0;
        } else {
            return c.getQueryStats(database, table);
        }
    }

    public long getQueryStats(long catalog, long database, long table, long index) {
        CatalogStats c = catalogStats.get(catalog);
        if (c == null) {
            return 0;
        } else {
            return c.getQueryStats(database, table, index);
        }
    }

    public long getQueryStats(long catalog, long database, long table, long index, String column) {
        CatalogStats c = catalogStats.get(catalog);
        if (c == null) {
            return 0;
        } else {
            return c.getQueryStats(database, table, index, column);
        }
    }

    public long getFilterStats(long catalog, long database, long table, long index, String column) {
        CatalogStats c = catalogStats.get(catalog);
        if (c == null) {
            return 0;
        } else {
            return c.getFilterStats(database, table, index, column);
        }
    }

    public long getStats(long replicaId) {
        return tabletStats.getTabletQueryStats(replicaId);
    }

    public Map<String, Map> getStats(String catalog, boolean summary) throws AnalysisException {
        try {
            CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
            return getStats(c.getId(), summary);
        } catch (AnalysisException e) {
            LOG.info("get stats failed. catalog: {}", catalog, e);
            return new HashMap<>();
        }
    }

    public Map<String, Map> getStats(long catalog, boolean summary) throws AnalysisException {
        if (catalogStats.containsKey(catalog)) {
            return catalogStats.get(catalog).getStats(summary);
        } else {
            return new HashMap<>();
        }
    }

    public Map<String, Map> getStats(String catalog, String database, boolean summary) throws AnalysisException {
        try {
            CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
            DatabaseIf d = c.getDbOrAnalysisException(database);
            return getStats(c.getId(), d.getId(), summary);
        } catch (AnalysisException e) {
            LOG.info("get stats failed. catalog: {}, database: {}", catalog, database, e);
            return new HashMap<>();
        }
    }

    public Map<String, Map> getStats(long catalog, long database, boolean summary) throws AnalysisException {
        if (catalogStats.containsKey(catalog)) {
            return catalogStats.get(catalog).getStats(database, summary);
        } else {
            return new HashMap<>();
        }
    }

    public Map<String, Map> getStats(String catalog, String database, String table, boolean summary)
            throws AnalysisException {
        try {
            CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
            DatabaseIf d = c.getDbOrAnalysisException(database);
            TableIf t = d.getTableOrAnalysisException(table);
            return getStats(c.getId(), d.getId(), t.getId(), summary);
        } catch (AnalysisException e) {
            LOG.info("get stats failed. catalog: {}, database: {}, table: {}", catalog, database, table, e);
            return new HashMap<>();
        }
    }

    public Map<String, Map> getStats(long catalog, long database, long table, boolean summary)
            throws AnalysisException {
        if (catalogStats.containsKey(catalog)) {
            return catalogStats.get(catalog).getStats(database, table, summary);
        } else {
            return new HashMap<>();
        }
    }

    public Map<String, Map> getStats(String catalog, String database, String table, String index, boolean summary)
            throws AnalysisException {
        try {
            CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
            DatabaseIf d = c.getDbOrAnalysisException(database);
            TableIf t = d.getTableOrAnalysisException(table);
            long indexId = TableStats.DEFAULT_INDEX_ID;
            if (t.isManagedTable()) {
                indexId = ((OlapTable) t).getIndexIdByName(index);
            }
            return getStats(c.getId(), d.getId(), t.getId(), indexId, summary);
        } catch (AnalysisException e) {
            LOG.info("get stats failed. catalog: {}, database: {}, table: {}, index: {}", catalog, database, table,
                    index, e);
            return new HashMap<>();
        }
    }

    public Map<String, Map> getStats(long catalog, long database, long table, long index, boolean summary)
            throws AnalysisException {
        if (catalogStats.containsKey(catalog)) {
            return catalogStats.get(catalog).getStats(database, table, index, summary);
        } else {
            return new HashMap<>();
        }
    }

    public Map<String, Long> getCatalogStats(String catalog) throws AnalysisException {
        Map<String, Long> result = new LinkedHashMap<>();
        CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
        c.getDbNamesOrEmpty().forEach(dbName -> result.put(dbName.toString(), 0L));
        if (!catalogStats.containsKey(c.getId())) {
            return result;
        }
        for (Map.Entry<Long, DataBaseStats> entry : catalogStats.get(c.getId()).getDataBaseStats().entrySet()) {
            if (result.containsKey(entry.getKey())) {
                result.put(c.getDbOrAnalysisException(entry.getKey()).getFullName(), entry.getValue().getQueryStats());
            }
        }
        return result;
    }


    public Map<String, Long> getDbStats(String catalog, String db) throws AnalysisException {
        Map<String, Long> result = new LinkedHashMap<>();
        CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
        DatabaseIf d = c.getDbOrAnalysisException(db);
        d.getTableNamesOrEmptyWithLock().forEach(tblName -> result.put(tblName.toString(), 0L));
        if (!catalogStats.containsKey(c.getId())) {
            return result;
        }
        if (!catalogStats.get(c.getId()).getDataBaseStats().containsKey(d.getId())) {
            return result;
        }
        DataBaseStats dbStats = catalogStats.get(c.getId()).getDataBaseStats().get(d.getId());
        for (Map.Entry<Long, TableStats> entry : dbStats.getTableStats().entrySet()) {
            TableIf tableIf = d.getTableNullable(entry.getKey());
            if (tableIf == null) {
                continue;
            }
            if (result.containsKey(tableIf.getName())) {
                result.put(tableIf.getName(), entry.getValue().getQueryStats());
            }
        }
        return result;
    }

    public Map<String, Pair<Long, Long>> getTblStats(String catalog, String db, String tbl) throws AnalysisException {
        Map<String, Pair<Long, Long>> result = new LinkedHashMap<>();
        CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
        DatabaseIf d = c.getDbOrAnalysisException(db);
        TableIf t = d.getTableOrAnalysisException(tbl);
        d.getTableOrAnalysisException(tbl).getBaseSchema().forEach(col -> result.put(col.getName(), Pair.of(0L, 0L)));
        if (!catalogStats.containsKey(c.getId())) {
            return result;
        }
        if (!catalogStats.get(c.getId()).getDataBaseStats().containsKey(d.getId())) {
            return result;
        }
        if (!catalogStats.get(c.getId()).getDataBaseStats().get(d.getId()).getTableStats().containsKey(t.getId())) {
            return result;
        }

        ConcurrentHashMap<Long, IndexStats> indexStats = catalogStats.get(c.getId()).getDataBaseStats().get(d.getId())
                .getTableStats().get(t.getId()).getIndexStats();

        if (t.isManagedTable()) {
            for (Map.Entry<Long, IndexStats> entry : indexStats.entrySet()) {
                for (Map.Entry<String, AtomicLong> indexEntry : entry.getValue().getColumnQueryStats().entrySet()) {
                    if (result.containsKey(indexEntry.getKey())) {
                        result.get(indexEntry.getKey()).first += indexEntry.getValue().get();
                    }
                }
                for (Map.Entry<String, AtomicLong> indexEntry : entry.getValue().getColumnFilterStats().entrySet()) {
                    if (result.containsKey(indexEntry.getKey())) {
                        result.get(indexEntry.getKey()).second += indexEntry.getValue().get();
                    }
                }
            }
        } else {
            IndexStats stats = indexStats.get(TableStats.DEFAULT_INDEX_ID);
            for (Map.Entry<String, AtomicLong> entry : stats.getColumnQueryStats().entrySet()) {
                if (result.containsKey(entry.getKey())) {
                    result.get(entry.getKey()).first = entry.getValue().get();
                }
            }
            for (Map.Entry<String, AtomicLong> entry : stats.getColumnFilterStats().entrySet()) {
                if (result.containsKey(entry.getKey())) {
                    result.get(entry.getKey()).second = entry.getValue().get();
                }
            }
        }
        return result;
    }

    public Map<String, Long> getTblAllStats(String catalog, String db, String tbl) throws AnalysisException {
        Map<String, Long> result = new LinkedHashMap<>();
        CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
        DatabaseIf d = c.getDbOrAnalysisException(db);
        TableIf t = d.getTableOrAnalysisException(tbl);
        if (t.isManagedTable()) {
            ((OlapTable) t).getIndexNameToId().keySet().forEach(indexName -> result.put(indexName, 0L));
        } else {
            result.put(tbl, 0L);
        }
        if (!catalogStats.containsKey(c.getId())) {
            return result;
        }
        if (!catalogStats.get(c.getId()).getDataBaseStats().containsKey(d.getId())) {
            return result;
        }
        if (!catalogStats.get(c.getId()).getDataBaseStats().get(d.getId()).getTableStats().containsKey(t.getId())) {
            return result;
        }
        ConcurrentHashMap<Long, IndexStats> indexStats = catalogStats.get(c.getId()).getDataBaseStats().get(d.getId())
                .getTableStats().get(t.getId()).getIndexStats();
        if (t.isManagedTable()) {
            for (Map.Entry<Long, IndexStats> entry : indexStats.entrySet()) {
                result.put(((OlapTable) t).getIndexNameById(entry.getKey()), entry.getValue().getQueryStats());
            }
        } else {
            for (Map.Entry<Long, IndexStats> entry : indexStats.entrySet()) {
                result.put(tbl, entry.getValue().getQueryStats());
            }
        }
        return result;
    }

    public Map<String, Map<String, Pair<Long, Long>>> getTblAllVerboseStats(String catalog, String db, String tbl)
            throws AnalysisException {
        Map<String, Map<String, Pair<Long, Long>>> result = new LinkedHashMap<>();
        CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
        DatabaseIf d = c.getDbOrAnalysisException(db);
        TableIf t = d.getTableOrAnalysisException(tbl);
        if (t.isManagedTable()) {
            ((OlapTable) t).getIndexNameToId().forEach((indexName, indexId) -> {
                Map<String, Pair<Long, Long>> indexResult = new LinkedHashMap<>();
                ((OlapTable) t).getSchemaByIndexId(indexId)
                        .forEach(col -> indexResult.put(col.getName(), Pair.of(0L, 0L)));
                result.put(indexName, indexResult);
            });
        } else {
            Map<String, Pair<Long, Long>> indexResult = new LinkedHashMap<>();
            t.getBaseSchema().forEach(col -> indexResult.put(col.getName(), Pair.of(0L, 0L)));
            result.put(tbl, indexResult);
        }
        if (!catalogStats.containsKey(c.getId())) {
            return result;
        }
        if (!catalogStats.get(c.getId()).getDataBaseStats().containsKey(d.getId())) {
            return result;
        }
        if (!catalogStats.get(c.getId()).getDataBaseStats().get(d.getId()).getTableStats().containsKey(t.getId())) {
            return result;
        }
        ConcurrentHashMap<Long, IndexStats> indexStats = catalogStats.get(c.getId()).getDataBaseStats().get(d.getId())
                .getTableStats().get(t.getId()).getIndexStats();
        for (Map.Entry<Long, IndexStats> entry : indexStats.entrySet()) {
            String indexName = t.isManagedTable() ? ((OlapTable) t).getIndexNameById(entry.getKey()) : tbl;
            if (!result.containsKey(indexName)) {
                continue;
            }
            Map<String, Pair<Long, Long>> indexResult = result.get(indexName);
            for (Map.Entry<String, AtomicLong> indexEntry : entry.getValue().getColumnQueryStats().entrySet()) {
                if (indexResult.containsKey(indexEntry.getKey())) {
                    indexResult.get(indexEntry.getKey()).first = indexEntry.getValue().get();
                }
            }
            for (Map.Entry<String, AtomicLong> indexEntry : entry.getValue().getColumnFilterStats().entrySet()) {
                if (indexResult.containsKey(indexEntry.getKey())) {
                    indexResult.get(indexEntry.getKey()).second = indexEntry.getValue().get();
                }
            }
            if (t.isManagedTable()) {
                result.get(((OlapTable) t).getIndexNameById(entry.getKey())).putAll(indexResult);
            } else {
                result.put(tbl, indexResult);
            }
        }
        return result;
    }

    public void clear() {
        catalogStats.clear();
        tabletStats.clear();
    }

    public void clear(CleanQueryStatsInfo cleanQueryStatsInfo) throws DdlException {
        switch (cleanQueryStatsInfo.getScope()) {
            case ALL:
                clear(cleanQueryStatsInfo.getCatalog());
                break;
            case DB:
                clear(cleanQueryStatsInfo.getCatalog(), cleanQueryStatsInfo.getDbName());
                break;
            case TABLE:
                clear(cleanQueryStatsInfo.getCatalog(), cleanQueryStatsInfo.getDbName(),
                        cleanQueryStatsInfo.getTableName());
                break;
            default:
                throw new DdlException("Unknown scope: " + cleanQueryStatsInfo.getScope());
        }
    }

    public void clear(String catalog) {
        CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalog);
        if (c != null) {
            clear(c.getId());
        }
    }

    public void clear(long catalog) {
        catalogStats.remove(catalog);
        if (catalog == InternalCatalog.INTERNAL_CATALOG_ID) {
            tabletStats.clear();
        }
    }

    public void clear(String catalog, String database) {
        try {
            CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
            DatabaseIf d = c.getDbOrAnalysisException(database);
            clear(c.getId(), d.getId());
        } catch (AnalysisException e) {
            LOG.warn("Failed to clear query stats", e);
        }
    }

    public void clear(long catalog, long database) {
        catalogStats.computeIfPresent(catalog, (k, v) -> {
            v.clear(database);
            return v;
        });
    }

    public void clear(String catalog, String database, String table) {
        try {
            CatalogIf c = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalog);
            DatabaseIf d = c.getDbOrAnalysisException(database);
            TableIf t = d.getTableOrAnalysisException(table);
            clear(c.getId(), d.getId(), t.getId());
        } catch (AnalysisException e) {
            LOG.warn("Failed to clear query stats", e);
        }
    }

    public void clear(long catalog, long database, long table) {
        catalogStats.computeIfPresent(catalog, (k, v) -> {
            v.clear(database, table);
            return v;
        });
    }


    public void clear(long catalog, long database, long table, long index) {
        catalogStats.computeIfPresent(catalog, (k, v) -> {
            v.clear(database, table, index);
            return v;
        });
    }

    public void rename(long catalog, long database, long table, long index, String column, String newName) {
        catalogStats.computeIfPresent(catalog, (k, v) -> {
            v.rename(database, table, index, column, newName);
            return v;
        });
    }

}
