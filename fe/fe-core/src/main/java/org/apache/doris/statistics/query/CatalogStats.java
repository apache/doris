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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CatalogStats is used to store the query statistics of the catalog.
 * The query statistics of the catalog is the sum of the query statistics of all databases.
 */
public class CatalogStats {
    private ConcurrentHashMap<Long, DataBaseStats> dataBaseStats;
    private CatalogIf catalog;

    public CatalogStats(long catalogId) throws AnalysisException {
        this.dataBaseStats = new ConcurrentHashMap<>();
        this.catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalogId);
    }

    public CatalogIf getCatalog() {
        return catalog;
    }

    /**
     * Add the query statistics of the database to the catalog.
     * If it does not exist, it will be created.
     */
    public void addStats(StatsDelta statsDelta) throws AnalysisException {
        long databaseId = statsDelta.getDatabase();
        DataBaseStats d = dataBaseStats.get(databaseId);
        if (d == null) {
            d = new DataBaseStats(catalog, databaseId);
            DataBaseStats old = dataBaseStats.putIfAbsent(databaseId, d);
            if (old == null) {
                d.addStats(statsDelta);
            } else {
                old.addStats(statsDelta);
            }
        } else {
            d.addStats(statsDelta);
        }
    }

    public ConcurrentHashMap<Long, DataBaseStats> getDataBaseStats() {
        return dataBaseStats;
    }

    /**
     * Get the query statistics of the table.
     * If it does not exist, return 0.
     */
    public long getQueryStats(long database, long table) {
        DataBaseStats d = dataBaseStats.get(database);
        if (d == null) {
            return 0;
        } else {
            return d.getQueryStats(table);
        }
    }

    /**
     * Get the query statistics of the index.
     * If it does not exist, return 0.
     */
    public long getQueryStats(long database, long table, long index) {
        DataBaseStats d = dataBaseStats.get(database);
        if (d == null) {
            return 0;
        } else {
            return d.getQueryStats(table, index);
        }
    }

    /**
     * Get the query statistics of the column.
     * If it does not exist, return 0.
     */
    public long getQueryStats(long database, long table, long index, String column) {
        DataBaseStats d = dataBaseStats.get(database);
        if (d == null) {
            return 0;
        } else {
            return d.getQueryStats(table, index, column);
        }
    }

    /**
     * Get the query statistics of the database.
     * If it does not exist, return 0.
     */
    public long getQueryStats(long database) {
        DataBaseStats d = dataBaseStats.get(database);
        if (d == null) {
            return 0;
        } else {
            return d.getQueryStats();
        }
    }

    /**
     * Get the all query statistics of the catalog.
     * If it does not exist, return 0.
     */
    public long getQueryStats() {
        long total = 0;
        for (DataBaseStats d : dataBaseStats.values()) {
            total = total + d.getQueryStats();
        }
        return total;
    }

    /**
     * Get the filter statistics of the column.
     * If it does not exist, return 0.
     */
    public long getFilterStats(long database, long table, long index, String column) {
        DataBaseStats d = dataBaseStats.get(database);
        if (d == null) {
            return 0;
        } else {
            return d.getFilterStats(table, index, column);
        }
    }

    /**
     * Get the all statistics of all the catalog.
     * If it does not exist, return 0.
     */
    public Map<String, Map> getStats(boolean summary) throws AnalysisException {
        Map<String, Map> stat = new HashMap<>();
        Map<String, Map> dstat = new HashMap<>();

        stat.put("summary", ImmutableMap.of("query", getQueryStats()));
        List<Long> dbIds = catalog.getDbIds();
        for (long dbId : dbIds) {
            String dbName = catalog.getDbOrAnalysisException(dbId).getFullName();
            if (dataBaseStats.containsKey(dbId)) {
                dstat.put(dbName, dataBaseStats.get(dbId).getStats(summary));
            } else {
                dstat.put(dbName, new HashMap<>());
            }
        }
        stat.put("detail", dstat);
        return stat;
    }

    /**
     * Get the all statistics of the database.
     * If it does not exist, return empty.
     */
    public Map<String, Map> getStats(long database, boolean summary) throws AnalysisException {
        if (dataBaseStats.containsKey(database)) {
            return dataBaseStats.get(database).getStats(summary);
        } else {
            return new HashMap<>();
        }
    }

    /**
     * Get the all statistics of the table.
     * If it does not exist, return empty.
     */
    public Map<String, Map> getStats(long database, long table, boolean summary) throws AnalysisException {
        if (dataBaseStats.containsKey(database)) {
            return dataBaseStats.get(database).getStats(table, summary);
        } else {
            return new HashMap<>();
        }
    }

    /**
     * Get the all statistics of the index.
     * If it does not exist, return empty.
     */
    public Map<String, Map> getStats(long database, long table, long index, boolean summary)
            throws AnalysisException {
        if (dataBaseStats.containsKey(database)) {
            return dataBaseStats.get(database).getStats(table, index, summary);
        } else {
            return new HashMap<>();
        }
    }

    public void clear() {
        dataBaseStats.clear();
    }

    // remove database if exist
    public void clear(long database) {
        dataBaseStats.remove(database);
    }

    public void clear(long database, long table) {
        dataBaseStats.computeIfPresent(database, (k, v) -> {
            v.clear(table);
            return v;
        });
    }

    public void clear(long database, long table, long index) {
        dataBaseStats.computeIfPresent(database, (k, v) -> {
            v.clear(table, index);
            return v;
        });
    }

    public void rename(long database, long table, long index, String column, String newName) {
        dataBaseStats.computeIfPresent(database, (k, v) -> {
            v.rename(table, index, column, newName);
            return v;
        });
    }
}
