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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to store the query statistics of all tables in a database.
 */
public class DataBaseStats {
    private ConcurrentHashMap<Long, TableStats> tableStats;
    private CatalogIf catalog;
    private DatabaseIf db;

    public DatabaseIf getDb() {
        return db;
    }

    public DataBaseStats(CatalogIf catalog, long databaseId) throws AnalysisException {
        tableStats = new ConcurrentHashMap<>();
        this.catalog = catalog;
        this.db = catalog.getDbOrAnalysisException(databaseId);
    }

    /**
     * Add the query statistics of the table to the database.
     */
    public void addStats(StatsDelta statsDelta) throws AnalysisException {
        long tableId = statsDelta.getTable();
        TableStats t = tableStats.get(tableId);
        if (t == null) {
            t = new TableStats(db, tableId);
            TableStats old = tableStats.putIfAbsent(tableId, t);
            if (old == null) {
                t.addStats(statsDelta);
            } else {
                old.addStats(statsDelta);
            }
        } else {
            t.addStats(statsDelta);
        }
    }

    public ConcurrentHashMap<Long, TableStats> getTableStats() {
        return tableStats;
    }

    /**
     * Get the query statistics of the table.
     */
    public long getQueryStats(long table) {
        TableStats t = tableStats.get(table);
        if (t == null) {
            return 0;
        } else {
            return t.getQueryStats();
        }
    }

    /**
     * Get the query statistics of the index.
     */
    public long getQueryStats(long table, long index) {
        TableStats t = tableStats.get(table);
        if (t == null) {
            return 0;
        } else {
            return t.getQueryStats(index);
        }
    }

    /**
     * Get the query statistics of the column.
     */
    public long getQueryStats(long table, long index, String column) {
        TableStats t = tableStats.get(table);
        if (t == null) {
            return 0;
        } else {
            return t.getQueryStats(index, column);
        }
    }

    public long getQueryStats() {
        long total = 0;
        for (TableStats t : tableStats.values()) {
            total = total + t.getQueryStats();
        }
        return total;
    }

    /**
     * Get the filter statistics of the column.
     */
    public long getFilterStats(long table, long index, String column) {
        TableStats t = tableStats.get(table);
        if (t == null) {
            return 0;
        } else {
            return t.getFilterStats(index, column);
        }
    }

    /**
     * Get the filter statistics of the database.
     */
    public Map<String, Map> getStats(boolean summary) {
        Map<String, Map> stat = new HashMap<>();
        Map<String, Map> dstat = new HashMap<>();
        List<TableIf> tables = db.getTablesIgnoreException();
        stat.put("summary", ImmutableMap.of("query", getQueryStats()));

        for (TableIf table : tables) {
            if (tableStats.containsKey(table.getId())) {
                dstat.put(table.getName(), tableStats.get(table.getId()).getStats(summary));
            } else {
                dstat.put(table.getName(), new HashMap<>());
            }
        }
        stat.put("detail", dstat);
        return stat;
    }

    public Map<String, Map> getStats(long table, boolean summary) {
        if (tableStats.containsKey(table)) {
            return tableStats.get(table).getStats(summary);
        } else {
            return new HashMap<>();
        }
    }

    public Map<String, Map> getStats(long table, long index, boolean summary) {
        if (tableStats.containsKey(table)) {
            return tableStats.get(table).getStats(index, summary);
        } else {
            return new HashMap<>();
        }
    }

    public void clear() {
        tableStats.clear();
    }

    public void clear(long table) {
        tableStats.remove(table);
    }

    public void clear(long table, long index) {
        tableStats.computeIfPresent(table, (k, v) -> {
            v.clear(index);
            return v;
        });
    }

    public void rename(long table, long index, String column, String newName) {
        tableStats.computeIfPresent(table, (k, v) -> {
            v.rename(index, column, newName);
            return v;
        });
    }

}
