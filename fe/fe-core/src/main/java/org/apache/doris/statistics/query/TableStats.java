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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats is used to store the query stats of a table.
 * It contains a map of index stats.
 */
public class TableStats {
    public static final long DEFAULT_INDEX_ID = -1L;
    private ConcurrentHashMap<Long, IndexStats> indexStats;
    private long tableId;
    private TableIf table;

    public TableStats(DatabaseIf db, long tableId) throws AnalysisException {
        this.tableId = tableId;
        indexStats = new ConcurrentHashMap<>();
        table = db.getTableOrAnalysisException(tableId);
    }

    public TableIf getTable() {
        return table;
    }

    public ConcurrentHashMap<Long, IndexStats> getIndexStats() {
        return indexStats;
    }

    /**
     * Add the query statistics of the index to the table.
     */
    public void addStats(StatsDelta statsDelta) {
        long indexId = statsDelta.getIndex();
        IndexStats i = indexStats.get(indexId);
        if (i == null) {
            i = new IndexStats(table, indexId);
            IndexStats old = indexStats.putIfAbsent(indexId, i);
            if (old == null) {
                i.addStats(statsDelta.getColumnStats());
            } else {
                old.addStats(statsDelta.getColumnStats());
            }
        } else {
            i.addStats(statsDelta.getColumnStats());
        }
    }

    /**
     * Get the query statistics of the table.
     */
    public long getQueryStats() {
        long total = 0;
        for (IndexStats i : indexStats.values()) {
            total = total + i.getQueryStats();
        }
        return total;
    }

    /**
     * Get the query statistics of the index.
     */
    public long getQueryStats(long index) {
        IndexStats i = indexStats.get(index);
        if (i == null) {
            return 0;
        } else {
            return i.getQueryStats();
        }
    }

    /**
     * Get the query statistics of the column.
     */
    public long getQueryStats(long index, String column) {
        IndexStats i = indexStats.get(index);
        if (i == null) {
            return 0;
        } else {
            return i.getQueryStats(column);
        }
    }

    /**
     * Get the filter statistics of the column.
     */
    public long getFilterStats(long index, String column) {
        IndexStats i = indexStats.get(index);
        if (i == null) {
            return 0;
        } else {
            return i.getFilterStats(column);
        }
    }

    /**
     * Get the query statistics of the column.
     */
    public Map<String, Map> getStats(boolean summary) {
        Map<String, Map> stat = new HashMap<>();
        stat.put("summary", ImmutableMap.of("query", getQueryStats()));
        Map<String, Map> dstat = new HashMap<>();
        if (table.isManagedTable()) {
            OlapTable olapTable = (OlapTable) table;
            for (Map.Entry<String, Long> entry : olapTable.getIndexNameToId().entrySet()) {
                if (indexStats.containsKey(entry.getValue())) {
                    dstat.put(entry.getKey(), indexStats.get(entry.getValue()).getStats(summary));
                } else {
                    dstat.put(entry.getKey(), new HashMap());
                }
            }
        } else {
            if (indexStats.containsKey(DEFAULT_INDEX_ID)) {
                dstat.put(table.getName(), indexStats.get(DEFAULT_INDEX_ID).getStats(summary));
            } else {
                dstat.put(table.getName(), new HashMap());
            }
        }
        stat.put("detail", dstat);
        return stat;
    }

    public Map<String, Map> getStats(long index, boolean summary) {
        if (indexStats.containsKey(index)) {
            return indexStats.get(index).getStats(summary);
        } else {
            return new HashMap<>();
        }
    }

    public void clear() {
        indexStats.clear();
    }

    public void clear(long index) {
        indexStats.remove(index);
    }

    public void rename(long index, String column, String newName) {

        indexStats.computeIfPresent(index, (k, v) -> {
            v.rename(column, newName);
            return v;
        });
    }
}
