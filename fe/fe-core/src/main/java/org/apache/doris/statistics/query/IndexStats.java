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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.Util;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to store the query statistics of materialized views.
 */
public class IndexStats {
    private ConcurrentHashMap<String, AtomicLong> columnQueryStats;
    private ConcurrentHashMap<String, AtomicLong> columnFilterStats;
    private AtomicLong queryHit;
    private TableIf table;
    private long indexId;

    /**
     * The statistics of the column in the materialized view.
     */
    public IndexStats(TableIf table, long indexId) {
        this.indexId = indexId;
        columnQueryStats = new ConcurrentHashMap<>();
        columnFilterStats = new ConcurrentHashMap<>();
        queryHit = new AtomicLong(0L);
        this.table = table;
    }

    public TableIf getTable() {
        return table;
    }

    public ConcurrentHashMap<String, AtomicLong> getColumnQueryStats() {
        return columnQueryStats;
    }

    public ConcurrentHashMap<String, AtomicLong> getColumnFilterStats() {
        return columnFilterStats;
    }

    private void addStats(String column, ConcurrentHashMap<String, AtomicLong> stats) {
        AtomicLong l = stats.get(column);
        if (l == null) {
            l = new AtomicLong(0);
            AtomicLong old = stats.putIfAbsent(column, l);
            if (old == null) {
                l.updateAndGet(Util.overflowSafeIncrement());
            } else {
                old.updateAndGet(Util.overflowSafeIncrement());
            }
        } else {
            l.updateAndGet(Util.overflowSafeIncrement());
        }
    }

    /**
     * Add the query statistics of the column to the materialized view.
     */
    public void addStats(Map<String, ColumnStatsDelta> columnStats) {
        queryHit.updateAndGet(Util.overflowSafeIncrement());
        for (Map.Entry<String, ColumnStatsDelta> columnStat : columnStats.entrySet()) {
            if (columnStat.getValue().filterHit) {
                addFilterStats(columnStat.getKey());
            }
            if (columnStat.getValue().queryHit) {
                addQueryStats(columnStat.getKey());
            }
        }
    }

    public void addQueryStats(String column) {
        addStats(column, columnQueryStats);
    }

    public void addFilterStats(String column) {
        addStats(column, columnFilterStats);
    }

    /**
     * Get the query statistics of the column in the materialized view.
     */
    public long getQueryStats(String column) {
        AtomicLong l = columnQueryStats.get(column);
        if (l == null) {
            return 0;
        } else {
            return l.get();
        }
    }

    /**
     * Get the query statistics of the materialized view.
     */
    public Long getQueryStats() {
        return queryHit.get();
    }

    /**
     * Get the filter statistics of all columns in the materialized view.
     */
    public long getFilterStats(String column) {
        AtomicLong l = columnFilterStats.get(column);
        if (l == null) {
            return 0;
        } else {
            return l.get();
        }
    }

    /**
     * Get the maximum filter statistics of all columns in the materialized view.
     */
    public Map<String, Map> getStats(boolean summary) {
        List<Column> indexColumns;
        if (table.isManagedTable()) {
            OlapTable olapTable = (OlapTable) table;
            indexColumns = olapTable.getSchemaByIndexId(indexId);
        } else {
            indexColumns = table.getBaseSchema();
        }
        Map<String, Map> stat = new HashMap<>();
        stat.put("summary", ImmutableMap.of("query", getQueryStats()));
        if (!summary) {
            Map<String, Map> columnStats = new HashMap<>();

            for (Column column : indexColumns) {
                Map<String, Long> dstat = new HashMap<>();
                dstat.put("query", getQueryStats(column.getName()));
                dstat.put("filter", getFilterStats(column.getName()));
                columnStats.put(column.getName(), dstat);
            }
            stat.put("detail", columnStats);
        }
        return stat;
    }

    public void rename(String column, String newName) {
        AtomicLong queryStata = columnQueryStats.get(column);
        if (queryStata != null) {
            columnQueryStats.put(newName, queryStata);
            columnQueryStats.remove(column);
        }
        AtomicLong filterStats = columnFilterStats.get(column);
        if (filterStats != null) {
            columnFilterStats.put(newName, filterStats);
            columnFilterStats.remove(column);
        }
    }
}
