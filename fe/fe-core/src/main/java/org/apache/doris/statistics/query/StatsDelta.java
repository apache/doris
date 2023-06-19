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

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * StatsDelta is used to record the stats delta of a table or index
 */
public class StatsDelta {
    private long catalogId;
    private long databaseId;
    private long tableId;
    private long indexId;
    private HashMap<String, ColumnStatsDelta> columnStats;  // column name -> column stats
    private List<Long> tabletStats;

    public StatsDelta(long catalogId, long databaseId, long tableId, long indexId) {
        this.catalogId = catalogId;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.indexId = indexId;
        this.columnStats = new HashMap<>();
        this.tabletStats = new ArrayList<>();
    }

    public StatsDelta(long catalogId, long databaseId, long tableId, long indexId, List<Long> tabletStats) {
        this.catalogId = catalogId;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.indexId = indexId;
        this.columnStats = new HashMap<>();
        this.tabletStats = tabletStats;
    }

    public long getCatalog() {
        return catalogId;
    }

    public long getDatabase() {
        return databaseId;
    }

    public long getTable() {
        return tableId;
    }

    public long getIndex() {
        return indexId;
    }

    public HashMap<String, ColumnStatsDelta> getColumnStats() {
        return columnStats;
    }

    /**
     * add column stats to this delta
     */
    public void addColumnStats(String column, ColumnStatsDelta delta) {
        if (this.columnStats.containsKey(column)) {
            this.columnStats.get(column).queryHit |= delta.queryHit;
            this.columnStats.get(column).filterHit |= delta.filterHit;
        } else {
            this.columnStats.put(column, delta);
        }
    }

    public void addQueryStats(String column) {
        if (this.columnStats.containsKey(column)) {
            this.columnStats.get(column).queryHit = true;
        } else {
            this.columnStats.put(column, new ColumnStatsDelta(true, false));
        }
    }

    public void addFilterStats(String column) {
        if (this.columnStats.containsKey(column)) {
            this.columnStats.get(column).filterHit = true;
        } else {
            this.columnStats.put(column, new ColumnStatsDelta(false, true));
        }
    }

    /**
     * add column stats to this delta
     */
    public void addStats(String column, boolean queryHit, boolean filterHit) {
        if (this.columnStats.containsKey(column)) {
            this.columnStats.get(column).queryHit |= queryHit;
            this.columnStats.get(column).filterHit |= filterHit;
        } else {
            this.columnStats.put(column, new ColumnStatsDelta(queryHit, filterHit));
        }
    }

    public boolean empty() {
        return columnStats.isEmpty();
    }

    public List<Long> getTabletStats() {
        return tabletStats;
    }

    public void addTabletStats(List<Long> tabletIds) {
        this.tabletStats.addAll(tabletIds);
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
