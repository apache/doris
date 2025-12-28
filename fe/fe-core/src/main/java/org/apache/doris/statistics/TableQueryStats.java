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

import org.apache.doris.info.TableNameInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Track query statistics for tables to support priority-based auto analysis.
 * This class maintains query frequency, last query time, and other metrics
 * to help determine which tables should be analyzed first.
 */
public class TableQueryStats {
    private static final Logger LOG = LogManager.getLogger(TableQueryStats.class);

    // Singleton instance
    private static final TableQueryStats INSTANCE = new TableQueryStats();

    // Statistics for each table: TableNameInfo -> TableStats
    private final ConcurrentHashMap<TableNameInfo, TableStats> tableStatsMap = new ConcurrentHashMap<>();

    // Time window for query counting (default: 1 hour)
    private static final long QUERY_COUNT_WINDOW_MS = 3600_000L; // 1 hour

    // Decay factor for query count (reduce weight of old queries)
    private static final double QUERY_COUNT_DECAY_FACTOR = 0.9;

    private TableQueryStats() {
    }

    public static TableQueryStats getInstance() {
        return INSTANCE;
    }

    /**
     * Record a query for a table.
     * This should be called when a table is accessed in a query.
     */
    public void recordQuery(TableNameInfo tableNameInfo) {
        if (tableNameInfo == null) {
            return;
        }
        TableStats stats = tableStatsMap.computeIfAbsent(tableNameInfo, k -> new TableStats());
        stats.recordQuery();
    }

    /**
     * Get query count for a table in the recent time window.
     */
    public long getQueryCount(TableNameInfo tableNameInfo) {
        TableStats stats = tableStatsMap.get(tableNameInfo);
        if (stats == null) {
            return 0;
        }
        return stats.getQueryCount();
    }

    /**
     * Get last query time for a table.
     */
    public long getLastQueryTime(TableNameInfo tableNameInfo) {
        TableStats stats = tableStatsMap.get(tableNameInfo);
        if (stats == null) {
            return 0;
        }
        return stats.getLastQueryTime();
    }

    /**
     * Calculate priority score for a table based on query frequency and recency.
     * Higher score means higher priority.
     */
    public double calculatePriorityScore(TableNameInfo tableNameInfo) {
        TableStats stats = tableStatsMap.get(tableNameInfo);
        if (stats == null) {
            return 0.0;
        }

        long queryCount = stats.getQueryCount();
        long lastQueryTime = stats.getLastQueryTime();
        long currentTime = System.currentTimeMillis();
        long timeSinceLastQuery = currentTime - lastQueryTime;

        // Base score from query count (with decay)
        double queryScore = queryCount * Math.pow(QUERY_COUNT_DECAY_FACTOR,
                timeSinceLastQuery / (QUERY_COUNT_WINDOW_MS * 10.0));

        // Recency bonus: more recent queries get higher priority
        double recencyScore = 0.0;
        if (timeSinceLastQuery < QUERY_COUNT_WINDOW_MS) {
            recencyScore = (QUERY_COUNT_WINDOW_MS - timeSinceLastQuery) / (double) QUERY_COUNT_WINDOW_MS;
        }

        // Combined score: query frequency (70%) + recency (30%)
        return queryScore * 0.7 + recencyScore * 100.0 * 0.3;
    }

    /**
     * Clean up old statistics to prevent memory leak.
     * Remove tables that haven't been queried for a long time.
     */
    public void cleanup(long maxIdleTimeMs) {
        long currentTime = System.currentTimeMillis();
        tableStatsMap.entrySet().removeIf(entry -> {
            TableStats stats = entry.getValue();
            long idleTime = currentTime - stats.getLastQueryTime();
            if (idleTime > maxIdleTimeMs) {
                LOG.debug("Removing idle table stats: {}, idle time: {} ms",
                        entry.getKey(), idleTime);
                return true;
            }
            return false;
        });
    }

    /**
     * Clear all statistics (for testing or reset).
     */
    public void clear() {
        tableStatsMap.clear();
    }

    /**
     * Get size of statistics map.
     */
    public int size() {
        return tableStatsMap.size();
    }

    /**
     * Internal class to track statistics for a single table.
     */
    private static class TableStats {
        // Total query count (with decay)
        private final LongAdder queryCount = new LongAdder();
        
        // Last query time
        private final AtomicLong lastQueryTime = new AtomicLong(System.currentTimeMillis());

        // Last cleanup time for decay calculation
        private volatile long lastCleanupTime = System.currentTimeMillis();

        void recordQuery() {
            queryCount.increment();
            lastQueryTime.set(System.currentTimeMillis());
        }

        long getQueryCount() {
            // Apply decay if needed
            long currentTime = System.currentTimeMillis();
            long timeSinceCleanup = currentTime - lastCleanupTime;
            if (timeSinceCleanup > QUERY_COUNT_WINDOW_MS) {
                // Apply decay
                long currentCount = queryCount.sum();
                long decayedCount = (long) (currentCount * Math.pow(QUERY_COUNT_DECAY_FACTOR,
                        timeSinceCleanup / (double) QUERY_COUNT_WINDOW_MS));
                queryCount.reset();
                queryCount.add(Math.max(decayedCount, 0));
                lastCleanupTime = currentTime;
            }
            return queryCount.sum();
        }

        long getLastQueryTime() {
            return lastQueryTime.get();
        }
    }
}

