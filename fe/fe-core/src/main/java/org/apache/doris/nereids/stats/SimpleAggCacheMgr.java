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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.LongSupplier;

/**
 * Async cache that stores exact MIN/MAX/COUNT values for OlapTable,
 * used by {@code RewriteSimpleAggToConstantRule} to replace simple
 * aggregations with constant values.
 *
 * <p>MIN/MAX values are obtained by executing
 * {@code SELECT min(col), max(col) FROM table}, and COUNT values by
 * {@code SELECT count(*) FROM table}, both as internal SQL queries
 * inside FE. Results are cached with a version stamp derived from
 * {@code OlapTable.getVisibleVersionTime()}.
 * When a caller provides a versionTime newer than the cached versionTime,
 * the stale entry is evicted and a background reload is triggered.
 *
 * <p>Cache validation uses a two-level check driven by {@code versionTime}:
 * <ol>
 *   <li>If times differ: the table has definitely changed — evict immediately,
 *       without calling {@code getVisibleVersion()} (which may involve an RPC in cloud mode).</li>
 *   <li>If times match: call {@code getVisibleVersion()} once to compare versions,
 *       guarding against the rare case of two writes completing within the same millisecond.</li>
 * </ol>
 *
 * <p>Only numeric and date/datetime columns are cached for MIN/MAX;
 * aggregated columns are skipped.
 */
public class SimpleAggCacheMgr {

    // ======================== Public inner types ========================

    /**
     * Holds exact min and max values for a column as strings.
     */
    public static class ColumnMinMax {
        private final String minValue;
        private final String maxValue;

        public ColumnMinMax(String minValue, String maxValue) {
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        public String minValue() {
            return minValue;
        }

        public String maxValue() {
            return maxValue;
        }
    }

    /**
     * Cache key identifying a column by its table ID and column name.
     */
    public static final class ColumnMinMaxKey {
        private final long tableId;
        private final String columnName;

        public ColumnMinMaxKey(long tableId, String columnName) {
            this.tableId = tableId;
            this.columnName = columnName;
        }

        public long getTableId() {
            return tableId;
        }

        public String getColumnName() {
            return columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ColumnMinMaxKey)) {
                return false;
            }
            ColumnMinMaxKey that = (ColumnMinMaxKey) o;
            return tableId == that.tableId && columnName.equalsIgnoreCase(that.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, columnName.toLowerCase());
        }

        @Override
        public String toString() {
            return "ColumnMinMaxKey{tableId=" + tableId + ", column=" + columnName + "}";
        }
    }

    private static class CacheValue {
        private final ColumnMinMax minMax;
        private final long version;
        private final long versionTime;

        CacheValue(ColumnMinMax minMax, long version, long versionTime) {
            this.minMax = minMax;
            this.version = version;
            this.versionTime = versionTime;
        }

        ColumnMinMax minMax() {
            return minMax;
        }

        long version() {
            return version;
        }

        long versionTime() {
            return versionTime;
        }
    }

    /**
     * Cached row count with version stamp.
     */
    private static class RowCountValue {
        private final long rowCount;
        private final long version;
        private final long versionTime;

        RowCountValue(long rowCount, long version, long versionTime) {
            this.rowCount = rowCount;
            this.version = version;
            this.versionTime = versionTime;
        }

        long rowCount() {
            return rowCount;
        }

        long version() {
            return version;
        }

        long versionTime() {
            return versionTime;
        }
    }

    private static final Logger LOG = LogManager.getLogger(SimpleAggCacheMgr.class);

    private static volatile SimpleAggCacheMgr INSTANCE;
    private static volatile SimpleAggCacheMgr TEST_INSTANCE;

    private final AsyncLoadingCache<ColumnMinMaxKey, Optional<CacheValue>> cache;
    private final AsyncLoadingCache<Long, Optional<RowCountValue>> rowCountCache;

    /**
     * Protected no-arg constructor for test subclassing.
     * Subclasses override {@link #getStats}, {@link #getRowCount}, etc.
     */
    protected SimpleAggCacheMgr() {
        this.cache = null;
        this.rowCountCache = null;
    }

    private SimpleAggCacheMgr(ExecutorService executor) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(Config.stats_cache_size)
                .executor(executor)
                .buildAsync(new CacheLoader());
        this.rowCountCache = Caffeine.newBuilder()
                .maximumSize(Config.stats_cache_size)
                .executor(executor)
                .buildAsync(new RowCountLoader());
    }

    private static SimpleAggCacheMgr getInstance() {
        if (INSTANCE == null) {
            synchronized (SimpleAggCacheMgr.class) {
                if (INSTANCE == null) {
                    ExecutorService executor = ThreadPoolManager.newDaemonCacheThreadPool(
                            4, "simple-agg-cache-pool", true);
                    INSTANCE = new SimpleAggCacheMgr(executor);
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Returns the singleton instance backed by async-loading cache,
     * or the test override if one has been set.
     */
    public static SimpleAggCacheMgr internalInstance() {
        SimpleAggCacheMgr test = TEST_INSTANCE;
        if (test != null) {
            return test;
        }
        return getInstance();
    }

    /**
     * Override used only in unit tests to inject a mock implementation.
     */
    @VisibleForTesting
    public static void setTestInstance(SimpleAggCacheMgr instance) {
        TEST_INSTANCE = instance;
    }

    /**
     * Reset the test override so that subsequent calls go back to the real cache.
     */
    @VisibleForTesting
    public static void clearTestInstance() {
        TEST_INSTANCE = null;
    }

    /**
     * Get the cached min/max for a column.
     *
     * <p>Cache validation uses a two-level check driven by {@code versionTime}:
     * <ol>
     *   <li>If {@code cachedVersionTime != callerVersionTime}: times differ, the table has
     *       definitely been modified — evict immediately. {@code versionSupplier} is
     *       <em>never</em> called, saving a potentially expensive RPC in cloud mode.</li>
     *   <li>If {@code cachedVersionTime == callerVersionTime}: times match, but two writes
     *       in the same millisecond could produce the same time with different versions.
     *       {@code versionSupplier} is called exactly once to confirm {@code version} equality.</li>
     * </ol>
     *
     * @param key               cache key (tableId + columnName)
     * @param callerVersionTime the caller's current {@code table.getVisibleVersionTime()} (cheap, always local)
     * @param versionSupplier   lazy supplier for {@code table.getVisibleVersion()}; called only when
     *                          {@code versionTime} values are equal. Returns -1 on RPC failure,
     *                          in which case the method returns empty conservatively.
     */
    public Optional<ColumnMinMax> getStats(ColumnMinMaxKey key, long callerVersionTime,
            LongSupplier versionSupplier) {
        CompletableFuture<Optional<CacheValue>> future = cache.get(key);
        if (future.isDone()) {
            try {
                Optional<CacheValue> cacheValue = future.get();
                if (cacheValue.isPresent()) {
                    CacheValue value = cacheValue.get();
                    if (value.versionTime() == callerVersionTime) {
                        // Times match — verify version to guard against two writes in the same ms.
                        // versionSupplier may be an RPC in cloud mode; it is only invoked here.
                        long callerVersion = versionSupplier.getAsLong();
                        if (callerVersion < 0) {
                            // RPC failed: cannot verify version — return empty conservatively.
                            // Do not invalidate: keep the existing entry so it can be reused once the RPC recovers.
                            return Optional.empty();
                        }
                        if (value.version() == callerVersion) {
                            return Optional.of(value.minMax());
                        }
                        // Same time but different version: two writes in the same ms — stale.
                    }
                    // Times differ → definitely stale; skip the version RPC entirely.
                }
                // Either empty (load failed) or stale — evict so next call triggers a fresh reload.
                cache.synchronous().invalidate(key);
            } catch (Exception e) {
                LOG.warn("Failed to get MinMax for column: {}, versionTime: {}", key, callerVersionTime, e);
                cache.synchronous().invalidate(key);
            }
        }
        return Optional.empty();
    }

    /**
     * Evict the cached stats for a column, if present. Used when we know the data has changed
     */
    public void removeStats(ColumnMinMaxKey key) {
        cache.synchronous().invalidate(key);
    }

    /**
     * Get the cached row count for a table.
     *
     * <p>Cache validation uses a two-level check driven by {@code versionTime}:
     * <ol>
     *   <li>If {@code cachedVersionTime != callerVersionTime}: times differ, the table has
     *       definitely been modified — evict immediately. {@code versionSupplier} is
     *       <em>never</em> called, saving a potentially expensive RPC in cloud mode.</li>
     *   <li>If {@code cachedVersionTime == callerVersionTime}: times match, but two writes
     *       in the same millisecond could produce the same time with different versions.
     *       {@code versionSupplier} is called exactly once to confirm {@code version} equality.</li>
     * </ol>
     *
     * @param tableId           the table id
     * @param callerVersionTime the caller's current {@code table.getVisibleVersionTime()} (cheap, always local)
     * @param versionSupplier   lazy supplier for {@code table.getVisibleVersion()}; called only when
     *                          {@code versionTime} values are equal. Returns -1 on RPC failure,
     *                          in which case the method returns empty conservatively.
     */
    public OptionalLong getRowCount(long tableId, long callerVersionTime, LongSupplier versionSupplier) {
        CompletableFuture<Optional<RowCountValue>> future = rowCountCache.get(tableId);
        if (future.isDone()) {
            try {
                Optional<RowCountValue> cached = future.get();
                if (cached.isPresent()) {
                    RowCountValue value = cached.get();
                    if (value.versionTime() == callerVersionTime) {
                        // Times match — verify version to guard against two writes in the same ms.
                        long callerVersion = versionSupplier.getAsLong();
                        if (callerVersion < 0) {
                            // RPC failed: cannot verify version — return empty conservatively.
                            // Do not invalidate: keep the existing entry so it can be reused once the RPC recovers.
                            return OptionalLong.empty();
                        }
                        if (value.version() == callerVersion) {
                            return OptionalLong.of(value.rowCount());
                        }
                        // Same time but different version: two writes in the same ms — stale.
                    }
                    // Times differ → definitely stale; skip the version RPC entirely.
                }
                // Either empty (load failed) or stale — evict so next call triggers a fresh reload.
                rowCountCache.synchronous().invalidate(tableId);
            } catch (Exception e) {
                LOG.warn("Failed to get row count for table: {}, versionTime: {}", tableId, callerVersionTime, e);
                rowCountCache.synchronous().invalidate(tableId);
            }
        }
        return OptionalLong.empty();
    }

    /**
     * Generate the internal SQL for fetching exact min/max values.
     */
    @VisibleForTesting
    public static String genMinMaxSql(List<String> qualifiers, String columnName) {
        // qualifiers: [catalogName, dbName, tableName]
        String quotedCol = "`" + StatisticsUtil.escapeColumnName(columnName) + "`";
        String fullTable = "`" + qualifiers.get(0) + "`.`"
                + qualifiers.get(1) + "`.`"
                + qualifiers.get(2) + "`";
        return "SELECT min(" + quotedCol + "), max(" + quotedCol + ") FROM " + fullTable;
    }

    /**
     * Generate the internal SQL for fetching exact row count.
     */
    @VisibleForTesting
    public static String genCountSql(List<String> qualifiers) {
        String fullTable = "`" + qualifiers.get(0) + "`.`"
                + qualifiers.get(1) + "`.`"
                + qualifiers.get(2) + "`";
        return "SELECT count(*) FROM " + fullTable;
    }

    /**
     * Async cache loader that issues internal SQL queries to compute exact min/max.
     */
    protected static final class CacheLoader
            implements AsyncCacheLoader<ColumnMinMaxKey, Optional<CacheValue>> {

        @Override
        public @NonNull CompletableFuture<Optional<CacheValue>> asyncLoad(
                @NonNull ColumnMinMaxKey key, @NonNull Executor executor) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return doLoad(key);
                } catch (Exception e) {
                    LOG.warn("Failed to load MinMax for column: {}", key, e);
                    return Optional.empty();
                }
            }, executor);
        }

        private Optional<CacheValue> doLoad(ColumnMinMaxKey key) throws Exception {
            // Look up the table by its ID
            TableIf tableIf = Env.getCurrentInternalCatalog().getTableByTableId(key.getTableId());
            if (!(tableIf instanceof OlapTable)) {
                return Optional.empty();
            }
            OlapTable olapTable = (OlapTable) tableIf;

            // Validate column exists and is eligible
            Column column = olapTable.getColumn(key.getColumnName());
            if (column == null) {
                return Optional.empty();
            }
            if (!column.getType().isNumericType() && !column.getType().isDateType()) {
                return Optional.empty();
            }
            if (column.isAggregated()) {
                return Optional.empty();
            }

            // Capture version and versionTime before the query.
            // Both are needed: versionTime (cheap) catches writes that cross a millisecond boundary;
            // version (may be RPC in cloud mode) is the ground truth for same-millisecond writes.
            long versionBefore = olapTable.getVisibleVersion();
            long versionTimeBefore = olapTable.getVisibleVersionTime();

            // Build and execute internal SQL
            List<String> qualifiers = olapTable.getFullQualifiers();
            String sql = genMinMaxSql(qualifiers, column.getName());

            List<ResultRow> rows;
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false)) {
                r.connectContext.getSessionVariable().setPipelineTaskNum("1");
                // Disable our own rule to prevent infinite recursion:
                // this internal SQL goes through Nereids and would otherwise trigger
                // RewriteSimpleAggToConstantRule again.
                r.connectContext.getSessionVariable().setDisableNereidsRules(
                        "REWRITE_SIMPLE_AGG_TO_CONSTANT");
                StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
                rows = stmtExecutor.executeInternalQuery();
            }
            if (rows == null || rows.isEmpty()) {
                return Optional.empty();
            }
            ResultRow row = rows.get(0);
            String minVal = row.get(0);
            String maxVal = row.get(1);
            if (minVal == null || maxVal == null) {
                return Optional.empty();
            }
            // Fast check: if versionTime changed, a write definitely occurred during the query.
            long versionTimeAfter = olapTable.getVisibleVersionTime();
            if (versionTimeAfter != versionTimeBefore) {
                return Optional.empty();
            }
            // Definitive check: compare version before and after the query.
            // A same-millisecond write would not be caught by versionTime alone but shows up here.
            long versionAfter = olapTable.getVisibleVersion();
            if (versionAfter != versionBefore) {
                return Optional.empty();
            }
            return Optional.of(new CacheValue(new ColumnMinMax(minVal, maxVal), versionAfter, versionTimeBefore));
        }
    }

    /**
     * Async cache loader that issues {@code SELECT count(*) FROM table}
     * to compute exact row counts.
     */
    protected static final class RowCountLoader
            implements AsyncCacheLoader<Long, Optional<RowCountValue>> {

        @Override
        public @NonNull CompletableFuture<Optional<RowCountValue>> asyncLoad(
                @NonNull Long tableId, @NonNull Executor executor) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return doLoad(tableId);
                } catch (Exception e) {
                    LOG.warn("Failed to load row count for table: {}", tableId, e);
                    return Optional.empty();
                }
            }, executor);
        }

        private Optional<RowCountValue> doLoad(Long tableId) throws Exception {
            TableIf tableIf = Env.getCurrentInternalCatalog().getTableByTableId(tableId);
            if (!(tableIf instanceof OlapTable)) {
                return Optional.empty();
            }
            OlapTable olapTable = (OlapTable) tableIf;

            // Capture version and versionTime before the query.
            long versionBefore = olapTable.getVisibleVersion();
            long versionTimeBefore = olapTable.getVisibleVersionTime();

            List<String> qualifiers = olapTable.getFullQualifiers();
            String sql = genCountSql(qualifiers);

            List<ResultRow> rows;
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false)) {
                r.connectContext.getSessionVariable().setPipelineTaskNum("1");
                // Disable our own rule to prevent infinite recursion:
                // this internal SQL goes through Nereids and would otherwise trigger
                // RewriteSimpleAggToConstantRule again.
                r.connectContext.getSessionVariable().setDisableNereidsRules(
                        "REWRITE_SIMPLE_AGG_TO_CONSTANT");
                StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
                rows = stmtExecutor.executeInternalQuery();
            }
            if (rows == null || rows.isEmpty()) {
                return Optional.empty();
            }
            String countStr = rows.get(0).get(0);
            if (countStr == null) {
                return Optional.empty();
            }
            long count = Long.parseLong(countStr);
            // Fast check: if versionTime changed, a write definitely occurred during the query.
            long versionTimeAfter = olapTable.getVisibleVersionTime();
            if (versionTimeAfter != versionTimeBefore) {
                return Optional.empty();
            }
            // Definitive check: compare version before and after the query.
            // A same-millisecond write would not be caught by versionTime alone but shows up here.
            long versionAfter = olapTable.getVisibleVersion();
            if (versionAfter != versionBefore) {
                return Optional.empty();
            }
            return Optional.of(new RowCountValue(count, versionAfter, versionTimeBefore));
        }
    }
}
