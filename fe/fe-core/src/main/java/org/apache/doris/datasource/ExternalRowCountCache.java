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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheRemovalReason;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.cache.BasicAsyncCacheLoader;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.LongFunction;

public class ExternalRowCountCache {

    private static final Logger LOG = LogManager.getLogger(ExternalRowCountCache.class);
    private final ExecutorService executor;
    private final LongFunction<CatalogMetaCache> rootResolver;
    private final RowCountCacheLoader loader = new RowCountCacheLoader();
    private final Map<Long, Entry> entries = new ConcurrentHashMap<>();
    // A process-wide retention policy, not a second metadata store. Values/futures belong exclusively to
    // catalog roots. Keep the existing global count bound while invalidation uses root resource scopes.
    private final Cache<RowCountKey, Retention> retention = Caffeine.newBuilder()
            .maximumSize(Config.max_external_table_row_count_cache_num)
            .removalListener((RowCountKey key, Retention token,
                    com.github.benmanes.caffeine.cache.RemovalCause cause) -> retire(token))
            .build();

    public ExternalRowCountCache(ExecutorService executor) {
        this(executor, id -> {
            CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(id);
            return catalog instanceof ExternalCatalog ? ((ExternalCatalog) catalog).getFeMetaCacheOwner() : null;
        });
    }

    ExternalRowCountCache(ExecutorService executor, LongFunction<CatalogMetaCache> rootResolver) {
        this.executor = executor;
        this.rootResolver = rootResolver;
    }

    private Entry entry(long catalogId) {
        CatalogMetaCache root = rootResolver.apply(catalogId);
        if (root == null) {
            throw new IllegalStateException("No external catalog for row count: " + catalogId);
        }
        return entries.compute(catalogId, (id, old) -> old != null && old.owner == root
                ? old : new Entry(root));
    }

    private void retire(Retention token) {
        if (token.entry.owner.isClosed() || retention.getIfPresent(token.key) == token) {
            return;
        }
        try {
            while (true) {
                RowCountValue current = token.entry.cache.getIfPresent(token.key);
                if (current == null || current.retention != token
                        || token.entry.cache.compareAndSet(token.key, current, null)) {
                    return;
                }
            }
        } catch (IllegalStateException failure) {
            // Catalog close may retire the physical entry between the owner check and the exact removal.
            if (!token.entry.owner.isClosed()) {
                throw failure;
            }
        }
    }

    private final class Entry {
        private final CatalogMetaCache owner;
        private final MetaCache<RowCountKey, RowCountValue> cache;

        private Entry(CatalogMetaCache owner) {
            this.owner = owner;
            MetaCacheDefinition.Builder<RowCountKey, RowCountValue> definition =
                    MetaCacheDefinition.<RowCountKey, RowCountValue>builder("row_count",
                            CacheSpec.of(true, Config.external_cache_expire_time_seconds_after_access,
                                    Config.max_external_table_row_count_cache_num),
                            key -> ScopePath.table(Long.toString(key.dbId), Long.toString(key.tableId))
                                    .in(ScopePath.Coordinates.ID))
                            .budgetPolicy(MetaCacheDefinition.BudgetPolicy.COUNT_ONLY)
                            .loader(key -> load(key, false))
                            .removalListener((key, value, reason) -> {
                                // Automatic refresh retains the same policy token across future replacement.
                                if (reason != MetaCacheRemovalReason.REPLACED) {
                                    retention.asMap().remove(key, value.retention);
                                }
                            });
            if (Config.external_cache_refresh_time_minutes > 0) {
                definition.refreshAfterWrite(Duration.ofMinutes(Config.external_cache_refresh_time_minutes), executor);
            }
            cache = owner.create(definition.build());
        }

        private RowCountValue load(RowCountKey key, boolean fillMetaCache) {
            RowCountValue current = cache.getIfPresent(key);
            Retention token = current == null ? new Retention(this, key) : current.retention;
            if (current != null) {
                // The framework runs refreshes on the background executor. Keep the previous successful
                // future visible until this load succeeds; submitting and waiting on the same pool can deadlock.
                // Like the original async-cache loader, automatic refresh does not fill metadata caches.
                Optional<Long> refreshed = loader.doLoad(key);
                return refreshed == null ? null
                        : new RowCountValue(token, CompletableFuture.completedFuture(refreshed));
            }
            CompletableFuture<Optional<Long>> future = fillMetaCache
                    ? CompletableFuture.supplyAsync(() -> loadRowCount(key, true), executor)
                    : loader.asyncLoad(key, executor);
            return new RowCountValue(token, future);
        }

        private void touch(RowCountKey key, RowCountValue value) {
            cache.runIfCurrent(key, value, () -> retention.put(key, value.retention));
        }
    }

    private static final class Retention {
        private final Entry entry;
        private final RowCountKey key;

        private Retention(Entry entry, RowCountKey key) {
            this.entry = entry;
            this.key = key;
        }
    }

    private static final class RowCountValue {
        private final Retention retention;
        private final CompletableFuture<Optional<Long>> future;

        private RowCountValue(Retention retention, CompletableFuture<Optional<Long>> future) {
            this.retention = retention;
            this.future = future;
        }
    }

    @Getter
    public static class RowCountKey {
        private final long catalogId;
        private final long dbId;
        private final long tableId;

        public RowCountKey(long catalogId, long dbId, long tableId) {
            this.catalogId = catalogId;
            this.dbId = dbId;
            this.tableId = tableId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof RowCountKey)) {
                return false;
            }
            return ((RowCountKey) obj).tableId == this.tableId;
        }

        @Override
        public int hashCode() {
            return (int) tableId;
        }
    }

    public static class RowCountCacheLoader extends BasicAsyncCacheLoader<RowCountKey, Optional<Long>> {
        @Override
        protected Optional<Long> doLoad(RowCountKey rowCountKey) {
            return loadRowCount(rowCountKey, false);
        }
    }

    static Optional<Long> loadRowCount(RowCountKey rowCountKey, boolean fillMetaCache) {
        try {
            ExternalTable table = (ExternalTable) StatisticsUtil.findTable(
                    rowCountKey.catalogId, rowCountKey.dbId, rowCountKey.tableId);
            return Optional.of(table.fetchRowCountWithMetaCache(fillMetaCache));
        } catch (Exception e) {
            String message = String.format("Failed to get table row count with catalogId %s, dbId %s, tableId %s. "
                            + "Reason %s",
                    rowCountKey.catalogId, rowCountKey.dbId, rowCountKey.tableId, e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.warn(message, e);
            } else {
                LOG.warn(message);
            }

            // Return Optional.empty() will cache this empty value in memory,
            // so we can't try to load the row count until the cache expire.
            // Throw an exception here will cause too much stack log in fe.out.
            // So we return null when exception happen.
            // Null may raise NPE in caller, but that is expected.
            // We catch that NPE and return a default value -1 without keep the value in cache,
            // so we can trigger the load function to fetch row count again next time in this exception case.
            return null;
        }
    }

    /**
     * Get cached row count for the given table. Return -1 if cached not loaded or table not exists.
     * Cached will be loaded async.
     * @param fillMetaCache whether loading the row count may fill external metadata caches
     * @return Cached row count or -1 if not exist
     */
    public long getCachedRowCount(long catalogId, long dbId, long tableId, boolean fillMetaCache) {
        RowCountKey key = new RowCountKey(catalogId, dbId, tableId);
        try {
            Entry entry = entry(catalogId);
            RowCountValue value = entry.cache.get(key, rowCountKey -> entry.load(rowCountKey, fillMetaCache));
            entry.touch(key, value);
            CompletableFuture<Optional<Long>> f = value.future;
            // Get row count synchronously by default.
            if (ConnectContext.get() == null
                    || ConnectContext.get().getSessionVariable().fetchHiveRowCountSync) {
                return result(entry, key, value);
            } else {
                if (f.isDone()) {
                    return result(entry, key, value);
                }
                LOG.info("Row count for table {}.{}.{} is still processing.", catalogId, dbId, tableId);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning row count", e);
        }
        return TableIf.UNKNOWN_ROW_COUNT;
    }

    /**
     * Get cached row count for the given table if present. Return -1 if cached not loaded.
     * This method will not trigger async loading if cache is missing.
     * @return Cached row count or -1 if not exist
     */
    public long getCachedRowCountIfPresent(long catalogId, long dbId, long tableId) {
        RowCountKey key = new RowCountKey(catalogId, dbId, tableId);
        try {
            Entry entry = entries.get(catalogId);
            if (entry == null || entry.owner.isClosed()) {
                return TableIf.UNKNOWN_ROW_COUNT;
            }
            RowCountValue value = entry.cache.getIfPresent(key);
            if (value == null) {
                return -1;
            } else if (value.future.isDone()) {
                entry.touch(key, value);
                return result(entry, key, value);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning row count if present", e);
        }
        return -1;
    }

    private long result(Entry entry, RowCountKey key, RowCountValue value) throws Exception {
        try {
            Optional<Long> loaded = value.future.get();
            return loaded == null ? TableIf.UNKNOWN_ROW_COUNT : loaded.orElse(TableIf.UNKNOWN_ROW_COUNT);
        } finally {
            if (value.future.isDone() && (value.future.isCompletedExceptionally()
                    || value.future.getNow(null) == null)) {
                entry.cache.compareAndSet(key, value, null);
            }
        }
    }

    // These compatibility entry points share the same resource generations as the unified change plan.
    void invalidateCatalog(long catalogId) {
        Entry entry = entries.get(catalogId);
        if (entry != null) {
            if (entry.owner.isClosed()) {
                entries.remove(catalogId, entry);
            } else {
                entry.owner.invalidateScope(ScopePath.catalog().in(ScopePath.Coordinates.ID));
            }
        }
    }

    void invalidateDb(long catalogId, long dbId) {
        Entry entry = entries.get(catalogId);
        if (entry != null && !entry.owner.isClosed()) {
            entry.owner.invalidateScope(ScopePath.database(Long.toString(dbId)).in(ScopePath.Coordinates.ID));
        }
    }

    void invalidateTable(long catalogId, long dbId, long tableId) {
        Entry entry = entries.get(catalogId);
        if (entry != null && !entry.owner.isClosed()) {
            entry.owner.invalidateScope(ScopePath.table(Long.toString(dbId), Long.toString(tableId))
                    .in(ScopePath.Coordinates.ID));
        }
    }
}
