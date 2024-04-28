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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.statistics.BasicAsyncCacheLoader;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class ExternalRowCountCache {

    private static final Logger LOG = LogManager.getLogger(ExternalRowCountCache.class);
    private final AsyncLoadingCache<RowCountKey, Optional<Long>> rowCountCache;

    public ExternalRowCountCache(ExecutorService executor) {
        // 1. set expireAfterWrite to 1 day, avoid too many entries
        // 2. set refreshAfterWrite to 10min(default), so that the cache will be refreshed after 10min
        CacheFactory rowCountCacheFactory = new CacheFactory(
                OptionalLong.of(86400L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_external_table_row_count_cache_num,
                false,
                null);
        rowCountCache = rowCountCacheFactory.buildAsyncCache(new RowCountCacheLoader(), executor);
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
            try {
                TableIf table = StatisticsUtil.findTable(rowCountKey.catalogId, rowCountKey.dbId, rowCountKey.tableId);
                return Optional.of(table.fetchRowCount());
            } catch (Exception e) {
                LOG.warn("Failed to get table with catalogId {}, dbId {}, tableId {}", rowCountKey.catalogId,
                        rowCountKey.dbId, rowCountKey.tableId);
                return Optional.empty();
            }
        }
    }

    /**
     * Get cached row count for the given table. Return 0 if cached not loaded or table not exists.
     * Cached will be loaded async.
     * @param catalogId
     * @param dbId
     * @param tableId
     * @return Cached row count or 0 if not exist
     */
    public long getCachedRowCount(long catalogId, long dbId, long tableId) {
        RowCountKey key = new RowCountKey(catalogId, dbId, tableId);
        try {
            CompletableFuture<Optional<Long>> f = rowCountCache.get(key);
            if (f.isDone()) {
                return f.get().orElse(0L);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning row count", e);
        }
        return 0;
    }

    /**
     * Get cached row count for the given table if present. Return 0 if cached not loaded.
     * This method will not trigger async loading if cache is missing.
     *
     * @param catalogId
     * @param dbId
     * @param tableId
     * @return
     */
    public long getCachedRowCountIfPresent(long catalogId, long dbId, long tableId) {
        RowCountKey key = new RowCountKey(catalogId, dbId, tableId);
        try {
            CompletableFuture<Optional<Long>> f = rowCountCache.getIfPresent(key);
            if (f == null) {
                return 0;
            } else if (f.isDone()) {
                return f.get().orElse(0L);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning row count if present", e);
        }
        return 0;
    }

}
