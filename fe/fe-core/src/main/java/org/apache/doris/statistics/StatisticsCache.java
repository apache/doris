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

import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

public class StatisticsCache {

    private static final Logger LOG = LogManager.getLogger(StatisticsCache.class);

    private final AsyncLoadingCache<StatisticsCacheKey, ColumnStatistic> cache = Caffeine.newBuilder()
            .maximumSize(Config.statistics_cache_max_size)
            .expireAfterAccess(Duration.ofHours(Config.statistics_cache_valid_duration_in_hours))
            .refreshAfterWrite(Duration.ofHours(Config.statistics_cache_refresh_interval))
            .buildAsync(new StatisticsCacheLoader());

    public ColumnStatistic getColumnStatistics(long tblId, String colName) {
        if (ConnectContext.get().getSessionVariable().internalSession) {
            return ColumnStatistic.UNKNOWN;
        }
        StatisticsCacheKey k = new StatisticsCacheKey(tblId, colName);
        CompletableFuture<ColumnStatistic> f = cache.get(k);
        if (f.isDone()) {
            try {
                return f.get();
            } catch (Exception e) {
                LOG.warn("Unexpected exception while returning ColumnStatistic", e);
                return ColumnStatistic.UNKNOWN;
            }
        }
        return ColumnStatistic.UNKNOWN;
    }

    // TODO: finish this method.
    public void eraseExpiredCache(long tblId, String colName) {
        cache.synchronous().invalidate(new StatisticsCacheKey(tblId, colName));
    }

    private static class StatisticsCacheLoader implements AsyncCacheLoader<StatisticsCacheKey, ColumnStatistic> {

        private static final String QUERY_COLUMN_STATISTICS = "SELECT * FROM " + StatisticConstants.STATISTIC_DB_NAME
                + "." + StatisticConstants.STATISTIC_TBL_NAME + " WHERE "
                + "id = CONCAT('${tblId}', '-', '${colId}')";

        // TODO: Maybe we should trigger a analyze job when the required ColumnStatistic doesn't exists.
        @Override
        public @NonNull CompletableFuture<ColumnStatistic> asyncLoad(@NonNull StatisticsCacheKey key,
                @NonNull Executor executor) {
            return CompletableFuture.supplyAsync(() -> {
                Map<String, String> params = new HashMap<>();
                params.put("tblId", String.valueOf(key.tableId));
                params.put("colId", String.valueOf(key.colName));
                List<ResultRow> resultBatches =
                        StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                                .replace(QUERY_COLUMN_STATISTICS));
                List<ColumnStatistic> columnStatistics = null;
                try {
                    columnStatistics = StatisticsUtil.deserializeToColumnStatistics(resultBatches);
                } catch (Exception e) {
                    LOG.warn("Failed to deserialize column statistics", e);
                    throw new CompletionException(e);
                }
                if (CollectionUtils.isEmpty(columnStatistics)) {
                    return ColumnStatistic.DEFAULT;
                }
                return columnStatistics.get(0);
            });
        }
    }
}
