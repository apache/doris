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

import org.apache.doris.qe.ConnectContext;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class StatisticsCache {

    private static final Logger LOG = LogManager.getLogger(StatisticsCache.class);

    private final AsyncLoadingCache<StatisticsCacheKey, ColumnStatistic> cache = Caffeine.newBuilder()
            .maximumSize(StatisticConstants.STATISTICS_RECORDS_CACHE_SIZE)
            .expireAfterAccess(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_VALID_DURATION_IN_HOURS))
            .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
            .buildAsync(new StatisticsCacheLoader());

    public ColumnStatistic getColumnStatistics(long tblId, String colName) {
        if (ConnectContext.get().getSessionVariable().internalSession) {
            return ColumnStatistic.DEFAULT;
        }
        StatisticsCacheKey k = new StatisticsCacheKey(tblId, colName);
        try {
            CompletableFuture<ColumnStatistic> f = cache.get(k);
            if (f.isDone()) {
                return f.get();
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning ColumnStatistic", e);
            return ColumnStatistic.DEFAULT;
        }
        return ColumnStatistic.DEFAULT;
    }

    // TODO: finish this method.
    public void eraseExpiredCache(long tblId, String colName) {
        cache.synchronous().invalidate(new StatisticsCacheKey(tblId, colName));
    }

    public void updateCache(long tblId, String colName, ColumnStatistic statistic) {

        cache.synchronous().put(new StatisticsCacheKey(tblId, colName), statistic);
    }

    public void refreshSync(long tblId, String colName) {
        cache.synchronous().refresh(new StatisticsCacheKey(tblId, colName));
    }
}
