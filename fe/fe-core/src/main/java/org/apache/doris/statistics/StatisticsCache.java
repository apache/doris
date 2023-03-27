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

//import org.apache.doris.common.ThreadPoolManager;

import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.qe.ConnectContext;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class StatisticsCache {

    private static final Logger LOG = LogManager.getLogger(StatisticsCache.class);

    /**
     * Use a standalone thread pool to avoid interference between this and any other jdk function
     * that use the thread of ForkJoinPool#common in the system.
     */
    private final ThreadPoolExecutor threadPool
            = ThreadPoolManager.newDaemonFixedThreadPool(
            10, Integer.MAX_VALUE, "STATS_FETCH", true);

    private final StatisticsCacheLoader cacheLoader = new StatisticsCacheLoader();

    private final AsyncLoadingCache<StatisticsCacheKey, ColumnLevelStatisticCache> cache = Caffeine.newBuilder()
            .maximumSize(StatisticConstants.STATISTICS_RECORDS_CACHE_SIZE)
            .expireAfterAccess(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_VALID_DURATION_IN_HOURS))
            .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
            .executor(threadPool)
            .buildAsync(cacheLoader);

    {
        threadPool.submit(() -> {
            while (true) {
                try {
                    cacheLoader.removeExpiredInProgressing();
                    Thread.sleep(TimeUnit.MINUTES.toMillis(15));
                } catch (Throwable t) {
                    // IGNORE
                }
            }

        });
    }

    public ColumnStatistic getColumnStatistics(long tblId, String colName) {
        ColumnLevelStatisticCache columnLevelStatisticCache = getColumnStatistics(tblId, -1, colName);
        if (columnLevelStatisticCache == null) {
            return ColumnStatistic.UNKNOWN;
        }
        return columnLevelStatisticCache.columnStatistic;
    }

    public ColumnLevelStatisticCache getColumnStatistics(long tblId, long idxId, String colName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().internalSession) {
            return null;
        }
        StatisticsCacheKey k = new StatisticsCacheKey(tblId, idxId, colName);
        try {
            CompletableFuture<ColumnLevelStatisticCache> f = cache.get(k);
            if (f.isDone() && f.get() != null) {
                return f.get();
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning ColumnStatistic", e);
        }
        return null;
    }

    public Histogram getHistogram(long tblId, String colName) {
        return getHistogram(tblId, -1, colName);
    }

    public Histogram getHistogram(long tblId, long idxId, String colName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().internalSession) {
            return null;
        }
        StatisticsCacheKey k = new StatisticsCacheKey(tblId, idxId, colName);
        try {
            CompletableFuture<ColumnLevelStatisticCache> f = cache.get(k);
            if (f.isDone() && f.get() != null) {
                return f.get().getHistogram();
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning Histogram", e);
        }
        return null;
    }

    // TODO: finish this method.
    public void eraseExpiredCache(long tblId, long idxId, String colName) {
        cache.synchronous().invalidate(new StatisticsCacheKey(tblId, idxId, colName));
    }

    public void updateCache(long tblId, long idxId, String colName, ColumnLevelStatisticCache statistic) {
        cache.synchronous().put(new StatisticsCacheKey(tblId, idxId, colName), statistic);
    }

    public void refreshSync(long tblId, long idxId, String colName) {
        cache.synchronous().refresh(new StatisticsCacheKey(tblId, idxId, colName));
    }
}
