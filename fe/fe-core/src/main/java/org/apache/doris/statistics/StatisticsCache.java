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

import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

    private final ColumnStatisticsCacheLoader columnStatisticsCacheLoader = new ColumnStatisticsCacheLoader();
    private final HistogramCacheLoader histogramCacheLoader = new HistogramCacheLoader();

    private final AsyncLoadingCache<StatisticsCacheKey, Optional<ColumnStatistic>> columnStatisticsCache =
            Caffeine.newBuilder()
                    .maximumSize(StatisticConstants.STATISTICS_RECORDS_CACHE_SIZE)
                    .expireAfterAccess(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_VALID_DURATION_IN_HOURS))
                    .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
                    .executor(threadPool)
                    .buildAsync(columnStatisticsCacheLoader);

    private final AsyncLoadingCache<StatisticsCacheKey, Optional<Histogram>> histogramCache =
            Caffeine.newBuilder()
                    .maximumSize(StatisticConstants.STATISTICS_RECORDS_CACHE_SIZE)
                    .expireAfterAccess(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_VALID_DURATION_IN_HOURS))
                    .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
                    .executor(threadPool)
                    .buildAsync(histogramCacheLoader);

    {
        threadPool.submit(() -> {
            while (true) {
                try {
                    columnStatisticsCacheLoader.removeExpiredInProgressing();
                    histogramCacheLoader.removeExpiredInProgressing();
                } catch (Throwable t) {
                    // IGNORE
                }
                Thread.sleep(TimeUnit.MINUTES.toMillis(15));
            }

        });
    }

    public ColumnStatistic getColumnStatistics(long tblId, String colName) {
        return getColumnStatistics(tblId, -1, colName).orElse(ColumnStatistic.UNKNOWN);
    }

    public Optional<ColumnStatistic> getColumnStatistics(long tblId, long idxId, String colName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().internalSession) {
            return Optional.empty();
        }
        StatisticsCacheKey k = new StatisticsCacheKey(tblId, idxId, colName);
        try {
            CompletableFuture<Optional<ColumnStatistic>> f = columnStatisticsCache.get(k);
            if (f.isDone()) {
                return f.get();
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning ColumnStatistic", e);
        }
        return Optional.empty();
    }

    public Histogram getHistogram(long tblId, String colName) {
        return getHistogram(tblId, -1, colName).orElse(null);
    }

    public Optional<Histogram> getHistogram(long tblId, long idxId, String colName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().internalSession) {
            return Optional.empty();
        }
        StatisticsCacheKey k = new StatisticsCacheKey(tblId, idxId, colName);
        try {
            CompletableFuture<Optional<Histogram>> f = histogramCache.get(k);
            if (f.isDone()) {
                return f.get();
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning Histogram", e);
        }
        return Optional.empty();
    }

    public void invidate(long tblId, long idxId, String colName) {
        columnStatisticsCache.synchronous().invalidate(new StatisticsCacheKey(tblId, idxId, colName));
    }

    public void updateColStatsCache(long tblId, long idxId, String colName, ColumnStatistic statistic) {
        columnStatisticsCache.synchronous().put(new StatisticsCacheKey(tblId, idxId, colName), Optional.of(statistic));
    }

    public void refreshColStatsSync(long tblId, long idxId, String colName) {
        columnStatisticsCache.synchronous().refresh(new StatisticsCacheKey(tblId, idxId, colName));
    }

    public void refreshHistogramSync(long tblId, long idxId, String colName) {
        histogramCache.synchronous().refresh(new StatisticsCacheKey(tblId, idxId, colName));
    }

    public void preHeat() {
        threadPool.submit(this::doPreHeat);
    }

    private void doPreHeat() {
        List<ResultRow> recentStatsUpdatedCols = null;
        long retryTimes = 0;
        while (!StatisticsUtil.statsTblAvailable()) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                // IGNORE
            }
        }

        while (retryTimes < StatisticConstants.PRELOAD_RETRY_TIMES) {
            try {
                recentStatsUpdatedCols = StatisticsRepository.fetchRecentStatsUpdatedCol();
                break;
            } catch (Throwable t) {
                // IGNORE
            }
            retryTimes++;
            try {
                Thread.sleep(StatisticConstants.PRELOAD_RETRY_INTERVAL_IN_SECONDS);
            } catch (Throwable t) {
                // IGNORE
            }
        }

        if (CollectionUtils.isEmpty(recentStatsUpdatedCols)) {
            return;
        }
        for (ResultRow r : recentStatsUpdatedCols) {
            try {
                String tblId = r.getColumnValue("tbl_id");
                String idxId = r.getColumnValue("idx_id");
                String colId = r.getColumnValue("col_id");
                final StatisticsCacheKey k =
                        new StatisticsCacheKey(Long.parseLong(tblId), Long.parseLong(idxId), colId);
                final ColumnStatistic c = ColumnStatistic.fromResultRow(r);
                CompletableFuture<Optional<ColumnStatistic>> f = new CompletableFuture<Optional<ColumnStatistic>>() {

                    @Override
                    public Optional<ColumnStatistic> get() throws InterruptedException, ExecutionException {
                        return Optional.of(c);
                    }

                    @Override
                    public boolean isDone() {
                        return true;
                    }

                    @Override
                    public boolean complete(Optional<ColumnStatistic> value) {
                        return true;
                    }

                    @Override
                    public Optional<ColumnStatistic> join() {
                        return Optional.of(c);
                    }
                };
                if (c == ColumnStatistic.UNKNOWN) {
                    continue;
                }
                columnStatisticsCache.put(k, f);
            } catch (Throwable t) {
                LOG.warn("Error when preheating stats cache", t);
            }
        }
    }

}
