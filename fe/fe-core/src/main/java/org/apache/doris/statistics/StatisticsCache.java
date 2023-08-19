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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUpdateFollowerStatsCacheRequest;

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
    private final TableStatisticsCacheLoader tableStatisticsCacheLoader = new TableStatisticsCacheLoader();

    private final AsyncLoadingCache<StatisticsCacheKey, Optional<ColumnStatistic>> columnStatisticsCache =
            Caffeine.newBuilder()
                    .maximumSize(Config.stats_cache_size)
                    .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
                    .executor(threadPool)
                    .buildAsync(columnStatisticsCacheLoader);

    private final AsyncLoadingCache<StatisticsCacheKey, Optional<Histogram>> histogramCache =
            Caffeine.newBuilder()
                    .maximumSize(Config.stats_cache_size)
                    .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
                    .executor(threadPool)
                    .buildAsync(histogramCacheLoader);

    private final AsyncLoadingCache<StatisticsCacheKey, Optional<TableStatistic>> tableStatisticsCache =
            Caffeine.newBuilder()
                    .maximumSize(Config.stats_cache_size)
                    .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
                    .executor(threadPool)
                    .buildAsync(tableStatisticsCacheLoader);

    {
        threadPool.submit(() -> {
            while (true) {
                try {
                    columnStatisticsCacheLoader.removeExpiredInProgressing();
                    histogramCacheLoader.removeExpiredInProgressing();
                    tableStatisticsCacheLoader.removeExpiredInProgressing();
                } catch (Throwable t) {
                    // IGNORE
                }
                Thread.sleep(TimeUnit.MINUTES.toMillis(15));
            }

        });
    }

    public ColumnStatistic getColumnStatistics(long catalogId, long dbId, long tblId, String colName) {
        return getColumnStatistics(catalogId, dbId, tblId, -1, colName).orElse(ColumnStatistic.UNKNOWN);
    }

    public Optional<ColumnStatistic> getColumnStatistics(long catalogId, long dbId,
            long tblId, long idxId, String colName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().internalSession) {
            return Optional.empty();
        }
        StatisticsCacheKey k = new StatisticsCacheKey(catalogId, dbId, tblId, idxId, colName);
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

    public Optional<TableStatistic> getTableStatistics(long catalogId, long dbId, long tableId) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().internalSession) {
            return Optional.empty();
        }
        StatisticsCacheKey k = new StatisticsCacheKey(catalogId, dbId, tableId);
        try {
            CompletableFuture<Optional<TableStatistic>> f = tableStatisticsCache.get(k);
            if (f.isDone()) {
                return f.get();
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning Histogram", e);
        }
        return Optional.empty();
    }

    public void invalidate(long tblId, long idxId, String colName) {
        columnStatisticsCache.synchronous().invalidate(new StatisticsCacheKey(tblId, idxId, colName));
    }

    public void updateColStatsCache(long tblId, long idxId, String colName, ColumnStatistic statistic) {
        columnStatisticsCache.synchronous().put(new StatisticsCacheKey(tblId, idxId, colName), Optional.of(statistic));
    }

    public void refreshColStatsSync(long tblId, long idxId, String colName) {
        columnStatisticsCache.synchronous().refresh(new StatisticsCacheKey(-1, -1, tblId, idxId, colName));
    }

    public void refreshColStatsSync(long catalogId, long dbId, long tblId, long idxId, String colName) {
        columnStatisticsCache.synchronous().refresh(new StatisticsCacheKey(catalogId, dbId, tblId, idxId, colName));
    }

    public void invalidateTableStats(long catalogId, long dbId, long tblId) {
        tableStatisticsCache.synchronous().invalidate(new StatisticsCacheKey(catalogId, dbId, tblId));
    }

    public void refreshTableStatsSync(long catalogId, long dbId, long tblId) {
        tableStatisticsCache.synchronous().refresh(new StatisticsCacheKey(catalogId, dbId, tblId));
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
                long tblId = Long.parseLong(r.getColumnValue("tbl_id"));
                long idxId = Long.parseLong(r.getColumnValue("idx_id"));
                String colId = r.getColumnValue("col_id");
                final StatisticsCacheKey k =
                        new StatisticsCacheKey(tblId, idxId, colId);
                final ColumnStatistic c = ColumnStatistic.fromResultRow(r);
                c.loadPartitionStats(tblId, idxId, colId);
                putCache(k, c);
            } catch (Throwable t) {
                LOG.warn("Error when preheating stats cache", t);
            }
        }
    }

    public void syncLoadColStats(long tableId, long idxId, String colName) {
        List<ResultRow> columnResults = StatisticsRepository.loadColStats(tableId, idxId, colName);
        final StatisticsCacheKey k =
                new StatisticsCacheKey(tableId, idxId, colName);
        final ColumnStatistic c = ColumnStatistic.fromResultRow(columnResults);
        if (c == ColumnStatistic.UNKNOWN) {
            return;
        }
        putCache(k, c);
        TUpdateFollowerStatsCacheRequest updateFollowerStatsCacheRequest = new TUpdateFollowerStatsCacheRequest();
        updateFollowerStatsCacheRequest.key = GsonUtils.GSON.toJson(k);
        updateFollowerStatsCacheRequest.colStats = GsonUtils.GSON.toJson(c);
        for (Frontend frontend : Env.getCurrentEnv().getFrontends(FrontendNodeType.FOLLOWER)) {
            if (frontend.getHost().equals(Env.getCurrentEnv().getSelfNode().getHost())) {
                // Doesn't need to send request to current node.
                continue;
            }
            TNetworkAddress address = new TNetworkAddress(frontend.getHost(),
                    frontend.getRpcPort());
            FrontendService.Client client = null;
            try {
                client = ClientPool.frontendPool.borrowObject(address);
                client.updateStatsCache(updateFollowerStatsCacheRequest);
            } catch (Throwable t) {
                LOG.warn("Failed to sync stats to follower: {}", address, t);
            } finally {
                if (client != null) {
                    ClientPool.frontendPool.returnObject(address, client);
                }
            }
        }

    }

    public void putCache(StatisticsCacheKey k, ColumnStatistic c) {
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
        if (c.isUnKnown) {
            return;
        }
        columnStatisticsCache.put(k, f);
    }

}
