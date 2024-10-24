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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TInvalidateFollowerStatsCacheRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUpdateFollowerPartitionStatsCacheRequest;
import org.apache.doris.thrift.TUpdateFollowerStatsCacheRequest;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

public class StatisticsCache {

    private static final Logger LOG = LogManager.getLogger(StatisticsCache.class);

    /**
     * Use a standalone thread pool to avoid interference between this and any other jdk function
     * that use the thread of ForkJoinPool#common in the system.
     */
    private final ThreadPoolExecutor threadPool
            = ThreadPoolManager.newDaemonFixedThreadPool(
            10, Integer.MAX_VALUE, "STATS_FETCH", true);

    private final ColumnStatisticsCacheLoader columnStatisticCacheLoader = new ColumnStatisticsCacheLoader();
    private final HistogramCacheLoader histogramCacheLoader = new HistogramCacheLoader();
    private final PartitionColumnStatisticCacheLoader partitionColumnStatisticCacheLoader
            = new PartitionColumnStatisticCacheLoader();

    private final AsyncLoadingCache<StatisticsCacheKey, Optional<ColumnStatistic>> columnStatisticsCache =
            Caffeine.newBuilder()
                    .maximumSize(Config.stats_cache_size)
                    .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
                    .executor(threadPool)
                    .buildAsync(columnStatisticCacheLoader);

    private final AsyncLoadingCache<StatisticsCacheKey, Optional<Histogram>> histogramCache =
            Caffeine.newBuilder()
                    .maximumSize(Config.stats_cache_size)
                    .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
                    .executor(threadPool)
                    .buildAsync(histogramCacheLoader);

    private final AsyncLoadingCache<PartitionColumnStatisticCacheKey, Optional<PartitionColumnStatistic>>
            partitionColumnStatisticCache =
            Caffeine.newBuilder()
                .maximumSize(Config.stats_cache_size)
                .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
                .executor(threadPool)
                .buildAsync(partitionColumnStatisticCacheLoader);

    public ColumnStatistic getColumnStatistics(long catalogId, long dbId, long tblId, long idxId, String colName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().internalSession) {
            return ColumnStatistic.UNKNOWN;
        }
        // Need to change base index id to -1 for OlapTable.
        try {
            idxId = changeBaseIndexId(catalogId, dbId, tblId, idxId);
        } catch (Exception e) {
            return ColumnStatistic.UNKNOWN;
        }
        StatisticsCacheKey k = new StatisticsCacheKey(catalogId, dbId, tblId, idxId, colName);
        try {
            CompletableFuture<Optional<ColumnStatistic>> f = columnStatisticsCache.get(k);
            if (f.isDone()) {
                return f.get().orElse(ColumnStatistic.UNKNOWN);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning ColumnStatistic", e);
        }
        return ColumnStatistic.UNKNOWN;
    }

    public PartitionColumnStatistic getPartitionColumnStatistics(long catalogId, long dbId, long tblId, long idxId,
                                                  String partName, String colName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().internalSession) {
            return PartitionColumnStatistic.UNKNOWN;
        }
        // Need to change base index id to -1 for OlapTable.
        try {
            idxId = changeBaseIndexId(catalogId, dbId, tblId, idxId);
        } catch (Exception e) {
            return PartitionColumnStatistic.UNKNOWN;
        }
        PartitionColumnStatisticCacheKey k = new PartitionColumnStatisticCacheKey(
                catalogId, dbId, tblId, idxId, partName, colName);
        try {
            CompletableFuture<Optional<PartitionColumnStatistic>> f = partitionColumnStatisticCache.get(k);
            if (f.isDone()) {
                return f.get().orElse(PartitionColumnStatistic.UNKNOWN);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning ColumnStatistic", e);
        }
        return PartitionColumnStatistic.UNKNOWN;
    }

    // Base index id should be set to -1 for OlapTable. Because statistics tables use -1 for base index.
    // TODO: Need to use the real index id in statistics table in later version.
    private long changeBaseIndexId(long catalogId, long dbId, long tblId, long idxId) {
        if (idxId != -1) {
            TableIf table = StatisticsUtil.findTable(catalogId, dbId, tblId);
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                if (idxId == olapTable.getBaseIndexId()) {
                    idxId = -1;
                }
            }
        }
        return idxId;
    }

    public Histogram getHistogram(long ctlId, long dbId, long tblId, String colName) {
        return getHistogram(ctlId, dbId, tblId, -1, colName).orElse(null);
    }

    private Optional<Histogram> getHistogram(long ctlId, long dbId, long tblId, long idxId, String colName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().internalSession) {
            return Optional.empty();
        }
        StatisticsCacheKey k = new StatisticsCacheKey(ctlId, dbId, tblId, idxId, colName);
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

    public void invalidateColumnStatsCache(long ctlId, long dbId, long tblId, long idxId, String colName) {
        columnStatisticsCache.synchronous().invalidate(new StatisticsCacheKey(ctlId, dbId, tblId, idxId, colName));
    }

    public void invalidatePartitionColumnStatsCache(long ctlId, long dbId, long tblId, long idxId,
                                                    String partName, String colName) {
        if (partName == null) {
            return;
        }
        partitionColumnStatisticCache.synchronous().invalidate(
            new PartitionColumnStatisticCacheKey(ctlId, dbId, tblId, idxId, partName, colName));
    }

    public void invalidateAllPartitionStatsCache() {
        partitionColumnStatisticCache.synchronous().invalidateAll();
    }

    public void updatePartitionColStatsCache(long ctlId, long dbId, long tblId, long idxId,
                                             String partName, String colName, PartitionColumnStatistic statistic) {
        partitionColumnStatisticCache.synchronous().put(
            new PartitionColumnStatisticCacheKey(ctlId, dbId, tblId, idxId, partName, colName), Optional.of(statistic));
    }

    public void updateColStatsCache(long ctlId, long dbId, long tblId, long idxId, String colName,
            ColumnStatistic statistic) {
        columnStatisticsCache.synchronous()
                .put(new StatisticsCacheKey(ctlId, dbId, tblId, idxId, colName), Optional.of(statistic));
    }

    public void refreshColStatsSync(long ctlId, long dbId, long tblId, long idxId, String colName) {
        columnStatisticsCache.synchronous().refresh(new StatisticsCacheKey(ctlId, dbId, tblId, idxId, colName));
    }

    public void refreshHistogramSync(long ctlId, long dbId, long tblId, long idxId, String colName) {
        histogramCache.synchronous().refresh(new StatisticsCacheKey(ctlId, dbId, tblId, idxId, colName));
    }

    public void preHeat() {
        if (!FeConstants.disablePreHeat) {
            threadPool.submit(this::doPreHeat);
        }
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
                StatsId statsId = new StatsId(r);
                final StatisticsCacheKey k =
                        new StatisticsCacheKey(statsId.catalogId, statsId.dbId, statsId.tblId, statsId.idxId,
                                statsId.colId);
                ColumnStatistic c = ColumnStatistic.fromResultRow(r);
                if (c.count > 0 && c.ndv == 0 && c.count != c.numNulls) {
                    c = ColumnStatistic.UNKNOWN;
                }
                putCache(k, c);
            } catch (Throwable t) {
                LOG.warn("Error when preheating stats cache. reason: [{}]. Row:[{}]", t.getMessage(), r);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(t);
                }
            }
        }
    }

    /**
     * Refresh stats cache, invalidate cache if the new data is unknown.
     */
    public void syncColStats(ColStatsData data) {
        StatsId statsId = data.statsId;
        final StatisticsCacheKey k = new StatisticsCacheKey(statsId.catalogId, statsId.dbId, statsId.tblId,
                statsId.idxId, statsId.colId);
        ColumnStatistic columnStatistic = data.toColumnStatistic();
        if (columnStatistic == ColumnStatistic.UNKNOWN) {
            invalidateColumnStatsCache(k.catalogId, k.dbId, k.tableId, k.idxId, k.colName);
        } else {
            putCache(k, columnStatistic);
        }
        TUpdateFollowerStatsCacheRequest updateFollowerStatsCacheRequest = new TUpdateFollowerStatsCacheRequest();
        updateFollowerStatsCacheRequest.key = GsonUtils.GSON.toJson(k);
        updateFollowerStatsCacheRequest.colStatsData = GsonUtils.GSON.toJson(data);
        // For compatible only, to be deprecated.
        updateFollowerStatsCacheRequest.statsRows = new ArrayList<>();
        SystemInfoService.HostInfo selfNode = Env.getCurrentEnv().getSelfNode();
        for (Frontend frontend : Env.getCurrentEnv().getFrontends(null)) {
            if (selfNode.getHost().equals(frontend.getHost())) {
                continue;
            }
            sendStats(frontend, updateFollowerStatsCacheRequest);
        }
    }

    @VisibleForTesting
    public void sendStats(Frontend frontend, TUpdateFollowerStatsCacheRequest updateFollowerStatsCacheRequest) {
        TNetworkAddress address = new TNetworkAddress(frontend.getHost(), frontend.getRpcPort());
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

    @VisibleForTesting
    public boolean invalidateStats(Frontend frontend, TInvalidateFollowerStatsCacheRequest request) {
        TNetworkAddress address = new TNetworkAddress(frontend.getHost(), frontend.getRpcPort());
        FrontendService.Client client = null;
        try {
            client = ClientPool.frontendPool.borrowObject(address);
            client.invalidateStatsCache(request);
        } catch (Throwable t) {
            LOG.warn("Failed to sync invalidate to follower: {}", address, t);
            return false;
        } finally {
            if (client != null) {
                ClientPool.frontendPool.returnObject(address, client);
            }
        }
        return true;
    }

    public void putCache(StatisticsCacheKey k, ColumnStatistic c) {
        CompletableFuture<Optional<ColumnStatistic>> f = new CompletableFuture<Optional<ColumnStatistic>>();
        f.obtrudeValue(Optional.of(c));
        columnStatisticsCache.put(k, f);
    }

    @VisibleForTesting
    public boolean updatePartitionStats(Frontend frontend, TUpdateFollowerPartitionStatsCacheRequest request) {
        TNetworkAddress address = new TNetworkAddress(frontend.getHost(), frontend.getRpcPort());
        FrontendService.Client client = null;
        try {
            client = ClientPool.frontendPool.borrowObject(address);
            client.updatePartitionStatsCache(request);
        } catch (Throwable t) {
            LOG.warn("Failed to update partition stats cache of follower: {}", address, t);
            return false;
        } finally {
            if (client != null) {
                ClientPool.frontendPool.returnObject(address, client);
            }
        }
        return true;
    }
}
