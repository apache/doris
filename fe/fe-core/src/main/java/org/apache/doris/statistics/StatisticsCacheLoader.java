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

import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 * Use to load stats cache.
 */
public class StatisticsCacheLoader implements AsyncCacheLoader<StatisticsCacheKey, Statistic> {

    private static final Logger LOG = LogManager.getLogger(StatisticsCacheLoader.class);

    private static final String QUERY_COLUMN_STATISTICS = "SELECT * FROM " + FeConstants.INTERNAL_DB_NAME
            + "." + StatisticConstants.STATISTIC_TBL_NAME + " WHERE "
            + "id = CONCAT('${tblId}', '-', ${idxId}, '-', '${colId}')";

    private static final String QUERY_HISTOGRAM_STATISTICS = "SELECT * FROM " + FeConstants.INTERNAL_DB_NAME
            + "." + StatisticConstants.HISTOGRAM_TBL_NAME + " WHERE "
            + "id = CONCAT('${tblId}', '-', ${idxId}, '-', '${colId}')";

    private final ConcurrentMap<StatisticsCacheKey, CompletableFuture<Statistic>>
            inProgressing = new ConcurrentHashMap<>();

    // TODO: Maybe we should trigger a analyze job when the required ColumnStatistic doesn't exists.
    @Override
    public @NonNull CompletableFuture<Statistic> asyncLoad(@NonNull StatisticsCacheKey key,
            @NonNull Executor executor) {
        CompletableFuture<Statistic> future = inProgressing.get(key);
        if (future != null) {
            return future;
        }
        future = CompletableFuture.supplyAsync(() -> {
            Statistic statistic = new Statistic();
            long startTime = 0;
            try {
                LOG.info("Query BE for column stats:{}-{} start time:{}", key.tableId, key.colName,
                        startTime);
                Map<String, String> params = new HashMap<>();
                params.put("tblId", String.valueOf(key.tableId));
                params.put("idxId", String.valueOf(key.idxId));
                params.put("colId", String.valueOf(key.colName));

                List<ColumnStatistic> columnStatistics;
                List<ResultRow> columnResult =
                        StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                                .replace(QUERY_COLUMN_STATISTICS));
                try {
                    columnStatistics = StatisticsUtil.deserializeToColumnStatistics(columnResult);
                } catch (Exception e) {
                    LOG.warn("Failed to deserialize column statistics", e);
                    throw new CompletionException(e);
                }
                if (CollectionUtils.isEmpty(columnStatistics)) {
                    statistic.setColumnStatistic(ColumnStatistic.DEFAULT);
                } else {
                    statistic.setColumnStatistic(columnStatistics.get(0));
                }

                List<Histogram> histogramStatistics;
                List<ResultRow> histogramResult =
                        StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                                .replace(QUERY_HISTOGRAM_STATISTICS));
                try {
                    histogramStatistics = StatisticsUtil.deserializeToHistogramStatistics(histogramResult);
                } catch (Exception e) {
                    LOG.warn("Failed to deserialize histogram statistics", e);
                    throw new CompletionException(e);
                }
                if (CollectionUtils.isEmpty(histogramStatistics)) {
                    statistic.setHistogram(Histogram.DEFAULT);
                } else {
                    statistic.setHistogram(histogramStatistics.get(0));
                }
            } finally {
                long endTime = System.currentTimeMillis();
                LOG.info("Query BE for column stats:{}-{} end time:{} cost time:{}", key.tableId, key.colName,
                        endTime, endTime - startTime);
                inProgressing.remove(key);
            }
            return statistic;
        });
        inProgressing.put(key, future);
        return future;
    }
}
