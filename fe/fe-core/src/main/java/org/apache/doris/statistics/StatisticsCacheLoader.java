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
import java.util.concurrent.Executor;

public class StatisticsCacheLoader implements AsyncCacheLoader<StatisticsCacheKey, ColumnStatistic> {

    private static final Logger LOG = LogManager.getLogger(StatisticsCacheLoader.class);

    private static final String QUERY_COLUMN_STATISTICS = "SELECT * FROM " + FeConstants.INTERNAL_DB_NAME
            + "." + StatisticConstants.STATISTIC_TBL_NAME + " WHERE "
            + "id = CONCAT('${tblId}', '-', '${colId}')";

    private static int CUR_RUNNING_LOAD = 0;

    private static final Object LOCK = new Object();

    // TODO: Maybe we should trigger a analyze job when the required ColumnStatistic doesn't exists.
    @Override
    public @NonNull CompletableFuture<ColumnStatistic> asyncLoad(@NonNull StatisticsCacheKey key,
            @NonNull Executor executor) {
        synchronized (LOCK) {
            if (CUR_RUNNING_LOAD > StatisticConstants.LOAD_TASK_LIMITS) {
                try {
                    LOCK.wait();
                } catch (InterruptedException e) {
                    LOG.warn("Ignore interruption", e);
                }
            }
            CUR_RUNNING_LOAD++;
            return CompletableFuture.supplyAsync(() -> {
                try {
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
                } finally {
                    synchronized (LOCK) {
                        CUR_RUNNING_LOAD--;
                        LOCK.notify();
                    }
                }
            });
        }
    }
}
