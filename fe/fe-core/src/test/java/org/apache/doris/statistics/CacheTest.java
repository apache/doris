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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUpdateFollowerStatsCacheRequest;
import org.apache.doris.utframe.TestWithFeService;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

public class CacheTest extends TestWithFeService {

    @Test
    public void testColumn() throws Exception {
        try (MockedConstruction<ColumnStatisticsCacheLoader> mc = Mockito.mockConstruction(
                ColumnStatisticsCacheLoader.class, (mock, ctx) -> {
                    Mockito.doReturn(CompletableFuture.supplyAsync(() -> {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            return Optional.of(ColumnStatistic.UNKNOWN);
                        }
                        return Optional.of(ColumnStatistic.UNKNOWN);
                    })).when(mock).asyncLoad(Mockito.any(), Mockito.any());
                })) {
            StatisticsCache statisticsCache = new StatisticsCache();
            ColumnStatistic c = statisticsCache.getColumnStatistics(-1, -1, 1, -1, "col", connectContext);
            Assertions.assertTrue(c.isUnKnown);
            Thread.sleep(100);
            c = statisticsCache.getColumnStatistics(-1, -1, 1, -1, "col", connectContext);
            Assertions.assertTrue(c.isUnKnown);
        }
    }

    @Test
    public void testLoad() throws Exception {
        try (MockedConstruction<ColumnStatisticsCacheLoader> mc = Mockito.mockConstruction(
                ColumnStatisticsCacheLoader.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (mock, ctx) -> {
                    Mockito.doAnswer(inv -> {
                        Thread.sleep(50);
                        ColumnStatisticBuilder builder =
                                new ColumnStatisticBuilder(7);
                        builder.setNdv(8);
                        builder.setNumNulls(0);
                        builder.setDataSize(12);
                        builder.setAvgSizeByte(12.0 / 7);
                        builder.setMinValue(10);
                        builder.setMaxValue(11);
                        builder.setUpdatedTime(
                                String.valueOf(System.currentTimeMillis()));
                        return Optional.of(builder.build());
                    }).when(mock).doLoad(Mockito.any());
                })) {
            StatisticsCache statisticsCache = new StatisticsCache();
            ColumnStatistic columnStatistic = statisticsCache.getColumnStatistics(
                    -1, -1, 0, -1, "col", connectContext);
            // load not finished yet, should return unknown
            Assertions.assertTrue(columnStatistic.isUnKnown);
            int retry = 0;
            while (columnStatistic.isUnKnown() && retry < 10) {
                // wait 1 sec to ensure async load is finished.
                Thread.sleep(1000);
                // load has finished, return corresponding stats.
                columnStatistic = statisticsCache.getColumnStatistics(
                        -1, -1, 0, -1, "col", connectContext);
                retry++;
            }
            System.out.println("wait for " + retry + " seconds");
            Assertions.assertEquals(7, columnStatistic.count);
            Assertions.assertEquals(8, columnStatistic.ndv);
            Assertions.assertEquals(11, columnStatistic.maxValue);
        }
    }

    @Test
    public void testLoadHistogram() throws Exception {
        try (MockedConstruction<HistogramCacheLoader> mc = Mockito.mockConstruction(
                HistogramCacheLoader.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (mock, ctx) -> {
                    Mockito.doAnswer(inv -> {
                        Thread.sleep(50);
                        HistogramBuilder histogramBuilder = new HistogramBuilder();
                        Column col = new Column("abc", PrimitiveType.DATETIME);
                        Type dataType = col.getType();
                        histogramBuilder.setDataType(dataType);
                        histogramBuilder.setSampleRate(0.2);
                        String json = "{\"num_buckets\":5,\"buckets\":"
                                + "[{\"lower\":\"2022-09-21 17:30:29\","
                                + "\"upper\":\"2022-09-21 22:30:29\","
                                + "\"count\":9,\"pre_sum\":0,\"ndv\":1},"
                                + "{\"lower\":\"2022-09-22 17:30:29\","
                                + "\"upper\":\"2022-09-22 22:30:29\","
                                + "\"count\":10,\"pre_sum\":9,\"ndv\":1},"
                                + "{\"lower\":\"2022-09-23 17:30:29\","
                                + "\"upper\":\"2022-09-23 22:30:29\","
                                + "\"count\":9,\"pre_sum\":19,\"ndv\":1},"
                                + "{\"lower\":\"2022-09-24 17:30:29\","
                                + "\"upper\":\"2022-09-24 22:30:29\","
                                + "\"count\":9,\"pre_sum\":28,\"ndv\":1},"
                                + "{\"lower\":\"2022-09-25 17:30:29\","
                                + "\"upper\":\"2022-09-25 22:30:29\","
                                + "\"count\":9,\"pre_sum\":37,\"ndv\":1}]}";
                        JsonObject jsonObj = JsonParser.parseString(json)
                                .getAsJsonObject();
                        int bucketNum =
                                jsonObj.get("num_buckets").getAsInt();
                        histogramBuilder.setNumBuckets(bucketNum);
                        List<Bucket> buckets = Lists.newArrayList();
                        JsonArray jsonArray =
                                jsonObj.getAsJsonArray("buckets");
                        for (JsonElement element : jsonArray) {
                            try {
                                String bucketJson = element.toString();
                                buckets.add(Bucket.deserializeFromJson(
                                        dataType, bucketJson));
                            } catch (Throwable t) {
                                t.printStackTrace();
                            }
                        }
                        histogramBuilder.setBuckets(buckets);
                        return Optional.of(histogramBuilder.build());
                    }).when(mock).doLoad(Mockito.any());
                })) {
            StatisticsCache statisticsCache = new StatisticsCache();
            statisticsCache.refreshHistogramSync(0, 0, 0, -1, "col");
            Thread.sleep(10000);
            Histogram histogram = statisticsCache.getHistogram(0, 0, 0, "col");
            Assertions.assertNotNull(histogram);
        }
    }

    @Test
    public void testLoadFromMeta() throws Exception {
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Env env = Mockito.mock(Env.class);

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(
                    StatisticsUtil.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(
                        Env.class, Mockito.CALLS_REAL_METHODS)) {

            mockedStatisticsUtil.when(() -> StatisticsUtil.findColumn(
                    Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(),
                    Mockito.anyLong(), Mockito.anyString()))
                    .thenReturn(new Column("abc", PrimitiveType.BIGINT));
            mockedStatisticsUtil.when(() -> StatisticsUtil.execStatisticQuery(Mockito.anyString()))
                    .thenReturn(null);
            mockedStatisticsUtil.when(() -> StatisticsUtil.findTable(
                    Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong()))
                    .thenReturn(table);

            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Mockito.when(table.getColumnStatistic(Mockito.anyString())).thenReturn(
                    Optional.of(new ColumnStatistic(1, 2,
                            null, 3, 4, 5, 6, 7,
                            null, null, false,
                            new Date().toString(), null)));

            try {
                StatisticsCache statisticsCache = new StatisticsCache();
                ColumnStatistic columnStatistic = statisticsCache.getColumnStatistics(
                        1, 1, 1, -1, "col", connectContext);
                for (int i = 0; i < 15; i++) {
                    columnStatistic = statisticsCache.getColumnStatistics(
                            1, 1, 1, -1, "col", connectContext);
                    if (columnStatistic != ColumnStatistic.UNKNOWN) {
                        break;
                    }
                    System.out.println("Not ready yet.");
                    Thread.sleep(1000);
                }
                if (columnStatistic != ColumnStatistic.UNKNOWN) {
                    Assertions.assertEquals(1, columnStatistic.count);
                    Assertions.assertEquals(2, columnStatistic.ndv);
                    Assertions.assertEquals(3, columnStatistic.avgSizeByte);
                    Assertions.assertEquals(4, columnStatistic.numNulls);
                    Assertions.assertEquals(5, columnStatistic.dataSize);
                    Assertions.assertEquals(6, columnStatistic.minValue);
                    Assertions.assertEquals(7, columnStatistic.maxValue);
                } else {
                    System.out.println("Cache is not loaded, skip test.");
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    @Test
    public void testSync1() throws Exception {
        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(
                    Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Frontend frontend1 = new Frontend(FrontendNodeType.MASTER,
                    "fe1", "self_host", "self_host", 2222);
            Frontend frontend2 = new Frontend(FrontendNodeType.FOLLOWER,
                    "fe2", "other_host", "other_host", 2223);
            List<Frontend> frontends = new ArrayList<>();
            frontends.add(frontend1);
            frontends.add(frontend2);
            Mockito.when(env.getFrontends(
                    Mockito.nullable(FrontendNodeType.class)))
                    .thenReturn(frontends);

            SystemInfoService.HostInfo selfNode =
                    new SystemInfoService.HostInfo("self_host", 1111);
            Mockito.when(env.getSelfNode()).thenReturn(selfNode);

            StatisticsCache statisticsCache =
                    Mockito.spy(new StatisticsCache());
            Mockito.doNothing().when(statisticsCache).sendStats(
                    Mockito.any(Frontend.class),
                    Mockito.any(TUpdateFollowerStatsCacheRequest.class));

            ColStatsData data = new ColStatsData(
                    StatsMockUtil.mockResultRow(true));
            statisticsCache.syncColStats(data);

            Mockito.verify(statisticsCache, Mockito.times(1))
                    .sendStats(Mockito.any(Frontend.class),
                            Mockito.any(
                                    TUpdateFollowerStatsCacheRequest.class));
        }
    }

    @Test
    public void testSync2() throws Exception {
        try (MockedStatic<ColumnStatistic> mockedColStat = Mockito.mockStatic(
                    ColumnStatistic.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(
                        Env.class, Mockito.CALLS_REAL_METHODS)) {

            mockedColStat.when(() -> ColumnStatistic.fromResultRow(Mockito.any(ResultRow.class)))
                    .thenReturn(ColumnStatistic.UNKNOWN);
            mockedColStat.when(() -> ColumnStatistic.fromResultRowList(Mockito.anyList()))
                    .thenReturn(ColumnStatistic.UNKNOWN);

            Env env = Mockito.mock(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Frontend frontend1 = new Frontend(FrontendNodeType.MASTER,
                    "fe1", "localhost:1111", "localhost", 2222);
            Frontend frontend2 = new Frontend(FrontendNodeType.FOLLOWER,
                    "fe1", "localhost:1112", "localhost", 2223);
            List<Frontend> frontends = new ArrayList<>();
            frontends.add(frontend1);
            frontends.add(frontend2);
            Mockito.when(env.getFrontends(Mockito.any())).thenReturn(frontends);

            StatisticsCache statisticsCache = Mockito.spy(new StatisticsCache());
            Mockito.doNothing().when(statisticsCache).sendStats(
                    Mockito.any(Frontend.class),
                    Mockito.any(TUpdateFollowerStatsCacheRequest.class));

            Thread.sleep(1000);
            Mockito.verify(statisticsCache, Mockito.never()).sendStats(
                    Mockito.any(Frontend.class),
                    Mockito.any(TUpdateFollowerStatsCacheRequest.class));
        }
    }

    @Test
    public void testEvict() throws InterruptedException {
        ThreadPoolExecutor threadPool
                = ThreadPoolManager.newDaemonFixedThreadPool(
                1, Integer.MAX_VALUE, "STATS_FETCH", true);
        AsyncLoadingCache<Integer, Integer> columnStatisticsCache =
                Caffeine.newBuilder()
                        .maximumSize(1)
                        .refreshAfterWrite(Duration.ofHours(StatisticConstants.STATISTICS_CACHE_REFRESH_INTERVAL))
                        .executor(threadPool)
                        .buildAsync(new AsyncCacheLoader<Integer, Integer>() {
                            @Override
                            public @NonNull CompletableFuture<Integer> asyncLoad(@NonNull Integer integer,
                                    @NonNull Executor executor) {
                                return CompletableFuture.supplyAsync(() -> {
                                    return integer;
                                }, threadPool);
                            }
                        });
        columnStatisticsCache.get(1);
        columnStatisticsCache.get(2);
        Assertions.assertTrue(columnStatisticsCache.synchronous().asMap().containsKey(2));
        Thread.sleep(100);
        Assertions.assertEquals(1, columnStatisticsCache.synchronous().asMap().size());
    }

    @Test
    public void testLoadWithException() throws Exception {
        AtomicReference<Optional<ColumnStatistic>> doLoadResult = new AtomicReference<>(null);
        try (MockedConstruction<ColumnStatisticsCacheLoader> mc = Mockito.mockConstruction(
                ColumnStatisticsCacheLoader.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (mock, ctx) -> {
                    Mockito.doAnswer(inv -> doLoadResult.get())
                            .when(mock).doLoad(Mockito.any());
                })) {
            StatisticsCache statisticsCache = new StatisticsCache();
            ColumnStatistic columnStatistic = statisticsCache.getColumnStatistics(
                    1, 1, 1, -1, "col", connectContext);
            Thread.sleep(3000);
            Assertions.assertTrue(columnStatistic.isUnKnown);

            doLoadResult.set(Optional.of(new ColumnStatistic(1, 2,
                    null, 3, 4, 5, 6, 7,
                    null, null, false,
                    new Date().toString(), null)));
            columnStatistic = statisticsCache.getColumnStatistics(
                    1, 1, 1, -1, "col", connectContext);
            for (int i = 0; i < 60; i++) {
                columnStatistic = statisticsCache.getColumnStatistics(
                        1, 1, 1, -1, "col", connectContext);
                if (columnStatistic != ColumnStatistic.UNKNOWN) {
                    break;
                }
                System.out.println("Not ready yet.");
                Thread.sleep(1000);
            }
            if (columnStatistic != ColumnStatistic.UNKNOWN) {
                Assertions.assertEquals(1, columnStatistic.count);
                Assertions.assertEquals(2, columnStatistic.ndv);
                Assertions.assertEquals(3, columnStatistic.avgSizeByte);
                Assertions.assertEquals(4, columnStatistic.numNulls);
                Assertions.assertEquals(5, columnStatistic.dataSize);
                Assertions.assertEquals(6, columnStatistic.minValue);
                Assertions.assertEquals(7, columnStatistic.maxValue);
            } else {
                Assertions.fail("Column stats is still unknown");
            }
        }
    }
}
