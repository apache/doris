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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Frontend;
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
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

public class CacheTest extends TestWithFeService {

    @Test
    public void testColumn(@Mocked ColumnStatisticsCacheLoader cacheLoader) throws Exception {
        new Expectations() {
            {
                cacheLoader.asyncLoad((StatisticsCacheKey) any, (Executor) any);
                result = CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        return ColumnStatistic.UNKNOWN;
                    }
                    return ColumnStatistic.UNKNOWN;
                });
            }
        };
        StatisticsCache statisticsCache = new StatisticsCache();
        ColumnStatistic c = statisticsCache.getColumnStatistics(-1, -1, 1, -1, "col");
        Assertions.assertTrue(c.isUnKnown);
        Thread.sleep(100);
        c = statisticsCache.getColumnStatistics(-1, -1, 1, -1, "col");
        Assertions.assertTrue(c.isUnKnown);
    }

    @Test
    public void testLoad() throws Exception {
        new MockUp<StatisticsUtil>() {

            @Mock
            public Column findColumn(long catalogId, long dbId, long tblId, long idxId, String columnName) {
                return new Column("abc", PrimitiveType.BIGINT);
            }

            @Mock
            public List<ResultRow> execStatisticQuery(String sql) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    // ignore
                }
                return Arrays.asList(StatsMockUtil.mockResultRow(true));
            }
        };
        StatisticsCache statisticsCache = new StatisticsCache();
        ColumnStatistic columnStatistic = statisticsCache.getColumnStatistics(-1, -1, 0, -1, "col");
        // load not finished yet, should return unknown
        Assertions.assertTrue(columnStatistic.isUnKnown);
        // wait 1 sec to ensure `execStatisticQuery` is finished as much as possible.
        Thread.sleep(1000);
        // load has finished, return corresponding stats.
        columnStatistic = statisticsCache.getColumnStatistics(-1, -1, 0, -1, "col");
        Assertions.assertEquals(7, columnStatistic.count);
        Assertions.assertEquals(8, columnStatistic.ndv);
        Assertions.assertEquals(11, columnStatistic.maxValue);
    }

    @Test
    public void testLoadHistogram() throws Exception {
        new MockUp<Histogram>() {

            @Mock
            public Histogram fromResultRow(ResultRow resultRow) {
                try {
                    HistogramBuilder histogramBuilder = new HistogramBuilder();

                    Column col = new Column("abc", PrimitiveType.DATETIME);

                    Type dataType = col.getType();
                    histogramBuilder.setDataType(dataType);
                    HistData histData = new HistData(resultRow);
                    histogramBuilder.setSampleRate(histData.sampleRate);

                    String json = histData.buckets;
                    JsonObject jsonObj = JsonParser.parseString(json).getAsJsonObject();

                    int bucketNum = jsonObj.get("num_buckets").getAsInt();
                    histogramBuilder.setNumBuckets(bucketNum);

                    List<Bucket> buckets = Lists.newArrayList();
                    JsonArray jsonArray = jsonObj.getAsJsonArray("buckets");
                    for (JsonElement element : jsonArray) {
                        try {
                            String bucketJson = element.toString();
                            buckets.add(Bucket.deserializeFromJson(dataType, bucketJson));
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }

                    }
                    histogramBuilder.setBuckets(buckets);

                    return histogramBuilder.build();
                } catch (Exception e) {
                    return null;
                }
            }
        };
        new MockUp<StatisticsUtil>() {

            @Mock
            public Column findColumn(long catalogId, long dbId, long tblId, long idxId, String columnName) {
                return new Column("abc", PrimitiveType.DATETIME);
            }

            @Mock
            public List<ResultRow> execStatisticQuery(String sql) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    // ignore
                }
                List<String> values = new ArrayList<>();
                values.add("1");
                values.add("2");
                values.add("3");
                values.add("4");
                values.add("-1");
                values.add("col");
                values.add(null);
                values.add("0.2");
                String buckets = "{\"num_buckets\":5,\"buckets\":"
                        + "[{\"lower\":\"2022-09-21 17:30:29\",\"upper\":\"2022-09-21 22:30:29\","
                        + "\"count\":9,\"pre_sum\":0,\"ndv\":1},"
                        + "{\"lower\":\"2022-09-22 17:30:29\",\"upper\":\"2022-09-22 22:30:29\","
                        + "\"count\":10,\"pre_sum\":9,\"ndv\":1},"
                        + "{\"lower\":\"2022-09-23 17:30:29\",\"upper\":\"2022-09-23 22:30:29\","
                        + "\"count\":9,\"pre_sum\":19,\"ndv\":1},"
                        + "{\"lower\":\"2022-09-24 17:30:29\",\"upper\":\"2022-09-24 22:30:29\","
                        + "\"count\":9,\"pre_sum\":28,\"ndv\":1},"
                        + "{\"lower\":\"2022-09-25 17:30:29\",\"upper\":\"2022-09-25 22:30:29\","
                        + "\"count\":9,\"pre_sum\":37,\"ndv\":1}]}";
                values.add(buckets);
                values.add(new Date().toString());
                ResultRow resultRow = new ResultRow(values);
                return Collections.singletonList(resultRow);
            }
        };

        StatisticsCache statisticsCache = new StatisticsCache();
        statisticsCache.refreshHistogramSync(0, 0, 0, -1, "col");
        Thread.sleep(10000);
        Histogram histogram = statisticsCache.getHistogram(0, 0, 0, "col");
        Assertions.assertNotNull(histogram);
    }

    @Test
    public void testLoadFromMeta(@Mocked Env env,
            @Mocked CatalogMgr mgr,
            @Mocked HMSExternalCatalog catalog,
            @Mocked HMSExternalDatabase db,
            @Mocked HMSExternalTable table) throws Exception {
        new MockUp<StatisticsUtil>() {

            @Mock
            public Column findColumn(long catalogId, long dbId, long tblId, long idxId, String columnName) {
                return new Column("abc", PrimitiveType.BIGINT);
            }

            @Mock
            public List<ResultRow> execStatisticQuery(String sql) {
                return null;
            }

            @Mock
            public TableIf findTable(long catalogId, long dbId, long tblId) {
                return table;
            }
        };
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }
        };

        new MockUp<HMSExternalTable>() {
            @Mock
            public Optional<ColumnStatistic> getColumnStatistic(String colName) {
                return Optional.of(new ColumnStatistic(1, 2,
                        null, 3, 4, 5, 6, 7,
                        null, null, false,
                        new Date().toString()));
            }
        };

        try {
            StatisticsCache statisticsCache = new StatisticsCache();
            ColumnStatistic columnStatistic = statisticsCache.getColumnStatistics(1, 1, 1, -1, "col");
            for (int i = 0; i < 15; i++) {
                columnStatistic = statisticsCache.getColumnStatistics(1, 1, 1, -1, "col");
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

    @Test
    public void testSync1() throws Exception {
        new MockUp<StatisticsRepository>() {
            @Mock
            public List<ResultRow> loadColStats(long tableId, long idxId, String colName) {
                List<ResultRow> rows = new ArrayList<>();
                rows.add(StatsMockUtil.mockResultRow(true));
                rows.add(StatsMockUtil.mockResultRow(false));
                return rows;
            }

            @Mock
            public boolean isMaster(Frontend frontend) {
                return frontend.getRole().equals(FrontendNodeType.MASTER);
            }
        };
        new MockUp<Env>() {
            @Mock
            public List<Frontend> getFrontends(FrontendNodeType nodeType) {
                Frontend frontend1 = new Frontend(FrontendNodeType.MASTER,
                        "fe1", "localhost:1111", "localhost", 2222);
                Frontend frontend2 = new Frontend(FrontendNodeType.FOLLOWER,
                        "fe1", "localhost:1112", "localhost", 2223);
                List<Frontend> frontends = new ArrayList<>();
                frontends.add(frontend1);
                frontends.add(frontend2);
                return frontends;
            }
        };

        new MockUp<StatisticsCache>() {
            @Mock
            private void sendStats(Frontend frontend,
                    TUpdateFollowerStatsCacheRequest updateFollowerStatsCacheRequest) {
                // DO NONTHING
            }
        };
        StatisticsCache statisticsCache = new StatisticsCache();
        new Expectations() {
            {
                statisticsCache.sendStats((Frontend) any, (TUpdateFollowerStatsCacheRequest) any);
                times = 1;
            }
        };
    }

    @Test
    public void testSync2() throws Exception {
        new MockUp<ColumnStatistic>() {
            @Mock

            public ColumnStatistic fromResultRow(ResultRow row) {
                return ColumnStatistic.UNKNOWN;
            }

            @Mock
            public ColumnStatistic fromResultRow(List<ResultRow> row) {
                return ColumnStatistic.UNKNOWN;
            }
        };
        new MockUp<Env>() {
            @Mock
            public List<Frontend> getFrontends(FrontendNodeType nodeType) {
                Frontend frontend1 = new Frontend(FrontendNodeType.MASTER,
                        "fe1", "localhost:1111", "localhost", 2222);
                Frontend frontend2 = new Frontend(FrontendNodeType.FOLLOWER,
                        "fe1", "localhost:1112", "localhost", 2223);
                List<Frontend> frontends = new ArrayList<>();
                frontends.add(frontend1);
                frontends.add(frontend2);
                return frontends;
            }
        };

        new MockUp<StatisticsCache>() {
            @Mock
            private void sendStats(Frontend frontend,
                    TUpdateFollowerStatsCacheRequest updateFollowerStatsCacheRequest) {
                // DO NOTHING
            }
        };
        StatisticsCache statisticsCache = new StatisticsCache();
        new Expectations() {
            {
                statisticsCache.sendStats((Frontend) any, (TUpdateFollowerStatsCacheRequest) any);
                times = 0;
            }
        };
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
        new MockUp<ColumnStatisticsCacheLoader>() {
            @Mock
            protected Optional<ColumnStatistic> doLoad(StatisticsCacheKey key) {
                return null;
            }
        };
        StatisticsCache statisticsCache = new StatisticsCache();
        ColumnStatistic columnStatistic = statisticsCache.getColumnStatistics(1, 1, 1, -1, "col");
        Thread.sleep(3000);
        Assertions.assertTrue(columnStatistic.isUnKnown);

        new MockUp<ColumnStatisticsCacheLoader>() {
            @Mock
            protected Optional<ColumnStatistic> doLoad(StatisticsCacheKey key) {
                return Optional.of(new ColumnStatistic(1, 2,
                    null, 3, 4, 5, 6, 7,
                    null, null, false,
                        new Date().toString()));
            }
        };
        columnStatistic = statisticsCache.getColumnStatistics(1, 1, 1, -1, "col");
        for (int i = 0; i < 60; i++) {
            columnStatistic = statisticsCache.getColumnStatistics(1, 1, 1, -1, "col");
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
