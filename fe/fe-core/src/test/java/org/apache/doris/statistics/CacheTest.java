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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class CacheTest extends TestWithFeService {

    @Test
    public void testColumn(@Mocked StatisticsCacheLoader cacheLoader) throws Exception {
        new Expectations() {
            {
                cacheLoader.asyncLoad((StatisticsCacheKey) any, (Executor) any);
                result = CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        return ColumnStatistic.DEFAULT;
                    }
                    return ColumnStatistic.DEFAULT;
                });
            }
        };
        StatisticsCache statisticsCache = new StatisticsCache();
        ColumnStatistic c = statisticsCache.getColumnStatistics(1, "col");
        Assertions.assertEquals(c, ColumnStatistic.DEFAULT);
        Thread.sleep(100);
        c = statisticsCache.getColumnStatistics(1, "col");
        Assertions.assertEquals(c, ColumnStatistic.DEFAULT);
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
                List<String> colNames = new ArrayList<>();
                colNames.add("count");
                colNames.add("ndv");
                colNames.add("null_count");
                colNames.add("data_size_in_bytes");
                colNames.add("catalog_id");
                colNames.add("db_id");
                colNames.add("idx_id");
                colNames.add("tbl_id");
                colNames.add("col_id");
                colNames.add("min");
                colNames.add("max");
                List<PrimitiveType> primitiveTypes = new ArrayList<>();
                primitiveTypes.add(PrimitiveType.BIGINT);
                primitiveTypes.add(PrimitiveType.BIGINT);
                primitiveTypes.add(PrimitiveType.BIGINT);
                primitiveTypes.add(PrimitiveType.BIGINT);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                List<String> values = new ArrayList<>();
                values.add("1");
                values.add("2");
                values.add("3");
                values.add("4");
                values.add("5");
                values.add("-1");
                values.add("6");
                values.add("7");
                values.add("8");
                values.add("9");
                values.add("10");
                ResultRow resultRow = new ResultRow(colNames, primitiveTypes, values);
                return Arrays.asList(resultRow);
            }
        };
        StatisticsCache statisticsCache = new StatisticsCache();
        ColumnStatistic columnStatistic = statisticsCache.getColumnStatistics(0, "col");
        Assertions.assertEquals(ColumnStatistic.DEFAULT, columnStatistic);
        Thread.sleep(1000);
        columnStatistic = statisticsCache.getColumnStatistics(0, "col");
        Assertions.assertEquals(1, columnStatistic.count);
        Assertions.assertEquals(2, columnStatistic.ndv);
        Assertions.assertEquals(10, columnStatistic.maxValue);
    }

    @Test
    public void testLoadHistogram() throws Exception {
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
                List<String> colNames = new ArrayList<>();
                colNames.add("catalog_id");
                colNames.add("db_id");
                colNames.add("idx_id");
                colNames.add("tbl_id");
                colNames.add("col_id");
                colNames.add("sample_rate");
                colNames.add("buckets");
                List<PrimitiveType> primitiveTypes = new ArrayList<>();
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                primitiveTypes.add(PrimitiveType.VARCHAR);
                List<String> values = new ArrayList<>();
                values.add("1");
                values.add("2");
                values.add("3");
                values.add("-1");
                values.add("4");
                values.add("0.2");
                String buckets = "{\"max_bucket_num\":128,\"bucket_num\":5,\"sample_rate\":1.0,\"buckets\":"
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
                ResultRow resultRow = new ResultRow(colNames, primitiveTypes, values);
                return Collections.singletonList(resultRow);
            }
        };

        StatisticsCache statisticsCache = new StatisticsCache();
        Histogram histogram = statisticsCache.getHistogram(0, "col");
        Assertions.assertEquals(Histogram.DEFAULT, histogram);
        Thread.sleep(1000);
        histogram = statisticsCache.getHistogram(0, "col");
        Assertions.assertEquals("DATETIME", histogram.dataType.toString());
        Assertions.assertEquals(128, histogram.maxBucketNum);
        Assertions.assertEquals(5, histogram.bucketNum);
        Assertions.assertEquals(0.2, histogram.sampleRate);
        Assertions.assertEquals(5, histogram.buckets.size());
    }
}
