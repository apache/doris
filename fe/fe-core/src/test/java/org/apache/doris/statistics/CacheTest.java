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
        new MockUp<ColumnStatistic>() {

            @Mock
            public Column findColumn(long catalogId, long dbId, long tblId, String columnName) {
                return new Column("abc", PrimitiveType.BIGINT);
            }
        };
        new MockUp<StatisticsUtil>() {

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
                List<String> values = new ArrayList<>();
                values.add("1");
                values.add("2");
                values.add("3");
                values.add("4");
                values.add("5");
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
        Thread.sleep(100);
        columnStatistic = statisticsCache.getColumnStatistics(0, "col");
        Assertions.assertEquals(1, columnStatistic.count);
        Assertions.assertEquals(2, columnStatistic.ndv);
        Assertions.assertEquals(10, columnStatistic.maxValue);
    }
}
