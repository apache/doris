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

package org.apache.doris.statistics.cache;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FileType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.trees.plans.commands.ShowColumnStatsCommand;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.statistics.analysis.AnalysisManager;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.PartitionColumnStatistic;
import org.apache.doris.statistics.model.PartitionColumnStatisticBuilder;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.repository.StatisticsRepository;
import org.apache.doris.statistics.util.Hll128;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class FileStatisticsCacheTest {
    @Test
    void testBothCacheLoadersRetainAvailableCounts() {
        ResultRow tableRow = new ResultRow(Arrays.asList("2--1-f", "0", "1", "2", "-1", "f", null,
                "10", null, "3", null, null, "70", "2026-09-16", null));
        ResultRow partRow = new ResultRow(Arrays.asList("0", "1", "2", "-1", "p1", "f",
                "10", "AA==", "3", null, null, "70", "2026-09-16"));
        try (MockedStatic<StatisticsUtil> utility = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<StatisticsRepository> repository = Mockito.mockStatic(StatisticsRepository.class)) {
            utility.when(() -> StatisticsUtil.findColumn(0, 1, 2, -1, "f"))
                    .thenReturn(new Column("f", FileType.create()));
            repository.when(() -> StatisticsRepository.loadColStats(0, 1, 2, -1, "f"))
                    .thenReturn(Collections.singletonList(tableRow));
            repository.when(() -> StatisticsRepository.loadPartitionColumnStats(0, 1, 2, -1, "'p1'", "f"))
                    .thenReturn(Collections.singletonList(partRow));
            Optional<ColumnStatistic> table = new ColumnStatisticsCacheLoader()
                    .doLoad(new StatisticsCacheKey(0, 1, 2, -1, "f"));
            Assertions.assertTrue(table.orElseThrow().ndvUnavailable);
            Assertions.assertFalse(table.orElseThrow().isUnKnown);
            Assertions.assertEquals(70, table.orElseThrow().dataSize);
            Optional<PartitionColumnStatistic> part = new PartitionColumnStatisticCacheLoader()
                    .doLoad(new PartitionColumnStatisticCacheKey(0, 1, 2, -1, "p1", "f"));
            Assertions.assertTrue(part.orElseThrow().ndvUnavailable);
            Assertions.assertFalse(part.orElseThrow().isUnKnown);
            Assertions.assertEquals(3, part.orElseThrow().numNulls);
        }
    }

    @Test
    void testShowTableAndPartitionCachedAndPersisted() {
        Env env = Mockito.mock(Env.class);
        AnalysisManager manager = Mockito.mock(AnalysisManager.class);
        Mockito.when(env.getAnalysisManager()).thenReturn(manager);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        ColumnStatistic column = new ColumnStatisticBuilder(10).setNdvUnavailable(true)
                .setNumNulls(3).setDataSize(70).setAvgSizeByte(7).build();
        PartitionColumnStatistic partition = new PartitionColumnStatisticBuilder(10).setNdv(new Hll128())
                .setNdvUnavailable(true).setNumNulls(3).setDataSize(70).setAvgSizeByte(7).build();
        ShowColumnStatsCommand command = new ShowColumnStatsCommand(
                new TableNameInfo("internal", "db", "t"), Collections.singletonList("f"), null, true);
        Deencapsulation.setField(command, "table", table);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            ShowResultSet tableResult = Deencapsulation.invoke(command, "constructResultSet",
                    Collections.singletonList(Pair.of(Pair.of("t", "f"), column)));
            List<String> row = tableResult.getResultRows().get(0);
            Assertions.assertEquals("10.0", row.get(2));
            Assertions.assertEquals("N/A", row.get(3));
            Assertions.assertEquals("3.0", row.get(4));
            Assertions.assertEquals("70.0", row.get(5));
            Assertions.assertEquals(Arrays.asList("N/A", "N/A"), row.subList(7, 9));
            Map<PartitionColumnStatisticCacheKey, PartitionColumnStatistic> cached = Collections.singletonMap(
                    new PartitionColumnStatisticCacheKey(0, 1, 2, -1, "p1", "f"), partition);
            ShowResultSet cachedResult = Deencapsulation.invoke(command, "constructPartitionCachedColumnStats",
                    cached, table);
            row = cachedResult.getResultRows().get(0);
            Assertions.assertEquals("N/A", row.get(4));
            Assertions.assertEquals(Arrays.asList("N/A", "N/A"), row.subList(6, 8));
            ResultRow persisted = new ResultRow(Arrays.asList("f", "p1", "-1", "10", null,
                    "3", null, null, "70", "2026-09-16"));
            ShowResultSet persistedResult = Deencapsulation.invoke(command, "constructPartitionResultSet",
                    Collections.singletonList(persisted), table);
            row = persistedResult.getResultRows().get(0);
            Assertions.assertEquals("N/A", row.get(4));
            Assertions.assertEquals(Arrays.asList("N/A", "N/A"), row.subList(6, 8));
            Assertions.assertEquals("70", row.get(8));
        }
    }

    @Test
    void testPartitionRepositoryDoesNotExposeCarrierAsZero() {
        OlapTable table = Mockito.mock(OlapTable.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(table.isPartitionedTable()).thenReturn(true);
        Mockito.when(table.getVisibleColumn("f")).thenReturn(new Column("f", FileType.create()));
        Mockito.when(table.getVisibleColumn("i")).thenReturn(new Column("i", Type.INT));
        try (MockedStatic<StatisticsUtil> mocked = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            mocked.when(() -> StatisticsUtil.executeQuery(Mockito.anyString(), Mockito.anyMap()))
                    .thenAnswer(invocation -> {
                        Map<String, String> params = invocation.getArgument(1);
                        Assertions.assertEquals("IF(col_id IN ('f'), NULL, hll_cardinality(ndv))",
                                params.get("partitionNdv"));
                        return Collections.emptyList();
                    });
            StatisticsRepository.queryColumnStatisticsByPartitions(table,
                    new java.util.HashSet<>(Arrays.asList("f", "i")), Collections.singletonList("p1"));
        }
    }
}
