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

package org.apache.doris.statistics.repository;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.AlterColumnStatsCommand;
import org.apache.doris.statistics.analysis.AnalysisManager;
import org.apache.doris.statistics.cache.StatisticsCache;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.StatsType;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FileAlterColumnStatisticsTest {
    @Test
    void testMissingHotValuesPersistAsSqlNullAndReload() throws Exception {
        try (Fixture fixture = new Fixture(5, 2, 73)) {
            StatisticsRepository.alterColumnStatistics(fixture.command);
            Map<String, String> params = fixture.writes.get(0);
            Assertions.assertEquals("NULL", params.get("hotValues"));
            Assertions.assertEquals("NULL", params.get("ndv"));
            Assertions.assertEquals("NULL", params.get("min"));
            Assertions.assertEquals("NULL", params.get("max"));
            Assertions.assertFalse(fixture.sql.get(0).contains("'null'"));
            Assertions.assertTrue(fixture.sql.get(0).endsWith(", NULL)"));
            ArgumentCaptor<ColStatsData> cached = ArgumentCaptor.forClass(ColStatsData.class);
            Mockito.verify(fixture.cache).syncColStats(cached.capture());
            Assertions.assertTrue(cached.getValue().ndvUnavailable);
            Assertions.assertNull(cached.getValue().hotValues);
            Assertions.assertTrue(cached.getValue().isValid());
            // Model the persisted SQL NULLs returned by the statistics table, independently
            // of the object sent to the cache during the ALTER operation.
            ResultRow persisted = new ResultRow(Arrays.asList(params.get("id"), params.get("catalogId"),
                    params.get("dbId"), params.get("tblId"), params.get("idxId"), params.get("colId"), null,
                    params.get("count"), null, params.get("nullCount"), null, null, params.get("dataSize"),
                    "2026-09-16", null));
            ColStatsData reloaded = new ColStatsData(persisted);
            Assertions.assertTrue(reloaded.isValid());
            ColumnStatistic stats = reloaded.toColumnStatistic();
            Assertions.assertFalse(stats.isUnKnown);
            Assertions.assertTrue(stats.ndvUnavailable);
            Assertions.assertEquals(5, stats.count);
            Assertions.assertEquals(2, stats.numNulls);
            Assertions.assertEquals(73, stats.dataSize);
            Assertions.assertEquals(14.6, stats.avgSizeByte);
        }
    }

    @Test
    void testUnavailableMetricsRejectedBeforeWritesAndCacheUpdates() throws Exception {
        for (StatsType metric : Arrays.asList(StatsType.NDV, StatsType.MIN_VALUE,
                StatsType.MAX_VALUE, StatsType.HOT_VALUES)) {
            for (String value : Arrays.asList("0", "")) {
                try (Fixture fixture = new Fixture(5, 2, 73)) {
                    Mockito.when(fixture.command.getValue(metric)).thenReturn(value);
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            () -> StatisticsRepository.alterColumnStatistics(fixture.command));
                    Assertions.assertTrue(error.getMessage().contains("FILE NDV, MIN, MAX and hot values"));
                    Assertions.assertTrue(fixture.writes.isEmpty());
                    Mockito.verifyNoInteractions(fixture.cache, fixture.manager);
                }
            }
        }
    }

    @Test
    void testEmptyAndAllNullManualStatisticsKeepZeroBytes() throws Exception {
        for (long count : new long[] {0, 5}) {
            try (Fixture fixture = new Fixture(count, count, 0)) {
                StatisticsRepository.alterColumnStatistics(fixture.command);
                Map<String, String> params = fixture.writes.get(0);
                Assertions.assertEquals("NULL", params.get("hotValues"));
                Assertions.assertEquals("0.0", params.get("dataSize"));
                ArgumentCaptor<ColStatsData> cached = ArgumentCaptor.forClass(ColStatsData.class);
                Mockito.verify(fixture.cache).syncColStats(cached.capture());
                ColumnStatistic stats = cached.getValue().toColumnStatistic();
                Assertions.assertTrue(stats.ndvUnavailable);
                Assertions.assertFalse(stats.isUnKnown);
                Assertions.assertEquals(count, stats.count);
                Assertions.assertEquals(count, stats.numNulls);
                Assertions.assertEquals(0, stats.dataSize);
                Assertions.assertEquals(0, stats.avgSizeByte);
            }
        }
    }

    private static class Fixture implements AutoCloseable {
        private final AlterColumnStatsCommand command = Mockito.mock(AlterColumnStatsCommand.class);
        private final StatisticsCache cache = Mockito.mock(StatisticsCache.class);
        private final AnalysisManager manager = Mockito.mock(AnalysisManager.class);
        private final List<Map<String, String>> writes = new ArrayList<>();
        private final List<String> sql = new ArrayList<>();
        private final MockedStatic<Env> environment;
        private final MockedStatic<StatisticsUtil> utility;

        Fixture(long count, long nulls, long bytes) {
            Env env = Mockito.mock(Env.class);
            InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
            Database db = Mockito.mock(Database.class);
            OlapTable table = Mockito.mock(OlapTable.class);
            TableNameInfo name = new TableNameInfo("internal", "db", "t");
            Column file = new Column("f", Type.FILE);
            Mockito.when(catalog.getId()).thenReturn(0L);
            Mockito.when(db.getId()).thenReturn(1L);
            Mockito.when(table.getId()).thenReturn(2L);
            Mockito.when(table.getColumn("f")).thenReturn(file);
            Mockito.when(command.getTableNameInfo()).thenReturn(name);
            Mockito.when(command.getPartitionIds()).thenReturn(Collections.emptyList());
            Mockito.when(command.getColumnName()).thenReturn("f");
            Mockito.when(command.getIndexId()).thenReturn(-1L);
            Mockito.when(command.getValue(StatsType.ROW_COUNT)).thenReturn(String.valueOf(count));
            Mockito.when(command.getValue(StatsType.NUM_NULLS)).thenReturn(String.valueOf(nulls));
            Mockito.when(command.getValue(StatsType.DATA_SIZE)).thenReturn(String.valueOf(bytes));
            Mockito.when(env.getStatisticsCache()).thenReturn(cache);
            Mockito.when(env.getAnalysisManager()).thenReturn(manager);
            environment = Mockito.mockStatic(Env.class);
            environment.when(Env::getCurrentEnv).thenReturn(env);
            utility = Mockito.mockStatic(StatisticsUtil.class);
            utility.when(() -> StatisticsUtil.convertTableNameToObjects(name))
                    .thenReturn(new DBObjects(catalog, db, table));
            utility.when(() -> StatisticsUtil.findColumn(0, 1, 2, -1, "f")).thenReturn(file);
            utility.when(() -> StatisticsUtil.execUpdate(Mockito.anyString(), Mockito.anyMap()))
                    .thenAnswer(invocation -> {
                        Map<String, String> params = invocation.getArgument(1);
                        writes.add(new HashMap<>(params));
                        sql.add(new StringSubstitutor(params).replace((String) invocation.getArgument(0)));
                        return null;
                    });
        }

        @Override
        public void close() {
            utility.close();
            environment.close();
        }
    }
}
