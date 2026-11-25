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

package org.apache.doris.statistics.analysis;

import org.apache.doris.analysis.TableSample;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.analysis.PluginDrivenSampleAnalysisTask.SamplePlan;
import org.apache.doris.statistics.analysis.TableStatisticsSqlBuilder.BatchedExternalColumnSpec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class ExternalSampleBatchCollectorTest {

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static PluginDrivenSampleAnalysisTask newExternalSampleTask(
            long jobId, ExternalTable table, Column column, TableSample sample) {
        PluginDrivenSampleAnalysisTask task = new PluginDrivenSampleAnalysisTask();
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getId()).thenReturn(10L);
        Mockito.when(catalog.getName()).thenReturn("ice_cat");
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        Mockito.when(db.getId()).thenReturn(20L);
        Mockito.when(db.getFullName()).thenReturn("db1");
        task.catalog = catalog;
        task.db = db;
        task.tbl = table;
        task.table = table;
        task.col = column;
        task.tableSample = sample;
        task.info = new AnalysisInfoBuilder()
                .setJobId(jobId)
                .setCatalogId(10L)
                .setDBId(20L)
                .setTblId(table.getId())
                .setColName(column.getName())
                .setIndexId(-1L)
                .setAnalysisMethod(AnalysisInfo.AnalysisMethod.SAMPLE)
                .setAnalysisType(AnalysisInfo.AnalysisType.FUNDAMENTALS)
                .build();
        return task;
    }

    @Test
    void testAnalysisJobCachesExternalChunkSizesOncePerTableWithoutMutation() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getId()).thenReturn(300L);
        AtomicInteger chunkCalls = new AtomicInteger();
        Mockito.when(table.getChunkSizes()).thenAnswer(inv -> {
            chunkCalls.incrementAndGet();
            return Arrays.asList(100L, 200L, 300L, 400L);
        });
        List<Column> schema = List.of(
                new Column("id", PrimitiveType.BIGINT),
                new Column("name", PrimitiveType.VARCHAR),
                new Column("ts", PrimitiveType.DATETIMEV2));
        Mockito.when(table.getFullSchema()).thenReturn(schema);
        Mockito.when(table.getDistributionColumnNames()).thenReturn(Set.of("id"));

        PluginDrivenSampleAnalysisTask t1 = newExternalSampleTask(
                1L, table, schema.get(0), new TableSample(true, 50L, 7L));
        PluginDrivenSampleAnalysisTask t2 = newExternalSampleTask(
                1L, table, schema.get(1), new TableSample(true, 50L, 7L));
        PluginDrivenSampleAnalysisTask t3 = newExternalSampleTask(
                1L, table, schema.get(2), new TableSample(true, 50L, 7L));

        Env mockEnv = Mockito.mock(Env.class);
        Mockito.when(mockEnv.getAnalysisManager()).thenReturn(Mockito.mock(AnalysisManager.class));
        AnalysisJob job;
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(mockEnv);
            job = new AnalysisJob(t1.info, List.of(t1, t2, t3));
        }

        Assertions.assertNotNull(t1.externalSampleBatch);
        Assertions.assertSame(t1.externalSampleBatch, t2.externalSampleBatch);
        Assertions.assertSame(t1.externalSampleBatch, t3.externalSampleBatch);

        Pair<Double, Long> s1 = t1.getSampleInfo();
        Pair<Double, Long> s2 = t2.getSampleInfo();
        Pair<Double, Long> s3 = t3.getSampleInfo();
        Assertions.assertEquals(1, chunkCalls.get(),
                "table.getChunkSizes() must execute only once per (job, table)");
        Assertions.assertEquals(s1, s2);
        Assertions.assertEquals(s1, s3);
        Assertions.assertEquals(List.of(100L, 200L, 300L, 400L), job.externalChunkSizes(table));
    }

    @Test
    void testBatchedExternalSampleSqlParsesCleanlyForLinearAndDuj1Columns() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getId()).thenReturn(500L);
        Mockito.when(table.getName()).thenReturn("events");
        Mockito.when(table.getChunkSizes()).thenReturn(List.of(
                1200L * 1024 * 1024, 1200L * 1024 * 1024));
        List<Column> schema = List.of(
                new Column("id", PrimitiveType.BIGINT),
                new Column("payload", PrimitiveType.STRING));
        Mockito.when(table.getFullSchema()).thenReturn(schema);
        Mockito.when(table.getDistributionColumnNames()).thenReturn(Set.of("id"));

        PluginDrivenSampleAnalysisTask task = newExternalSampleTask(
                9L, table, schema.get(0), new TableSample(false, 1000L, 11L));
        SamplePlan plan = task.planExternalSample();
        Assertions.assertTrue(plan.limited());
        Assertions.assertTrue(plan.limitClause().startsWith("limit "));
        Assertions.assertTrue(plan.scaleFactor() > 1.0);

        String fromClause = task.qualifiedExternalFromClause(plan);
        List<BatchedExternalColumnSpec> specs = List.of(
                new BatchedExternalColumnSpec(0, 10L, 20L, 500L, "id", "`id`", false, 8, true, -1L),
                new BatchedExternalColumnSpec(1, 10L, 20L, 500L, "payload", "`payload`", true, 16, false, 1024L));
        String sql = TableStatisticsSqlBuilder.batchedExternalSampleSql(
                fromClause, specs, plan.scaleFactor(), 10);

        ConnectContext prev = ConnectContext.get();
        ConnectContext ctx = new ConnectContext();
        ctx.getState().setInternal(true);
        ctx.setThreadLocalInfo();
        try {
            LogicalPlan parsed = new NereidsParser().parseSingle(sql);
            Assertions.assertNotNull(parsed);
        } finally {
            ConnectContext.remove();
            if (prev != null) {
                prev.setThreadLocalInfo();
            }
        }
        Assertions.assertTrue(sql.contains("TABLESAMPLE(1000 ROWS)"));
        Assertions.assertTrue(sql.contains("NDV(`sample_base`.`__s0`)"));
        Assertions.assertTrue(sql.contains("xxhash_64(SUBSTRING(CAST("));
    }
}
