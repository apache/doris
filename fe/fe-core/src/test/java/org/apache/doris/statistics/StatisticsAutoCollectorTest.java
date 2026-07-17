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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class StatisticsAutoCollectorTest {

    @Test
    public void testFetchJob() {
        AnalysisManager manager = new AnalysisManager();
        TableNameInfo high1 = new TableNameInfo("catalog", "db", "high1");
        TableNameInfo high2 = new TableNameInfo("catalog", "db", "high2");
        TableNameInfo mid1 = new TableNameInfo("catalog", "db", "mid1");
        TableNameInfo mid2 = new TableNameInfo("catalog", "db", "mid2");
        TableNameInfo low1 = new TableNameInfo("catalog", "db", "low1");

        manager.highPriorityJobs.put(high1, new HashSet<>());
        manager.highPriorityJobs.get(high1).add(Pair.of("index1", "col1"));
        manager.highPriorityJobs.get(high1).add(Pair.of("index1", "col2"));
        manager.highPriorityJobs.put(high2, new HashSet<>());
        manager.highPriorityJobs.get(high2).add(Pair.of("index1", "col3"));
        manager.midPriorityJobs.put(mid1, new HashSet<>());
        manager.midPriorityJobs.get(mid1).add(Pair.of("index1", "col4"));
        manager.midPriorityJobs.put(mid2, new HashSet<>());
        manager.midPriorityJobs.get(mid2).add(Pair.of("index1", "col5"));
        manager.lowPriorityJobs.put(low1, new HashSet<>());
        manager.lowPriorityJobs.get(low1).add(Pair.of("index1", "col6"));
        manager.lowPriorityJobs.get(low1).add(Pair.of("index1", "col7"));

        Env mockEnv = Mockito.mock(Env.class);
        Mockito.when(mockEnv.getAnalysisManager()).thenReturn(manager);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(mockEnv);
            envStatic.when(Env::getServingEnv).thenReturn(mockEnv);
            StatisticsAutoCollector collector = new StatisticsAutoCollector();
            Pair<Entry<TableNameInfo, Set<Pair<String, String>>>, JobPriority> job = collector.getJob();
            Assertions.assertEquals(high1, job.first.getKey());
            Assertions.assertEquals(2, job.first.getValue().size());
            Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col1")));
            Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col2")));
            Assertions.assertEquals(JobPriority.HIGH, job.second);

            job = collector.getJob();
            Assertions.assertEquals(high2, job.first.getKey());
            Assertions.assertEquals(1, job.first.getValue().size());
            Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col3")));
            Assertions.assertEquals(JobPriority.HIGH, job.second);

            job = collector.getJob();
            Assertions.assertEquals(mid1, job.first.getKey());
            Assertions.assertEquals(1, job.first.getValue().size());
            Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col4")));
            Assertions.assertEquals(JobPriority.MID, job.second);

            job = collector.getJob();
            Assertions.assertEquals(mid2, job.first.getKey());
            Assertions.assertEquals(1, job.first.getValue().size());
            Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col5")));
            Assertions.assertEquals(JobPriority.MID, job.second);

            job = collector.getJob();
            Assertions.assertEquals(low1, job.first.getKey());
            Assertions.assertEquals(2, job.first.getValue().size());
            Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col6")));
            Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col7")));
            Assertions.assertEquals(JobPriority.LOW, job.second);

            job = collector.getJob();
            Assertions.assertNull(job);
        }
    }

    @Test
    public void testSupportAutoAnalyze() throws DdlException {
        StatisticsAutoCollector collector = new StatisticsAutoCollector();
        Assertions.assertFalse(collector.supportAutoAnalyze(null));
        Column column1 = new Column("placeholder", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column1);
        OlapTable table1 = new OlapTable(200, "testTable", schema, null, null, null);
        Assertions.assertTrue(collector.supportAutoAnalyze(table1));

        // A plugin-driven table is admitted to auto-analyze IFF its connector declares column auto-analyze:
        // the capability — not the PluginDrivenExternalTable type — is the gate (post-cutover iceberg/paimon
        // declare it; jdbc/es do not). The real getConnector()->capability plumbing is covered by
        // PluginDrivenExternalTableTest; here we pin the whitelist decision in both directions.
        PluginDrivenExternalTable capablePluginTable = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(capablePluginTable.supportsColumnAutoAnalyze()).thenReturn(true);
        Assertions.assertTrue(collector.supportAutoAnalyze(capablePluginTable));

        PluginDrivenExternalTable incapablePluginTable = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(incapablePluginTable.supportsColumnAutoAnalyze()).thenReturn(false);
        Assertions.assertFalse(collector.supportAutoAnalyze(incapablePluginTable));
    }

    @Test
    public void testProcessOneJobForcesFullAnalyzeForCapablePluginTable() {
        // A flipped plugin table (iceberg/paimon) whose connector declares column auto-analyze must be
        // analyzed with FULL, never SAMPLE: ExternalAnalysisTask.doSample throws for external SQL-driven
        // tables. Force the SAMPLE precondition (huge data size, not partitioned) so only the plugin FULL
        // arm under test can flip the method to FULL. We assert via the isSampleAnalyze flag the decision
        // is passed to readyToSample with.
        StatisticsAutoCollector collector = Mockito.spy(new StatisticsAutoCollector());
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.supportsColumnAutoAnalyze()).thenReturn(true);
        Mockito.when(table.getDataSize(true)).thenReturn(Long.MAX_VALUE);
        Mockito.when(table.isPartitionedTable()).thenReturn(false);
        Mockito.when(table.getId()).thenReturn(1L);
        Mockito.when(table.getRowCount()).thenReturn(100L);

        AnalysisManager manager = Mockito.mock(AnalysisManager.class);
        Env mockEnv = Mockito.mock(Env.class);
        Mockito.when(mockEnv.getAnalysisManager()).thenReturn(manager);
        // Early-return out of processOneJob immediately after the analysis-method decision is consumed by
        // readyToSample, capturing the isSampleAnalyze flag it was called with.
        ArgumentCaptor<Boolean> isSampleAnalyze = ArgumentCaptor.forClass(Boolean.class);
        Mockito.doReturn(false).when(collector).readyToSample(Mockito.any(), Mockito.anyLong(),
                Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getServingEnv).thenReturn(mockEnv);
            collector.processOneJob(table, Sets.newHashSet(), JobPriority.HIGH);
        }

        Mockito.verify(collector).readyToSample(Mockito.eq(table), Mockito.anyLong(), Mockito.any(),
                Mockito.any(), isSampleAnalyze.capture());
        Assertions.assertFalse(isSampleAnalyze.getValue(),
                "plugin table with column-auto-analyze capability must use FULL analyze, not SAMPLE");
    }

    @Test
    public void testCreateAnalyzeJobForTbl() {
        StatisticsAutoCollector collector = new StatisticsAutoCollector();
        OlapTable table = Mockito.mock(OlapTable.class);
        Database db = Mockito.mock(Database.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(db.getId()).thenReturn(100L);
        Mockito.when(catalog.getId()).thenReturn(10L);


        Assertions.assertNull(collector.createAnalyzeJobForTbl(table, null, null, AnalysisMethod.SAMPLE, 100, null, 10));

        Set<Pair<String, String>> jobColumns = Sets.newHashSet();
        jobColumns.add(Pair.of("a", "b"));
        jobColumns.add(Pair.of("c", "d"));
        AnalysisInfo analyzeJobForTbl = collector.createAnalyzeJobForTbl(table, jobColumns, JobPriority.HIGH, AnalysisMethod.SAMPLE, 100,
                null, 10);
        Assertions.assertEquals("[a:b,c:d]", analyzeJobForTbl.colName);
        Assertions.assertEquals(JobPriority.HIGH, analyzeJobForTbl.priority);
        Assertions.assertEquals(AnalysisMethod.SAMPLE, analyzeJobForTbl.analysisMethod);
        Assertions.assertEquals(100, analyzeJobForTbl.rowCount);
        Assertions.assertEquals(10, analyzeJobForTbl.tableVersion);
    }

    @Test
    public void testReadyToSample() {
        StatisticsAutoCollector collector = new StatisticsAutoCollector();
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("table");
        Mockito.when(table.getRowCountForIndex(Mockito.anyLong(), Mockito.anyBoolean())).thenReturn(TableIf.UNKNOWN_ROW_COUNT);
        // not sample
        Assertions.assertTrue(collector.readyToSample(table, 100, null, null, false));
        // not fully reported.
        Assertions.assertFalse(collector.readyToSample(table, 100, null, null, true));

        Mockito.when(table.getRowCountForIndex(Mockito.anyLong(), Mockito.anyBoolean())).thenReturn(100L);
        // Row count is 0
        Assertions.assertFalse(collector.readyToSample(table, 0, null, null, true));
        // ready to sample
        Assertions.assertTrue(collector.readyToSample(table, 100, null, null, true));

    }
}
