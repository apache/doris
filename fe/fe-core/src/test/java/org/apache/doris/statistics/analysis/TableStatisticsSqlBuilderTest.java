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

import org.apache.doris.analysis.FixedBlockSampleSpec;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.plans.ComputeResultSet;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.sampling.SampledScanSqlRenderer;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FixedBlockScanClause;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class TableStatisticsSqlBuilderTest {
    @Test
    void hotEntriesEvaluateWithRealDorisExpressions() {
        ConnectContext ctx = new ConnectContext();
        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(ctx)) {
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setDatabase("stats_test");
            String entry = TableStatisticsSqlBuilder.HOT_ENTRY.replace("hot_top.column_key", "'a:;b'")
                    .replace("hot_top.freq", "1").replace("stats.sampled_non_null_rows", "3");
            Assertions.assertEquals(List.of("a\\:\\;b :0.33"), evaluate(ctx, "SELECT " + entry));
            // Inputs after the left join for all-NULL, empty or zero-top-k samples.
            for (int nonNullRows : List.of(0, 12)) {
                String empty = TableStatisticsSqlBuilder.HOT_ENTRY.replace("hot_top.column_key", "CAST(NULL AS STRING)")
                        .replace("hot_top.freq", "CAST(NULL AS BIGINT)")
                        .replace("stats.sampled_non_null_rows", Integer.toString(nonNullRows));
                Assertions.assertEquals(List.of(""), evaluate(ctx, "SELECT IFNULL(" + empty + ", '')"));
            }
        }
    }

    private static List<String> evaluate(ConnectContext ctx, String sql) {
        PlanChecker checker = PlanChecker.from(ctx).analyze(sql).rewrite().optimize();
        var plan = checker.getBestPlanTree();
        return ((ComputeResultSet) plan).computeResultInFe(checker.getCascadesContext(), plan.getOutput())
                .orElseThrow(() -> new AssertionError(plan.treeString())).getResultRows().get(0);
    }

    @Test
    void fixedHotAndUniquePlansHaveOnePhysicalSampleScan() throws Exception {
        OlapTable table = Mockito.spy(PlanConstructor.newOlapTable(100, "events", 0));
        table.setBaseIndexId(10);
        table.setIndexMeta(10, "events", table.getFullSchema(), 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setIndexMeta(11, "rollup_events", table.getFullSchema(), 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        Database db = new Database(10, "stats_test");
        Mockito.doReturn(db).when(table).getDatabase();
        table.setQualifiedDbName(db.getFullName());
        MaterializedIndex index = new MaterializedIndex(11, MaterializedIndex.IndexState.NORMAL);
        TabletMeta meta = new TabletMeta(10, 100, 12, 11, 0, TStorageMedium.HDD, false);
        Backend be = Mockito.spy(new Backend(14, "127.0.0.1", 9050));
        be.setAlive(true);
        Mockito.doReturn(true).when(be).isSupportsFixedBlockSamplingV1();
        Mockito.doReturn(ImmutableMap.of(be.getId(), be)).when(table).getAllBackendsByAllCluster();
        LocalTablet tablet = new LocalTablet(13);
        tablet.addReplica(new LocalReplica(15, be.getId(), Replica.ReplicaState.NORMAL, 2, 0), true);
        index.addTablet(tablet, meta, true);
        MaterializedIndex base = new MaterializedIndex(10, MaterializedIndex.IndexState.NORMAL);
        Partition part = new Partition(12, "events", base, table.getDefaultDistributionInfo());
        part.createRollupIndex(index);
        part.updateVisibleVersion(2);
        table.addPartition(part);
        TabletInvertedIndex inverted = Mockito.mock(TabletInvertedIndex.class);
        Mockito.when(inverted.getTabletMetaList(List.of(13L))).thenReturn(List.of(meta));
        var blocks = new FixedBlockSampleSpec(8, 123, List.of(new FixedBlockSampleSpec.Group(0.5, 0.5, 64, List.of(13L))));
        Map<String, String> params = new HashMap<>();
        params.put("index", "INDEX `rollup_events`");
        params.put("colName", "`name`");
        params.put("subStringColName", "xxhash_64(SUBSTRING(`name`, 1, 1024))");
        params.put("dataSizeFunction", "SUM(column_length)");
        params.put("rawDataSizeFunction", "SUM(LENGTH(`name`))");
        params.put("dbName", "stats_test");
        params.put("catalogName", "internal");
        params.put("tblName", "events");
        params.put("catalogId", "0");
        params.put("dbId", "10");
        params.put("tblId", "100");
        params.put("idxId", "-1");
        params.put("colId", "name");
        params.put("rowCount", "12345");
        params.put("min", "'a'");
        params.put("max", "'z'");
        params.put("hotValueCollectCount", "10");
        OlapAnalysisTask task = new OlapAnalysisTask();
        task.info = new AnalysisInfoBuilder().setColName("name").build();
        task.col = table.getColumn("name");
        task.addLengthAssertParam(params);
        params.put("limit", "");
        params.put("preAggHint", "");
        params.put("sampleHints", SampledScanSqlRenderer.render(new FixedBlockScanClause(blocks), 4096));
        ConnectContext ctx = new ConnectContext();
        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(ctx);
                MockedStatic<Env> env = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            env.when(Env::getCurrentInvertedIndex).thenReturn(inverted);
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setDatabase("stats_test");
            ctx.setCurrentUserIdentity(org.apache.doris.analysis.UserIdentity.ROOT);
            ctx.getState().setInternal(true);
            BaseAnalysisTask.configureStatsCte(ctx.getSessionVariable());
            Map<String, Boolean> queries = new LinkedHashMap<>();
            for (int topK : List.of(10, 0, -1)) {
                params.put("hotValueCollectCount", Integer.toString(Math.max(0, topK)));
                queries.put(TableStatisticsSqlBuilder.fixed(params, topK < 0), true);
            }
            params.put("sampleHints", "");
            params.put("dataSizeFunction", "SUM(LENGTH(`name`))");
            for (int topK : List.of(10, 0)) {
                params.put("hotValueCollectCount", Integer.toString(topK));
                queries.put(new StringSubstitutor(params).replace(TableStatisticsSqlBuilder.FULL_ANALYZE_TEMPLATE), false);
            }
            for (var query : queries.entrySet()) {
                String sql = query.getKey();
                var statement = MemoTestUtils.createStatementContext(ctx, sql);
                statement.setTables(Map.of(List.of("internal", "stats_test", "events"), table));
                var cascades = MemoTestUtils.createCascadesContext(statement, sql);
                var physical = PlanChecker.from(cascades).analyze().rewrite().optimize().getBestPlanTree();
                List<PhysicalOlapScan> scans = physical.collectToList(PhysicalOlapScan.class::isInstance);
                Assertions.assertEquals(1, scans.size(), physical.treeString());
                Assertions.assertEquals(11, scans.get(0).getSelectedIndexId());
                Assertions.assertTrue(physical.treeString().contains("assert_true"), physical.treeString());
                if (query.getValue()) {
                    Assertions.assertEquals(blocks, scans.get(0).getTableSample().orElseThrow().fixedBlockSpec);
                }
                PlanTranslatorContext translated = new PlanTranslatorContext();
                physical.accept(new PhysicalPlanTranslator(translated), translated);
                Assertions.assertEquals(1, translated.getScanNodes().size());
                Assertions.assertEquals(query.getValue(),
                        ((OlapScanNode) translated.getScanNodes().get(0)).hasFixedBlockSample());
            }
        }
    }
}
