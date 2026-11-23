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

package org.apache.doris.planner;

import org.apache.doris.analysis.FixedBlockSampleSpec;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletSampleOptions;
import org.apache.doris.thrift.TTabletSamplingAlgorithm;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class FixedBlockTableSamplePlanTest {
    private static final String GROUP = "GROUP (tablet_rate=0.25, unit_rate=8e-2, max_units=2048, TABLETS (101))";
    private static final String METHOD = "FIXED_BLOCK unit_rows=1024, " + GROUP;
    private static final String SAMPLE = "(" + METHOD + ") REPEATABLE 8101";
    private final QueryState state = new QueryState();
    private MockedStatic<ConnectContext> current;

    @BeforeEach
    void setUp() {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(ctx.getState()).thenReturn(state);
        Mockito.when(ctx.getStatementContext()).thenReturn(new StatementContext());
        state.setNereids(true);
        state.setInternal(true);
        current = Mockito.mockStatic(ConnectContext.class);
        current.when(ConnectContext::get).thenReturn(ctx);
    }

    @AfterEach
    void tearDown() {
        current.close();
    }

    private TableSample parse(String sample) {
        return parseRelation("SELECT 1 FROM tbl TABLESAMPLE " + sample).getTableSample().get();
    }

    private UnboundRelation parseRelation(String sql) {
        Plan plan = new NereidsParser().parseSingle(sql);
        while (!(plan instanceof UnboundRelation)) {
            plan = plan.child(0);
        }
        return (UnboundRelation) plan;
    }

    @Test
    void parseAndTranslate() {
        TableSample first = parse(SAMPLE);
        Assertions.assertEquals(first, parse(SAMPLE));
        Assertions.assertEquals(first, parse(SAMPLE.replace("1024", "1.024e3").replace("2048", "2048.0")));
        Assertions.assertEquals(first.hashCode(), parse(SAMPLE).hashCode());
        TableSample multi = parse("(" + METHOD + ", " + GROUP.replace("101", "107,108")
                .replace("8e-2", "1") + ") REPEATABLE 8101");
        Assertions.assertNotEquals(first, multi);
        FixedBlockSampleSpec spec = multi.translateToLegacyTableSample().getFixedBlockSpec();
        Assertions.assertEquals(Set.of(101L, 107L, 108L), spec.tabletIds());
        Assertions.assertEquals(0.08, spec.groupFor(101L).unitRate);
        Assertions.assertEquals(1.0, spec.groupFor(108L).unitRate);
        state.setInternal(false);
        Assertions.assertThrows(AnalysisException.class, () -> parse(SAMPLE));
        Assertions.assertEquals(new TableSample(10, true, -1), parse("(10 PERCENT)"));
        Assertions.assertEquals(new TableSample(25, false, 0), parse("(25 ROWS) REPEATABLE 0"));
    }

    @Test
    void invalidSpecs() {
        Assertions.assertThrows(AnalysisException.class,
                () -> new NereidsParser().parseSingle("SHOW DATA SKEW FROM tbl TABLESAMPLE " + SAMPLE));
        for (String invalid : List.of("()", "(" + METHOD + ")", SAMPLE.replace("8101", "-2"),
                SAMPLE.replace("1024", "0"), SAMPLE.replace("2048", "0"), SAMPLE.replace("8e-2", "1e500"),
                SAMPLE.replace("0.25", "-0.2"), SAMPLE.replace("8e-2", "0"), SAMPLE.replace("8e-2", "1.1"),
                SAMPLE.replace("FIXED_BLOCK", "FIXED_BLOK"), SAMPLE.replace("unit_rows", "rows_count"),
                SAMPLE.replace("unit_rate", "tablet_rate"), SAMPLE.replace("max_units=2048,", ""),
                SAMPLE.replace("TABLETS (101)", "TABLETS (101,101)"), "(" + METHOD + "," + GROUP + ") REPEATABLE 1")) {
            Assertions.assertThrows(AnalysisException.class, () -> parse(invalid), invalid);
        }
    }

    @Test
    void allOlapAggregateRuleActionsPreserveTheSample() {
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                Mockito.mock(OlapTable.class), List.of(), List.of(), List.of(), Optional.of(parse(SAMPLE)), List.of());
        LogicalFilter<?> filter = new LogicalFilter<>(Set.of(), scan);
        Alias constant = new Alias(new IntegerLiteral(1), "c");
        LogicalProject<?> project = new LogicalProject<>(List.of(constant), scan);
        LogicalProject<?> filterProject = new LogicalProject<>(List.of(constant), filter);
        Alias count = new Alias(new Count(), "n");
        LogicalAggregate<?> filtered = new LogicalAggregate<>(List.of(), List.of(count), filter);
        LogicalAggregate<?> projectedFilter = new LogicalAggregate<>(List.of(), List.of(count), filterProject);
        Map<RuleType, Plan> inputs = Map.of(
                RuleType.COUNT_ON_INDEX_WITHOUT_PROJECT, filtered,
                RuleType.COUNT_ON_INDEX, projectedFilter,
                RuleType.STORAGE_LAYER_AGGREGATE_MINMAX_ON_UNIQUE_WITHOUT_PROJECT, filtered,
                RuleType.STORAGE_LAYER_AGGREGATE_MINMAX_ON_UNIQUE, projectedFilter,
                RuleType.STORAGE_LAYER_WITH_PROJECT_NO_SLOT_REF, project,
                RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT, new LogicalAggregate<>(List.of(), List.of(count), scan),
                RuleType.STORAGE_LAYER_AGGREGATE_WITH_PROJECT, new LogicalAggregate<>(List.of(), List.of(count), project));
        int checked = 0;
        CascadesContext context = Mockito.mock(CascadesContext.class);
        for (Rule rule : new AggregateStrategies().buildRules()) {
            Plan input = inputs.get(rule.getRuleType());
            if (input != null) {
                Assertions.assertSame(input, rule.transform(input, context).get(0), rule.toString());
                checked++;
            }
        }
        Assertions.assertEquals(7, checked);
    }

    @Test
    void bindingChecksIndexAndPartitionNamespaceBeforePruning() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getId()).thenReturn(23L);
        Mockito.when(table.getType()).thenReturn(TableIf.TableType.OLAP);
        Mockito.when(table.isManagedTable()).thenReturn(true);
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);
        Mockito.when(table.getIndexIdByName("rollup")).thenReturn(99L);
        MaterializedIndexMeta rollup = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(table.getIndexMetaByIndexId(99L)).thenReturn(rollup);
        Mockito.when(table.getPartitionIds()).thenReturn(List.of(1L));
        Partition normal = Mockito.mock(Partition.class);
        Partition temp = Mockito.mock(Partition.class);
        Mockito.when(temp.getId()).thenReturn(2L);
        Mockito.when(table.getPartition(1L)).thenReturn(normal);
        Mockito.when(table.getPartition(2L)).thenReturn(temp);
        Mockito.when(table.getPartition("tp", true)).thenReturn(temp);
        Mockito.when(table.isTemporaryPartition(2L)).thenReturn(true);
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        Mockito.when(index.getTablet(101L)).thenReturn(Mockito.mock(Tablet.class));
        Mockito.when(table.getPartitionIndex(Mockito.any(Partition.class), Mockito.anyLong())).thenReturn(index);
        StatementContext statement = Mockito.mock(StatementContext.class);
        Mockito.when(statement.getAndCacheTable(Mockito.anyList(), Mockito.eq(StatementContext.TableFrom.QUERY),
                Mockito.any())).thenReturn(table);
        CascadesContext context = Mockito.mock(CascadesContext.class);
        Mockito.when(context.getStatementContext()).thenReturn(statement);
        ConnectContext ctx = ConnectContext.get();
        Mockito.when(context.getConnectContext()).thenReturn(ctx);
        TabletInvertedIndex inverted = Mockito.mock(TabletInvertedIndex.class);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            env.when(Env::getCurrentInvertedIndex).thenReturn(inverted);
            Mockito.when(inverted.getTabletMetaList(List.of(101L))).thenReturn(List.of(
                    new TabletMeta(1, 23, 1, 99, 0, TStorageMedium.HDD, false)));
            assertBindingFails(context, "", "index 10");
            bind(context, " INDEX rollup");
            Mockito.when(inverted.getTabletMetaList(List.of(101L))).thenReturn(List.of(
                    new TabletMeta(1, 23, 2, 10, 0, TStorageMedium.HDD, false)));
            assertBindingFails(context, "", "normal partition");
            bind(context, " TEMPORARY PARTITION(tp)");
            Mockito.when(inverted.getTabletMetaList(List.of(101L))).thenReturn(List.of(
                    new TabletMeta(1, 23, 1, 10, 0, TStorageMedium.HDD, false)));
            assertBindingFails(context, " TEMPORARY PARTITION(tp)", "temporary partition");
            bind(context, "");
            Mockito.when(inverted.getTabletMetaList(List.of(101L)))
                    .thenReturn(Collections.singletonList(null));
            assertBindingFails(context, "", "index 10");
            Mockito.when(inverted.getTabletMetaList(List.of(101L)))
                    .thenReturn(List.of(TabletInvertedIndex.NOT_EXIST_TABLET_META));
            assertBindingFails(context, "", "index 10");
        }
    }

    private void assertBindingFails(CascadesContext context, String scanClause, String reason) {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class, () -> bind(context, scanClause));
        Assertions.assertTrue(error.getMessage().contains("Stale FIXED_BLOCK"), error::getMessage);
        Assertions.assertTrue(error.getMessage().contains(reason), error::getMessage);
    }

    private void bind(CascadesContext context, String scanClause) {
        UnboundRelation relation = parseRelation("SELECT 1 FROM internal.db.tbl" + scanClause
                + " TABLESAMPLE " + SAMPLE + " WHERE 1 = 0");
        new BindRelation().build().transform(relation, context);
    }

    @Test
    void mixedVersionReplicasUseOnlyCapableBackends() throws Exception {
        Backend old = new Backend(7, "127.0.0.1", 9050);
        Backend upgraded = new Backend(8, "127.0.0.2", 9050);
        old.setAlive(true);
        BackendHbResponse hb = new BackendHbResponse(8, 9060, 8040, 8060, 1L, 1L, "test", "mix", 0, 0, false, 0);
        hb.setSupportsFixedBlockSamplingV1(true);
        upgraded.handleHbResponse(hb, false);
        org.apache.doris.analysis.TableSample sample = parse(SAMPLE).translateToLegacyTableSample();
        List<Backend> backends = List.of(old, upgraded);
        SessionVariable vars = ConnectContext.get().getSessionVariable();
        for (int fixed : List.of(-1, 0)) {
            vars.useFixReplica = fixed;
            for (boolean fallback : List.of(false, true)) {
                vars.fallbackOtherReplicaWhenFixedCorrupt = fallback;
                for (boolean cooldown : List.of(false, true)) {
                    vars.enableCooldownReplicaAffinity = cooldown;
                    OlapScanNode node = scan(backends, sample, List.of(1L), Set.of());
                    TScanRangeLocations range = node.getScanRangeLocations(0).get(0);
                    Assertions.assertEquals(1, range.getLocationsSize());
                    Assertions.assertEquals(upgraded.getId(), range.getLocations().get(0).getBackendId());
                    Assertions.assertEquals(List.of(range.getLocations().get(0).getServer()),
                            range.getScanRange().getPaloScanRange().getHosts());
                }
            }
        }
        hb.setSupportsFixedBlockSamplingV1(false);
        upgraded.handleHbResponse(hb, false);
        UserException error = Assertions.assertThrows(UserException.class, () -> scan(backends, sample,
                List.of(1L), Set.of()));
        for (Backend backend : backends) {
            Assertions.assertTrue(error.getMessage().contains("Backend " + backend.getId()
                    + " does not support FIXED_BLOCK sampling v1"), error::getMessage);
        }
    }

    @Test
    void selectedIndexPruningProtocolAndHeartbeatDowngrade() throws Exception {
        Backend backend = new Backend(7L, "127.0.0.1", 9050);
        backend.setAlive(true);
        org.apache.doris.analysis.TableSample sample = parse(SAMPLE).translateToLegacyTableSample();
        UserException unsupported = Assertions.assertThrows(UserException.class,
                () -> scan(backend, sample, List.of(1L, 2L), Set.of()));
        Assertions.assertTrue(unsupported.getMessage().contains("does not support FIXED_BLOCK"));
        BackendHbResponse hb = new BackendHbResponse(7, 9060, 8040, 8060, 1L, 1L, "test", "mix", 0, 0, false, 0);
        hb.setSupportsFixedBlockSamplingV1(true);
        backend.handleHbResponse(hb, false);
        OlapScanNode hit = scan(backend, sample, List.of(1L, 2L), Set.of());
        Assertions.assertTrue(hit.getNodeExplainString("", TExplainLevel.BRIEF)
                .contains("TABLET SAMPLE: FIXED_BLOCK("));
        Assertions.assertEquals(1, hit.getScanRangeLocations(0).size());
        TPaloScanRange range = hit.getScanRangeLocations(0).get(0).getScanRange().getPaloScanRange();
        Assertions.assertEquals(101L, range.getTabletId());
        TTabletSampleOptions opts = range.getSampleOptions();
        Assertions.assertEquals(1, opts.getProtocolVersion());
        Assertions.assertEquals(TTabletSamplingAlgorithm.FIXED_ROWID_BLOCK, opts.getAlgorithm());
        Assertions.assertEquals(8101L, opts.getSeed());
        Assertions.assertEquals(0.25, opts.getTabletInclusionProbability());
        Assertions.assertEquals(0.08, opts.getIntraTabletUnitProbability());
        Assertions.assertEquals(1024, opts.getFixedRowidBlockParams().getSampleUnitRows());
        Assertions.assertEquals(2048, opts.getFixedRowidBlockParams().getMaxSampleUnitsPerTablet());

        Assertions.assertTrue(scan(backend, sample, List.of(2L), Set.of()).getScanRangeLocations(0).isEmpty());
        Assertions.assertTrue(scan(backend, sample, List.of(1L, 2L), Set.of(202L)).getScanRangeLocations(0).isEmpty());
        UserException stale = Assertions.assertThrows(UserException.class, () -> scan(backend,
                parse(SAMPLE.replace("101", "505")).translateToLegacyTableSample(), List.of(2L), Set.of()));
        Assertions.assertTrue(stale.getMessage().contains("Stale FIXED_BLOCK"));
        backend.handleHbResponse(new BackendHbResponse(7, 9060, 8040, 8060, 2L, 2L, "old", "mix", 0, 0, false, 0),
                false);
        Assertions.assertFalse(backend.isSupportsFixedBlockSamplingV1());
    }

    private OlapScanNode scan(Backend backend, org.apache.doris.analysis.TableSample sample,
            List<Long> partitions, Set<Long> prunedTablets) throws UserException {
        return scan(List.of(backend), sample, partitions, prunedTablets);
    }

    private OlapScanNode scan(List<Backend> backends, org.apache.doris.analysis.TableSample sample,
            List<Long> partitions, Set<Long> prunedTablets) throws UserException {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);
        ImmutableMap.Builder<Long, Backend> byId = ImmutableMap.builder();
        List<Replica> replicas = new ArrayList<>();
        for (Backend backend : backends) {
            byId.put(backend.getId(), backend);
            replicas.add(new LocalReplica(backend.getId(), backend.getId(), Replica.ReplicaState.NORMAL, 10L, 0));
        }
        Mockito.when(table.getAllBackendsByAllCluster()).thenReturn(byId.build());
        List<Partition> all = new ArrayList<>();
        for (long id : List.of(1L, 2L)) {
            Partition part = Mockito.mock(Partition.class);
            Mockito.when(part.getId()).thenReturn(id);
            Mockito.when(part.getVisibleVersion()).thenReturn(10L);
            DistributionInfo buckets = Mockito.mock(DistributionInfo.class);
            Mockito.when(buckets.getBucketNum()).thenReturn(1);
            Mockito.when(part.getDistributionInfo()).thenReturn(buckets);
            Tablet tablet = Mockito.mock(Tablet.class);
            Mockito.when(tablet.getId()).thenReturn(id * 101);
            Mockito.when(tablet.getCooldownReplicaId()).thenReturn(replicas.get(0).getId());
            Mockito.when(tablet.getQueryableReplicas(Mockito.eq(10L), Mockito.anyMap(), Mockito.eq(false)))
                    .thenAnswer(call -> new ArrayList<>(replicas));
            MaterializedIndex rollup = Mockito.mock(MaterializedIndex.class);
            Mockito.when(rollup.getTablets()).thenReturn(List.of(tablet));
            Mockito.when(rollup.getTablet(id * 101)).thenReturn(tablet);
            Mockito.when(table.getPartition(id)).thenReturn(part);
            Mockito.when(table.getPartitionIndex(part, 99L)).thenReturn(rollup);
            all.add(part);
        }
        Mockito.when(table.getAllPartitions()).thenReturn(all);
        TupleDescriptor desc = Mockito.mock(TupleDescriptor.class);
        Mockito.when(desc.getId()).thenReturn(new TupleId(1));
        Mockito.when(desc.getTable()).thenReturn(table);
        OlapScanNode scan = new OlapScanNode(new PlanNodeId(1), desc, "sampled", ScanContext.EMPTY);
        scan.setTableSample(sample);
        scan.setSelectedIndexInfo(99L, false, "");
        scan.setSelectedPartitionIds(partitions);
        scan.setNereidsPrunedTabletIds(prunedTablets);
        TabletInvertedIndex inverted = Mockito.mock(TabletInvertedIndex.class);
        Mockito.when(inverted.getTabletMetaList(Mockito.anyList())).thenAnswer(call -> {
            List<TabletMeta> metas = new ArrayList<>();
            for (long id : call.<List<Long>>getArgument(0)) {
                metas.add(id == 101 || id == 202
                        ? new TabletMeta(1, table.getId(), id / 101, 99, 0, TStorageMedium.HDD, false)
                        : TabletInvertedIndex.NOT_EXIST_TABLET_META);
            }
            return metas;
        });
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            env.when(Env::getCurrentInvertedIndex).thenReturn(inverted);
            scan.createScanRangeLocations();
        }
        return scan;
    }
}
