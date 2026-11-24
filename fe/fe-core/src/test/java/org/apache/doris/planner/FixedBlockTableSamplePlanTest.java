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
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.Type;
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
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletSampleOptions;
import org.apache.doris.thrift.TTabletSamplingAlgorithm;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
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
import java.util.concurrent.CompletableFuture;

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
        TableSample keyRange = parse(SAMPLE.replace("FIXED_BLOCK", "KEY_RANGE_BLOCK"));
        Assertions.assertNotEquals(first, keyRange);
        var rangeOpts = keyRange.fixedBlockSpec.toThrift(101);
        Assertions.assertEquals(TTabletSamplingAlgorithm.KEY_RANGE_BLOCK, rangeOpts.getAlgorithm());
        Assertions.assertFalse(rangeOpts.isSetFixedRowidBlockParams());
        Assertions.assertEquals(1024, rangeOpts.getKeyRangeBlockParams().getSampleUnitRows());
        Assertions.assertEquals(2048, rangeOpts.getKeyRangeBlockParams().getMaxSampleUnitsPerTablet());
        state.setInternal(false);
        Assertions.assertThrows(AnalysisException.class, () -> parse(SAMPLE.replace("FIXED_BLOCK", "KEY_RANGE_BLOCK")));
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
            var key = new Column("k", Type.INT, true, null, "0", "");
            var schema = List.of(key, new Column(Column.DELETE_SIGN, Type.TINYINT),
                    new Column(Column.TTL_COL, Type.BIGINT));
            var base = Mockito.mock(MaterializedIndexMeta.class);
            Mockito.when(base.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
            Mockito.when(base.getSchema()).thenReturn(schema);
            Mockito.when(table.getIndexMetaByIndexId(10L)).thenReturn(base);
            Mockito.when(table.getDefaultDistributionInfo()).thenReturn(new HashDistributionInfo(1, List.of(key)));
            Mockito.when(table.getBaseSchema(true)).thenReturn(schema);
            Mockito.when(table.hasDeleteSign()).thenReturn(true);
            Mockito.when(table.hasRowTtl()).thenReturn(true);
            Mockito.when(table.isMorTable()).thenReturn(true);
            var relation = parseRelation("SELECT 1 FROM internal.db.tbl TABLESAMPLE "
                    + SAMPLE.replace("FIXED_BLOCK", "KEY_RANGE_BLOCK"));
            var binding = new BindRelation().build();
            Plan visible = binding.transform(relation, context).get(0);
            var ttl = Assertions.assertInstanceOf(LogicalFilter.class, visible);
            Assertions.assertTrue(ttl.getConjuncts().toString().contains(Column.TTL_COL));
            var delete = Assertions.assertInstanceOf(LogicalFilter.class, ttl.child());
            Assertions.assertTrue(delete.getConjuncts().toString().contains(Column.DELETE_SIGN));
            Assertions.assertTrue(((LogicalOlapScan) delete.child()).getPreAggStatus().isOff());
            Mockito.when(statement.isHintForcePreAggOn()).thenReturn(true);
            var conflict = Assertions.assertThrows(AnalysisException.class, () -> binding.transform(relation, context));
            Assertions.assertTrue(conflict.getMessage().contains("PREAGGOPEN"));
            Mockito.when(statement.isHintForcePreAggOn()).thenReturn(false);
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
        var keyRange = parse(SAMPLE.replace("FIXED_BLOCK", "KEY_RANGE_BLOCK")).translateToLegacyTableSample();
        Assertions.assertThrows(UserException.class, () -> scan(backend, keyRange, List.of(1L), Set.of()));
        hb.setSupportsKeyRangeBlockSamplingV1(true);
        backend.handleHbResponse(hb, false);
        Assertions.assertEquals(TTabletSamplingAlgorithm.KEY_RANGE_BLOCK, scan(backend, keyRange, List.of(1L), Set.of())
                .getScanRangeLocations(0).get(0).getScanRange().getPaloScanRange().getSampleOptions().getAlgorithm());
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
        Assertions.assertFalse(backend.isSupportsKeyRangeBlockSamplingV1());
        Assertions.assertThrows(UserException.class, () -> scan(backend, keyRange, List.of(1L), Set.of()));
    }

    private OlapScanNode scan(Backend backend, org.apache.doris.analysis.TableSample sample,
            List<Long> partitions, Set<Long> prunedTablets) throws UserException {
        return scan(List.of(backend), sample, partitions, prunedTablets);
    }

    private OlapScanNode scan(List<Backend> backends, org.apache.doris.analysis.TableSample sample,
            List<Long> partitions, Set<Long> prunedTablets) throws UserException {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);
        var key = new Column("k", Type.INT, true, null, "0", "");
        var distribution = new HashDistributionInfo(1, List.of(key));
        var metadata = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(metadata.getSchema()).thenReturn(List.of(key));
        Mockito.when(metadata.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        Mockito.when(table.getIndexMetaByIndexId(99L)).thenReturn(metadata);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(distribution);
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

    @Test
    void randomAggKeyRangeCoordinatesSharedIntervalsAndReplicaFailover() throws Exception {
        Backend be1 = new Backend(11L, "127.0.0.1", 9050);
        Backend be2 = new Backend(12L, "127.0.0.2", 9050);
        BackendHbResponse hb1 = new BackendHbResponse(11, 9060, 8040, 8061, 1L, 1L, "v", "mix", 0, 0, false, 0);
        hb1.setSupportsFixedBlockSamplingV1(true);
        hb1.setSupportsKeyRangeBlockSamplingV1(true);
        be1.handleHbResponse(hb1, false);
        BackendHbResponse hb2 = new BackendHbResponse(12, 9060, 8040, 8062, 1L, 1L, "v", "mix", 0, 0, false, 0);
        hb2.setSupportsFixedBlockSamplingV1(true);
        hb2.setSupportsKeyRangeBlockSamplingV1(true);
        be2.handleHbResponse(hb2, false);

        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(77L);
        Mockito.when(table.getName()).thenReturn("rand_agg");
        Mockito.when(table.getBaseIndexId()).thenReturn(99L);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        var key = new Column("k", Type.INT, true, null, "0", "");
        var val = new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", "");
        var randomDist = new RandomDistributionInfo(2);
        var metadata = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(metadata.getSchema()).thenReturn(List.of(key, val));
        Mockito.when(metadata.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        Mockito.when(table.getIndexMetaByIndexId(99L)).thenReturn(metadata);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(randomDist);
        Mockito.when(table.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of(11L, be1, 12L, be2));

        Partition part = Mockito.mock(Partition.class);
        Mockito.when(part.getId()).thenReturn(500L);
        Mockito.when(part.getName()).thenReturn("p0");
        Mockito.when(part.getVisibleVersion()).thenReturn(15L);
        Mockito.when(part.getDistributionInfo()).thenReturn(randomDist);
        Mockito.doReturn(List.of(part)).when(table).getPartitions();
        Mockito.when(table.getAllPartitions()).thenReturn(List.of(part));
        Mockito.when(table.getPartition(500L)).thenReturn(part);

        LocalReplica r101a = new LocalReplica(1L, 11L, Replica.ReplicaState.NORMAL, 15L, 0);
        r101a.setRowCount(100L);
        LocalReplica r101b = new LocalReplica(4L, 12L, Replica.ReplicaState.NORMAL, 15L, 0);
        r101b.setRowCount(100L);
        LocalReplica r102a = new LocalReplica(2L, 11L, Replica.ReplicaState.NORMAL, 15L, 0);
        r102a.setRowCount(800L);
        LocalReplica r102b = new LocalReplica(3L, 12L, Replica.ReplicaState.NORMAL, 15L, 0);
        r102b.setRowCount(800L);

        Tablet t101 = Mockito.mock(Tablet.class);
        Mockito.when(t101.getId()).thenReturn(101L);
        Mockito.when(t101.getReplicas()).thenReturn(List.of(r101a, r101b));
        Mockito.when(t101.getQueryableReplicas(Mockito.eq(15L), Mockito.anyMap(), Mockito.eq(false)))
                .thenAnswer(call -> new ArrayList<>(List.of(r101a, r101b)));

        Tablet t102 = Mockito.mock(Tablet.class);
        Mockito.when(t102.getId()).thenReturn(102L);
        Mockito.when(t102.getReplicas()).thenReturn(List.of(r102a, r102b));
        Mockito.when(t102.getQueryableReplicas(Mockito.eq(15L), Mockito.anyMap(), Mockito.eq(false)))
                .thenAnswer(call -> new ArrayList<>(List.of(r102a, r102b)));

        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        Mockito.when(index.getTablets()).thenReturn(List.of(t101, t102));
        Mockito.when(index.getTablet(101L)).thenReturn(t101);
        Mockito.when(index.getTablet(102L)).thenReturn(t102);
        Mockito.when(table.getPartitionIndex(part, 99L)).thenReturn(index);

        TabletInvertedIndex inverted = Mockito.mock(TabletInvertedIndex.class);
        Mockito.when(inverted.getTabletMetaList(Mockito.anyList())).thenAnswer(call -> {
            List<TabletMeta> metas = new ArrayList<>();
            for (long id : call.<List<Long>>getArgument(0)) {
                metas.add(id == 101L || id == 102L
                        ? new TabletMeta(1, 77L, 500L, 99L, 0, TStorageMedium.HDD, false)
                        : TabletInvertedIndex.NOT_EXIST_TABLET_META);
            }
            return metas;
        });

        TupleDescriptor desc = Mockito.mock(TupleDescriptor.class);
        Mockito.when(desc.getId()).thenReturn(new TupleId(1));
        Mockito.when(desc.getTable()).thenReturn(table);

        // Incomplete RANDOM partition tablet list must be rejected.
        var partialSample = parse("(KEY_RANGE_BLOCK unit_rows=256, GROUP (tablet_rate=1.0, unit_rate=0.1, "
                + "max_units=64, TABLETS (101))) REPEATABLE 99").translateToLegacyTableSample();
        OlapScanNode partialScan = new OlapScanNode(new PlanNodeId(1), desc, "s", ScanContext.EMPTY);
        partialScan.setTableSample(partialSample);
        partialScan.setSelectedIndexInfo(99L, false, "");
        partialScan.setSelectedPartitionIds(List.of(500L));

        var fullSample = parse("(KEY_RANGE_BLOCK unit_rows=256, GROUP (tablet_rate=1.0, unit_rate=0.1, "
                + "max_units=64, TABLETS (101, 102))) REPEATABLE 99").translateToLegacyTableSample();

        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<BackendServiceProxy> proxyStatic = Mockito.mockStatic(BackendServiceProxy.class)) {
            env.when(Env::getCurrentInvertedIndex).thenReturn(inverted);
            UserException partialErr = Assertions.assertThrows(UserException.class,
                    partialScan::createScanRangeLocations);
            Assertions.assertTrue(partialErr.getMessage().contains("requires all partition tablets"),
                    partialErr::getMessage);

            // Backends lacking supportsSharedKeyRangeBlockSamplingV1 must be rejected for RANDOM KEY_RANGE_BLOCK.
            OlapScanNode unreadyScan = new OlapScanNode(new PlanNodeId(2), desc, "s", ScanContext.EMPTY);
            unreadyScan.setTableSample(fullSample);
            unreadyScan.setSelectedIndexInfo(99L, false, "");
            unreadyScan.setSelectedPartitionIds(List.of(500L));
            Assertions.assertThrows(UserException.class, unreadyScan::createScanRangeLocations);

            hb1.setSupportsSharedKeyRangeBlockSamplingV1(true);
            hb2.setSupportsSharedKeyRangeBlockSamplingV1(true);
            be1.handleHbResponse(hb1, false);
            be2.handleHbResponse(hb2, false);

            // Default useFixReplica = -1 keeps both replicas [be1, be2] on tablet 102:
            // be1 fails with RpcException, be2 succeeds and returns 1 shared interval.
            BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
            proxyStatic.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Mockito.when(proxy.sampleTabletKeyRanges(
                    Mockito.eq(new TNetworkAddress("127.0.0.1", 8061)), Mockito.any()))
                    .thenThrow(new RpcException("127.0.0.1", "connection refused"));
            var response = InternalService.PSampleTabletKeyRangesResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()))
                    .setPopulationUnits(20L)
                    .setSelectedUnits(2L)
                    .addIntervals(InternalService.PKeyRangeInterval.newBuilder()
                            .setLower(InternalService.PKeyRangeBound.newBuilder()
                                    .setPrefix(ByteString.copyFromUtf8("k_010"))
                                    .addTuple(ByteString.copyFromUtf8("\u0002k_010")))
                            .setUpper(InternalService.PKeyRangeBound.newBuilder()
                                    .setPrefix(ByteString.copyFromUtf8("k_030"))))
                    .build();
            var reqCaptor = org.mockito.ArgumentCaptor.forClass(InternalService.PSampleTabletKeyRangesRequest.class);
            Mockito.when(proxy.sampleTabletKeyRanges(
                    Mockito.eq(new TNetworkAddress("127.0.0.2", 8062)), reqCaptor.capture()))
                    .thenReturn(CompletableFuture.completedFuture(response));

            OlapScanNode okScan = new OlapScanNode(new PlanNodeId(3), desc, "s", ScanContext.EMPTY);
            okScan.setTableSample(fullSample);
            okScan.setSelectedIndexInfo(99L, false, "");
            okScan.setSelectedPartitionIds(List.of(500L));
            okScan.createScanRangeLocations();

            // Single-BE coverage sends all partition tablets [101, 102] with domain_id = partitionId (500).
            Assertions.assertEquals(List.of(101L, 102L), reqCaptor.getValue().getTabletIdsList());
            Assertions.assertFalse(reqCaptor.getValue().getReturnBoundaryCuts());
            Assertions.assertEquals(500L, reqCaptor.getValue().getDomainId());
            Assertions.assertEquals(15L, reqCaptor.getValue().getVersion());
            Assertions.assertEquals(2, okScan.getScanRangeLocations(0).size());
            for (TScanRangeLocations loc : okScan.getScanRangeLocations(0)) {
                var kr = loc.getScanRange().getPaloScanRange().getSampleOptions().getKeyRangeBlockParams();
                Assertions.assertEquals(20L, kr.getSharedPopulationUnits());
                Assertions.assertEquals(2L, kr.getSharedSelectedUnits());
                Assertions.assertEquals(1, kr.getSharedIntervalsSize());
                Assertions.assertEquals("k_010", ByteString.copyFrom(
                        kr.getSharedIntervals().get(0).getLower().bufferForPrefix()).toStringUtf8());
                Assertions.assertEquals("k_030", ByteString.copyFrom(
                        kr.getSharedIntervals().get(0).getUpper().bufferForPrefix()).toStringUtf8());
            }

            // Multi-BE layout (t101 only on be1, t102 only on be2): merges weighted boundary cuts across BEs.
            Mockito.when(t101.getReplicas()).thenReturn(List.of(r101a));
            Mockito.when(t101.getQueryableReplicas(Mockito.eq(15L), Mockito.anyMap(), Mockito.eq(false)))
                    .thenAnswer(call -> new ArrayList<>(List.of(r101a)));
            Mockito.when(t102.getReplicas()).thenReturn(List.of(r102b));
            Mockito.when(t102.getQueryableReplicas(Mockito.eq(15L), Mockito.anyMap(), Mockito.eq(false)))
                    .thenAnswer(call -> new ArrayList<>(List.of(r102b)));
            Mockito.reset(proxy);
            var slice1 = InternalService.PSampleTabletKeyRangesResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()))
                    .setTotalRows(512L)
                    .addBoundaryCuts(InternalService.PKeyRangeWeightedCut.newBuilder()
                            .setBound(InternalService.PKeyRangeBound.newBuilder()
                                    .setPrefix(ByteString.copyFromUtf8("k_0000")))
                            .setWeight(256L))
                    .addBoundaryCuts(InternalService.PKeyRangeWeightedCut.newBuilder()
                            .setBound(InternalService.PKeyRangeBound.newBuilder()
                                    .setPrefix(ByteString.copyFromUtf8("k_0256")))
                            .setWeight(256L))
                    .build();
            var slice2 = InternalService.PSampleTabletKeyRangesResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()))
                    .setTotalRows(512L)
                    .addBoundaryCuts(InternalService.PKeyRangeWeightedCut.newBuilder()
                            .setBound(InternalService.PKeyRangeBound.newBuilder()
                                    .setPrefix(ByteString.copyFromUtf8("k_0512")))
                            .setWeight(256L))
                    .addBoundaryCuts(InternalService.PKeyRangeWeightedCut.newBuilder()
                            .setBound(InternalService.PKeyRangeBound.newBuilder()
                                    .setPrefix(ByteString.copyFromUtf8("k_0768")))
                            .setWeight(256L))
                    .build();
            Mockito.when(proxy.sampleTabletKeyRanges(
                    Mockito.eq(new TNetworkAddress("127.0.0.1", 8061)), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(slice1));
            Mockito.when(proxy.sampleTabletKeyRanges(
                    Mockito.eq(new TNetworkAddress("127.0.0.2", 8062)), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(slice2));

            var halfSample = parse("(KEY_RANGE_BLOCK unit_rows=256, GROUP (tablet_rate=1.0, unit_rate=0.5, "
                    + "max_units=64, TABLETS (101, 102))) REPEATABLE 99").translateToLegacyTableSample();
            OlapScanNode multiBeScan = new OlapScanNode(new PlanNodeId(4), desc, "s", ScanContext.EMPTY);
            multiBeScan.setTableSample(halfSample);
            multiBeScan.setSelectedIndexInfo(99L, false, "");
            multiBeScan.setSelectedPartitionIds(List.of(500L));
            multiBeScan.createScanRangeLocations();
            for (TScanRangeLocations loc : multiBeScan.getScanRangeLocations(0)) {
                var kr = loc.getScanRange().getPaloScanRange().getSampleOptions().getKeyRangeBlockParams();
                Assertions.assertEquals(4L, kr.getSharedPopulationUnits());
                Assertions.assertEquals(2L, kr.getSharedSelectedUnits());
                Assertions.assertTrue(kr.getSharedIntervalsSize() >= 1);
            }
        }
    }
}
