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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonMvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonSnapshotCacheValue;
import org.apache.doris.datasource.paimon.source.PaimonScanNode;
import org.apache.doris.datasource.paimon.source.PaimonSplit;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.processor.post.materialize.MaterializeProbeVisitor;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.EliminateNotNull;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.table.VectorSearch;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TVectorMetric;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.BatchVectorSearchBuilder;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PaimonVectorSearchTableValuedFunctionTest {
    private static final class Fixture implements AutoCloseable {
        private final ConnectContext previous = ConnectContext.get();
        private final MockedStatic<Env> lookup;
        private final FileStoreTable table = Mockito.mock(FileStoreTable.class);
        private final PaimonExternalTable external = Mockito.mock(PaimonExternalTable.class);

        private Fixture(String metric) throws Exception {
            this(metric, true);
        }

        private Fixture(String metric, boolean nullableId) throws Exception {
            MemoTestUtils.createConnectContext();
            Env realEnv = Env.getCurrentEnv();
            Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(env.getFunctionRegistry()).thenReturn(realEnv.getFunctionRegistry());
            CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
            Mockito.when(env.getAccessManager().checkTblPriv(Mockito.any(ConnectContext.class),
                    Mockito.any(TableName.class), Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);
            PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(catalogs.getCatalog("paimon_catalog")).thenReturn(catalog);
            org.apache.doris.datasource.ExternalDatabase<?> database = catalog.getDbOrAnalysisException("db");
            Mockito.doReturn(external).when(database).getTableOrAnalysisException("items");
            Mockito.when(external.getCatalog()).thenReturn(catalog);
            Mockito.doReturn(database).when(external).getDatabase();
            Mockito.when(database.getCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getName()).thenReturn("paimon_catalog");
            Mockito.when(database.getFullName()).thenReturn("db");
            Mockito.when(external.getName()).thenReturn("items");
            Mockito.when(external.getPaimonTable(
                    Mockito.<Optional<org.apache.doris.datasource.mvcc.MvccSnapshot>>any()))
                    .thenReturn(table);
            Map<String, String> options = new HashMap<>(ImmutableMap.of("pk-vector.index.columns", "embedding",
                    "fields.embedding.pk-vector.distance.metric", metric,
                    "fields.embedding.pk-vector.index.type", "ivf-rq"));
            options.put("deletion-vectors.enabled", "true");
            options.put("deletion-vectors.merge-on-read", "false");
            List<DataField> fields = Arrays.asList(new DataField(0, "id", new IntType(nullableId)),
                    new DataField(1, "embedding", new VectorType(2, new FloatType())));
            Mockito.when(table.rowType()).thenReturn(new RowType(fields));
            Mockito.when(table.options()).thenReturn(options);
            Mockito.when(table.partitionKeys()).thenReturn(Collections.emptyList());
            Mockito.when(table.schema()).thenReturn(new TableSchema(1, fields, 1,
                    Collections.emptyList(), Collections.singletonList("id"), options, null));
            Mockito.when(table.copy(Mockito.anyMap())).thenReturn(table);
            Mockito.when(table.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(table);
            Mockito.when(external.loadSnapshot(Mockito.any(), Mockito.any())).thenReturn(
                    new PaimonMvccSnapshot(new PaimonSnapshotCacheValue(null,
                            new PaimonSnapshot(7L, 1L, table))));
            lookup = Mockito.mockStatic(Env.class);
            lookup.when(Env::getCurrentEnv).thenReturn(env);
            lookup.when(Env::getCurrentSystemInfo).thenReturn(realEnv.getClusterInfo());
        }

        private PaimonVectorSearchTableValuedFunction function(Map<String, String> overrides) {
            Map<String, String> properties = new HashMap<>(ImmutableMap.of(
                    "table", "paimon_catalog.db.items", "column", "embedding", "query_vector", "[1,2]"));
            properties.putAll(overrides);
            return Assertions.assertInstanceOf(PaimonVectorSearchTableValuedFunction.class,
                    new VectorSearch(new Properties(properties)).getCatalogFunction());
        }

        @Override
        public void close() {
            lookup.close();
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testPaimonCatalogDispatchAndOutputSchema() throws Exception {
        try (Fixture fixture = new Fixture("l2")) {
            PaimonVectorSearchTableValuedFunction function = fixture.function(Collections.emptyMap());
            Assertions.assertEquals(Arrays.asList("id", "embedding", "_distance"),
                    function.getTableColumns().stream().map(column -> column.getName())
                            .collect(java.util.stream.Collectors.toList()));
            TExternalSearchRequest request = function.getSearchRequest();
            Assertions.assertTrue(request.getPaimonOptions().isTvfDistance());
            Assertions.assertFalse(request.isSetVectorSearchOptions());
            Assertions.assertEquals(TVectorMetric.L2, request.getSearchQuery().getVectorSearch().getMetric());
            Assertions.assertEquals(1.0f, ByteBuffer.wrap(request.getSearchQuery().getVectorSearch()
                    .getQueryVector().getValues()).order(ByteOrder.LITTLE_ENDIAN).getFloat());
        }
    }

    @Test
    public void testSearchOptionsAndDotMetric() throws Exception {
        try (Fixture fixture = new Fixture("inner_product")) {
            PaimonVectorSearchTableValuedFunction function = fixture.function(ImmutableMap.of(
                    "metric", "dot", "nprobes", "4", "refine_factor", "8", "use_index", "false"));
            TExternalSearchRequest request = function.getSearchRequest();
            Assertions.assertEquals(TVectorMetric.DOT_PRODUCT, request.getSearchQuery().getVectorSearch().getMetric());
            Assertions.assertEquals(ImmutableMap.of("ivf.nprobe", "4", "refine_factor", "8"),
                    request.getPaimonOptions().getOptions());
            Assertions.assertFalse(request.getVectorSearchOptions().isUseIndex());
            request.getPaimonOptions().setTvfDistance(false);
            Assertions.assertTrue(function.getSearchRequest().getPaimonOptions().isTvfDistance());
        }
    }

    @Test
    public void testFlatSearchTransportsAllFilesWithoutIndexSources() throws Exception {
        for (boolean useIndex : new boolean[] {true, false}) {
            try (Fixture fixture = new Fixture("l2")) {
                fixture.table.options().put("vector-index.search-mode", "fast");
                Map<String, String> fullOptions = new HashMap<>(fixture.table.options());
                fullOptions.put("vector-index.search-mode", "full");
                FileStoreTable fullTable = Mockito.mock(FileStoreTable.class);
                Mockito.when(fullTable.options()).thenReturn(fullOptions);
                Mockito.doReturn(fixture.table.rowType()).when(fullTable).rowType();
                Mockito.when(fullTable.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(fullTable);
                Mockito.when(fixture.table.copyWithoutTimeTravel(
                        Collections.singletonMap("vector-index.search-mode", "full"))).thenReturn(fullTable);

                DataSplit data = DataSplit.builder().withSnapshot(7L).withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0).withBucketPath("file:///bucket-0")
                        .withDataFiles(Arrays.asList(dataFile("indexed.parquet"), dataFile("unindexed.parquet")))
                        .build();
                byte[] sourceMeta = new PrimaryKeyIndexSourceMeta(1,
                        new PrimaryKeyIndexSourceFile("indexed.parquet", 1L)).serialize();
                IndexFileMeta index = new IndexFileMeta("ivf-rq", "missing.index", 128L, 1L,
                        new GlobalIndexMeta(0, 0, 1, null, null, sourceMeta), null);
                Map<String, List<Range>> ranges = ImmutableMap.of(
                        "indexed.parquet", Collections.singletonList(new Range(0, 0)),
                        "unindexed.parquet", Collections.singletonList(new Range(0, 0)));
                BucketVectorSearchSplit input = new BucketVectorSearchSplit(data,
                        Collections.singletonList(index), ranges);
                BatchVectorSearchBuilder builder = Mockito.mock(
                        BatchVectorSearchBuilder.class, Mockito.RETURNS_SELF);
                VectorScan scan = Mockito.mock(VectorScan.class);
                VectorScan.Plan plan = Mockito.mock(VectorScan.Plan.class);
                Mockito.when(fixture.table.newBatchVectorSearchBuilder()).thenReturn(builder);
                Mockito.when(fullTable.newBatchVectorSearchBuilder()).thenReturn(builder);
                Mockito.when(builder.newVectorScan()).thenReturn(scan);
                Mockito.when(scan.scan()).thenReturn(plan);
                Mockito.when(plan.splits()).thenReturn(Collections.singletonList(input));

                PaimonScanNode node = createScanNode(fixture.function(
                        ImmutableMap.of("use_index", Boolean.toString(useIndex))));
                FileStoreTable processed = Deencapsulation.invoke(node, "getProcessedTable");
                Assertions.assertEquals(useIndex ? "fast" : "full",
                        processed.options().get("vector-index.search-mode"));
                PaimonSplit split = (PaimonSplit) node.getVectorSearchSplits().get(0);
                BucketVectorSearchSplit transported = BucketVectorSearchSplit.deserialize(
                        new DataInputDeserializer(split.getVectorPayloadBytes()));
                Assertions.assertEquals(data, transported.dataSplit());
                Assertions.assertEquals(ranges, transported.rowRangesByFile());
                Assertions.assertEquals(useIndex ? Collections.singletonList(index) : Collections.emptyList(),
                        transported.payloadFiles());
                Assertions.assertFalse(input.payloadFiles().isEmpty(), "the cached input must retain its index");
                Assertions.assertEquals("fast", fixture.table.options().get("vector-index.search-mode"));
                Assertions.assertEquals(useIndex, searchRequest(node).getVectorSearchOptions().isUseIndex());
            }
        }
    }

    private static DataFileMeta dataFile(String name) {
        return DataFileMeta.forAppend(name, 1024L, 1L, SimpleStats.EMPTY_STATS, 1L, 1L, 1L,
                Collections.emptyList(), null, FileSource.APPEND, Collections.emptyList(), null, null,
                Collections.emptyList());
    }

    @Test
    public void testRejectUnsupportedMetricDimensionAndOptions() throws Exception {
        try (Fixture fixture = new Fixture("l2")) {
            for (Map<String, String> properties : Arrays.asList(
                    ImmutableMap.of("metric", "cosine"), ImmutableMap.of("metric", "dot"),
                    ImmutableMap.of("query_vector", "[1]"), ImmutableMap.of("query_vector", "[1,null]"),
                    ImmutableMap.of("query_vector", "[1,1e100]"), ImmutableMap.of("top_k", "0"),
                    ImmutableMap.of("offset", "-1"), ImmutableMap.of("nprobes", "0"),
                    ImmutableMap.of("ef", "8"), ImmutableMap.of("top_k", "100000", "offset", "1"),
                    ImmutableMap.of("refine_factor", "2147483647"))) {
                Assertions.assertThrows(RuntimeException.class, () -> fixture.function(properties),
                        properties.toString());
            }
        }
    }

    @Test
    public void testAnalyzedTvfRetainsGlobalBoundsAndFilter() throws Exception {
        try (Fixture fixture = new Fixture("l2")) {
            LogicalPlan plan = new NereidsParser().parseSingle(
                    "select id, _distance from vector_search('table'='paimon_catalog.db.items',"
                            + "'column'='embedding','query_vector'='[1,2]','top_k'='3','offset'='2',"
                            + "'filter'='id > 1','prefilter'='true')");
            CascadesContext cascades = CascadesContext.initContext(
                    MemoTestUtils.createStatementContext(ConnectContext.get(), ""), plan, PhysicalProperties.ANY);
            cascades.newAnalyzer().analyze();
            List<LogicalTopN> bounds = cascades.getRewritePlan().collectToList(LogicalTopN.class::isInstance);
            Assertions.assertEquals(1, bounds.size());
            Assertions.assertEquals(3, bounds.get(0).getLimit());
            Assertions.assertEquals(2, bounds.get(0).getOffset());
            LogicalTVFRelation relation = cascades.getRewritePlan().<LogicalTVFRelation>collectToList(
                    LogicalTVFRelation.class::isInstance).get(0);
            PaimonVectorSearchTableValuedFunction function = Assertions.assertInstanceOf(
                    PaimonVectorSearchTableValuedFunction.class, relation.getFunction().getCatalogFunction());
            PaimonScanNode node = createScanNode(function);
            TFileScanRangeParams params = new TFileScanRangeParams();
            Deencapsulation.setField(node, "params", params);
            Deencapsulation.invoke(node, "setExternalSearchRequest");
            Assertions.assertEquals(1, params.getExternalSearchRequest().getPaimonOptions().getPrefilterSize());
        }
    }

    @Test
    public void testExplicitPrefilterNormalizesComparisons() throws Exception {
        try (Fixture fixture = new Fixture("l2")) {
            Map<String, TExprOpcode> comparisons = ImmutableMap.of(
                    "id <=> 7", TExprOpcode.EQ,
                    "7 <=> id", TExprOpcode.EQ,
                    "7 < id", TExprOpcode.GT,
                    "7 >= id", TExprOpcode.LE);
            for (Map.Entry<String, TExprOpcode> comparison : comparisons.entrySet()) {
                LogicalTVFRelation relation = analyzeSearch(comparison.getKey());
                PaimonVectorSearchTableValuedFunction function =
                        (PaimonVectorSearchTableValuedFunction) relation.getFunction().getCatalogFunction();
                TExpr predicate = searchRequest(createScanNode(function))
                        .getPaimonOptions().getPrefilter().get(0);
                Assertions.assertEquals(comparison.getValue(), predicate.getNodes().get(0).getOpcode(),
                        comparison.getKey());
                Assertions.assertEquals(TExprNodeType.SLOT_REF, predicate.getNodes().get(1).getNodeType(),
                        comparison.getKey());
            }
        }
    }

    @Test
    public void testExplicitPrefilterNormalizesNullLiteral() throws Exception {
        try (Fixture fixture = new Fixture("l2")) {
            for (String filter : Arrays.asList("id <=> null", "null <=> id")) {
                LogicalTVFRelation relation = analyzeSearch(filter);
                PaimonVectorSearchTableValuedFunction function =
                        (PaimonVectorSearchTableValuedFunction) relation.getFunction().getCatalogFunction();
                TExpr predicate = searchRequest(createScanNode(function))
                        .getPaimonOptions().getPrefilter().get(0);
                Assertions.assertEquals("is_null_pred",
                        predicate.getNodes().get(0).getFn().getName().getFunctionName());
                Assertions.assertEquals(TExprNodeType.SLOT_REF, predicate.getNodes().get(1).getNodeType());
            }
        }
    }

    @Test
    public void testPrefilterSlotSurvivesEliminatedFilterAndScanProjection() throws Exception {
        try (Fixture fixture = new Fixture("l2", false)) {
            CascadesContext cascades = analyzeSearchPlan("id IS NOT NULL");
            // Eliminate the tautology, as the real rewrite pipeline does. The remaining
            // distance-only projection cannot discover the private prefilter's id slot.
            PlanChecker.from(cascades).applyTopDown(new EliminateNotNull()).customRewrite(new ColumnPruning());
            Assertions.assertFalse(cascades.getRewritePlan().containsType(LogicalFilter.class));
            LogicalProject<?> scanProjection = cascades.getRewritePlan().<LogicalProject<?>>collectToList(
                    plan -> plan instanceof LogicalProject && plan.child(0) instanceof LogicalTVFRelation).get(0);
            Assertions.assertEquals(Collections.singletonList("_distance"), scanProjection.getOutput().stream()
                    .map(Slot::getName).collect(java.util.stream.Collectors.toList()));
            LogicalTVFRelation relation = cascades.getRewritePlan().<LogicalTVFRelation>collectToList(
                    LogicalTVFRelation.class::isInstance).get(0);
            PlanTranslatorContext translation = new PlanTranslatorContext();
            PaimonScanNode scan = translateDistanceProjection(relation, translation);
            Assertions.assertEquals(Arrays.asList("id", "_distance"), scan.getTupleDesc().getSlots().stream()
                    .map(slot -> slot.getColumn().getName()).collect(java.util.stream.Collectors.toList()));
            TExpr prefilter = searchRequest(scan).getPaimonOptions().getPrefilter().get(0);
            int slotId = prefilter.getNodes().stream().filter(node -> node.getNodeType() == TExprNodeType.SLOT_REF)
                    .findFirst().get().getSlotRef().getSlotId();
            Assertions.assertTrue(translation.getDescTable().toThrift().getSlotDescriptors().stream()
                    .anyMatch(slot -> slot.getId() == slotId), "BE prefilter must reference a transmitted descriptor");
        }
    }

    @Test
    public void testPostfilterDoesNotRetainHiddenScanDependencies() throws Exception {
        try (Fixture fixture = new Fixture("l2", false)) {
            CascadesContext cascades = analyzeSearchPlan("id IS NOT NULL", false);
            PlanChecker.from(cascades).applyTopDown(new EliminateNotNull()).customRewrite(new ColumnPruning());
            Assertions.assertFalse(cascades.getRewritePlan().containsType(LogicalFilter.class));
            LogicalTVFRelation relation = cascades.getRewritePlan().<LogicalTVFRelation>collectToList(
                    LogicalTVFRelation.class::isInstance).get(0);
            PaimonScanNode scan = translateDistanceProjection(relation, new PlanTranslatorContext());
            Assertions.assertEquals(Collections.singletonList("_distance"), scan.getTupleDesc().getSlots().stream()
                    .map(slot -> slot.getColumn().getName()).collect(java.util.stream.Collectors.toList()));
            Assertions.assertFalse(searchRequest(scan).getPaimonOptions().isSetPrefilter());
        }
    }

    private static LogicalTVFRelation analyzeSearch(String filter) {
        return analyzeSearchPlan(filter).getRewritePlan().<LogicalTVFRelation>collectToList(
                LogicalTVFRelation.class::isInstance).get(0);
    }

    private static CascadesContext analyzeSearchPlan(String filter) {
        return analyzeSearchPlan(filter, true);
    }

    private static CascadesContext analyzeSearchPlan(String filter, boolean prefilter) {
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select _distance from vector_search('table'='paimon_catalog.db.items',"
                        + "'column'='embedding','query_vector'='[1,2]','top_k'='1',"
                        + "'filter'='" + filter + "','prefilter'='" + prefilter + "')");
        CascadesContext cascades = CascadesContext.initContext(
                MemoTestUtils.createStatementContext(ConnectContext.get(), ""), plan, PhysicalProperties.ANY);
        cascades.newAnalyzer().analyze();
        return cascades;
    }

    private static PaimonScanNode translateDistanceProjection(LogicalTVFRelation relation,
            PlanTranslatorContext translation) throws Exception {
        PaimonVectorSearchTableValuedFunction function =
                (PaimonVectorSearchTableValuedFunction) relation.getFunction().getCatalogFunction();
        PaimonVectorSearchTableValuedFunction provider = Mockito.spy(function);
        Mockito.doAnswer(call -> {
            PaimonScanNode node = Mockito.spy((PaimonScanNode) function.getScanNode(
                    call.getArgument(0), call.getArgument(1), call.getArgument(2)));
            // Exercise real descriptor creation and projection pruning without external storage IO.
            Mockito.doNothing().when(node).init();
            return node;
        }).when(provider).getScanNode(Mockito.any(), Mockito.any(), Mockito.any());
        VectorSearch expression = Mockito.mock(VectorSearch.class);
        Mockito.when(expression.getCatalogFunction()).thenReturn(provider);
        Mockito.when(expression.getTable()).thenReturn(function.getTable());
        Mockito.when(expression.getName()).thenReturn(VectorSearchTableValuedFunction.NAME);
        PhysicalTVFRelation physical = new PhysicalTVFRelation(relation.getRelationId(), expression,
                Collections.emptyList(), relation.getLogicalProperties());
        Slot distance = relation.getOutput().stream().filter(slot -> slot.getName().equals("_distance"))
                .findFirst().get();
        PhysicalProject<PhysicalTVFRelation> project = new PhysicalProject<>(
                Collections.singletonList(distance), null, physical);
        PlanFragment fragment = project.accept(new PhysicalPlanTranslator(translation), translation);
        return Assertions.assertInstanceOf(PaimonScanNode.class, fragment.getPlanRoot());
    }

    private static TExternalSearchRequest searchRequest(PaimonScanNode node) {
        TFileScanRangeParams params = new TFileScanRangeParams();
        Deencapsulation.setField(node, "params", params);
        Deencapsulation.invoke(node, "setExternalSearchRequest");
        return params.getExternalSearchRequest();
    }

    @Test
    public void testPaimonDoesNotUseLanceRowIdMaterialization() throws Exception {
        try (Fixture fixture = new Fixture("l2")) {
            PaimonVectorSearchTableValuedFunction function = fixture.function(Collections.emptyMap());
            VectorSearch expression = Mockito.mock(VectorSearch.class);
            Mockito.when(expression.getName()).thenReturn(VectorSearchTableValuedFunction.NAME);
            Mockito.when(expression.getTVFProperties()).thenReturn(new Properties(Collections.emptyMap()));
            Mockito.when(expression.getCatalogFunction()).thenReturn(function);
            PhysicalTVFRelation relation = Mockito.mock(PhysicalTVFRelation.class);
            Mockito.when(relation.getFunction()).thenReturn(expression);
            boolean supported = Deencapsulation.invoke(new MaterializeProbeVisitor(),
                    "checkTVFRelationTableSupportedType", relation);
            Assertions.assertFalse(supported, "Paimon cannot fetch Lance row IDs");
        }
    }

    @Test
    public void testEmptySnapshotDoesNotPlanAgainstNewLatest() throws Exception {
        try (Fixture fixture = new Fixture("l2")) {
            Mockito.when(fixture.external.loadSnapshot(Mockito.any(), Mockito.any())).thenReturn(
                    new PaimonMvccSnapshot(new PaimonSnapshotCacheValue(null,
                            new PaimonSnapshot(PaimonSnapshot.INVALID_SNAPSHOT_ID, 1L, fixture.table))));
            PaimonScanNode node = createScanNode(fixture.function(Collections.emptyMap()));
            Assertions.assertTrue(node.getVectorSearchSplits().isEmpty());
            Mockito.verify(fixture.table, Mockito.never()).newBatchVectorSearchBuilder();
        }
    }

    @Test
    public void testExplicitVersionLoadsSnapshotWithoutOrdinaryRelation() throws Exception {
        try (Fixture fixture = new Fixture("l2")) {
            fixture.function(ImmutableMap.of("version", "7"));
            Mockito.verify(fixture.external).loadSnapshot(
                    Mockito.argThat(snapshot -> snapshot.isPresent()
                            && "7".equals(snapshot.get().getValue())), Mockito.eq(Optional.empty()));
        }
    }

    @Test
    public void testScanUsesPaimonAndFoldsOffsetOnlyForCandidateRetrieval() throws Exception {
        try (Fixture fixture = new Fixture("l2")) {
            PaimonVectorSearchTableValuedFunction function = fixture.function(
                    ImmutableMap.of("top_k", "3", "offset", "2"));
            PaimonScanNode node = createScanNode(function);
            Assertions.assertTrue(node.isExternalSearch());
            Assertions.assertTrue(node.isVectorSearch());
            TFileScanRangeParams params = new TFileScanRangeParams();
            Deencapsulation.setField(node, "params", params);
            Deencapsulation.invoke(node, "setExternalSearchRequest");
            Assertions.assertEquals(5, params.getExternalSearchRequest().getSearchQuery().getVectorSearch().getTopK());
            Assertions.assertEquals(0,
                    params.getExternalSearchRequest().getSearchQuery().getVectorSearch().getOffset());
            Assertions.assertEquals(3, function.getTopK());
            Assertions.assertEquals(2, function.getOffset());
        }
    }

    private static PaimonScanNode createScanNode(PaimonVectorSearchTableValuedFunction function) throws Exception {
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor tuple = descriptors.createTupleDescriptor();
        tuple.setTable(function.getTable());
        for (org.apache.doris.catalog.Column column : function.getTableColumns()) {
            SlotDescriptor slot = descriptors.addSlotDescriptor(tuple);
            slot.setColumn(column);
            slot.setType(column.getType());
        }
        return Assertions.assertInstanceOf(PaimonScanNode.class,
                function.getScanNode(new PlanNodeId(0), tuple, ConnectContext.get().getSessionVariable()));
    }
}
