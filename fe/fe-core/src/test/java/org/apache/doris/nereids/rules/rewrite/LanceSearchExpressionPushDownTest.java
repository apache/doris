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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.metadata.LanceMvccSnapshot;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProduct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2Distance;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Md5;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.tablefunction.FullTextSearchTableValuedFunction;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;
import org.apache.doris.thrift.TFtsMatchOperator;
import org.apache.doris.thrift.TFtsQueryType;
import org.apache.doris.thrift.TSearchFilterFormat;
import org.apache.doris.thrift.TVectorMetric;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class LanceSearchExpressionPushDownTest {
    private ConnectContext context;
    private ConnectContext previous;
    private LogicalFileScan scan;
    private LanceExternalTable table;
    private LanceTableMetadata metadata;
    private SlotReference id;
    private SlotReference body;
    private SlotReference embedding;

    @BeforeEach
    void setup() {
        previous = ConnectContext.get();
        context = MemoTestUtils.createConnectContext();
        Schema schema = new Schema(ImmutableList.of(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("body", ArrowType.Utf8.INSTANCE),
                new Field("embedding", FieldType.nullable(
                        new ArrowType.FixedSizeList(2)), ImmutableList.of(Field.nullable("item",
                                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))))));
        metadata = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()), 42, schema,
                Collections.emptyList(), ImmutableMap.of("id", 0, "body", 1, "embedding", 2),
                Collections.emptyList());
        table = Mockito.mock(LanceExternalTable.class);
        Mockito.when(table.getName()).thenReturn("items");
        Mockito.when(table.getDbName()).thenReturn("db");
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getMetadata(Mockito.any())).thenReturn(metadata);
        List<Column> columns = ImmutableList.of(new Column("id", Type.BIGINT),
                new Column("body", Type.STRING), new Column("embedding",
                        org.apache.doris.catalog.ArrayType.create(Type.FLOAT, true)));
        Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(columns);
        List<String> qualifier = ImmutableList.of("catalog", "db", "items");
        id = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), table, columns.get(0), qualifier);
        body = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), table, columns.get(1), qualifier);
        embedding = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), table, columns.get(2), qualifier);
        scan = new LogicalFileScan(new RelationId(1), table, ImmutableList.of("catalog", "db"),
                Collections.emptyList(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(ImmutableList.of(id, body, embedding)),
                Optional.of(new LanceMvccSnapshot(metadata)));
    }

    @AfterEach
    void cleanup() {
        ConnectContext.remove();
        if (previous != null) {
            previous.setThreadLocalInfo();
        }
    }

    private ArrayLiteral vector() {
        return new ArrayLiteral(ImmutableList.of(new FloatLiteral(1), new FloatLiteral(2)));
    }

    private Plan rewrite(Expression expression, boolean ascending, Plan input) {
        return rewrite(expression, ascending, input, 5, 2);
    }

    private Plan rewrite(Expression expression, boolean ascending, Plan input, long limit, long offset) {
        Alias result = new Alias(expression, "result");
        LogicalProject<Plan> project = new LogicalProject<>(ImmutableList.of(id, result), input);
        LogicalTopN<?> topN = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(result.toSlot(), ascending, false)), limit, offset, project);
        return PlanChecker.from(context, topN)
                .applyTopDown(new PushDownSearchTopNIntoLanceScan())
                .getPlan();
    }

    private LogicalTVFRelation search(Plan plan) {
        List<LogicalTVFRelation> searches = plan.collectToList(LogicalTVFRelation.class::isInstance);
        Assertions.assertEquals(1, searches.size(), plan.treeString());
        return searches.get(0);
    }

    @Test
    void l2TopNUsesLanceAndRestoresEuclideanDistance() {
        Plan plan = rewrite(new L2DistanceApproximate(embedding, vector()), true, scan);
        VectorSearchTableValuedFunction function = (VectorSearchTableValuedFunction)
                search(plan).getFunction().getCatalogFunction();
        Assertions.assertEquals(TVectorMetric.L2, function.getSearchRequest().getSearchQuery()
                .getVectorSearch().getMetric());
        Assertions.assertEquals(42, function.getMetadata().getVersion());
        Assertions.assertEquals(7, function.getTopK());
        Assertions.assertEquals(0, function.getOffset());
        Assertions.assertTrue(plan.treeString().toLowerCase().contains("sqrt"), plan.treeString());
        Assertions.assertEquals(2, ((LogicalTopN<?>) plan).getOffset());
        Assertions.assertEquals(ImmutableList.of("id", "result"),
                plan.getOutput().stream().map(Slot::getName).collect(Collectors.toList()));
    }

    @Test
    void innerProductTopNUsesDotAndRestoresSimilarity() {
        Plan plan = rewrite(new InnerProductApproximate(embedding, vector()), false, scan);
        VectorSearchTableValuedFunction function = (VectorSearchTableValuedFunction)
                search(plan).getFunction().getCatalogFunction();
        Assertions.assertEquals(TVectorMetric.DOT_PRODUCT, function.getSearchRequest().getSearchQuery()
                .getVectorSearch().getMetric());
        Assertions.assertTrue(plan.anyMatch(p -> ((Plan) p).getExpressions().stream()
                .anyMatch(e -> e.anyMatch(Subtract.class::isInstance))),
                plan.treeString());
    }

    @Test
    void matchAndScoreUseLanceBm25() {
        Plan input = new LogicalFilter<>(ImmutableSet.of(new MatchAny(body, new StringLiteral("apple"))), scan);
        Plan plan = rewrite(new Score(), false, input);
        FullTextSearchTableValuedFunction function = (FullTextSearchTableValuedFunction)
                search(plan).getFunction().getCatalogFunction();
        Assertions.assertEquals(TFtsMatchOperator.OR, function.getSearchRequest().getSearchQuery()
                .getFullTextSearch().getMatchOperator());
        Assertions.assertEquals("body", function.getSearchRequest().getSearchQuery()
                .getFullTextSearch().getColumn());
        Assertions.assertFalse(plan.anyMatch(p -> ((Plan) p).getExpressions().stream()
                .anyMatch(e -> e.anyMatch(Score.class::isInstance))), plan.treeString());
    }

    @Test
    void ordinaryFiltersRunBeforeVectorTopK() {
        Plan input = new LogicalFilter<>(ImmutableSet.of(new EqualTo(id, new BigIntLiteral(9))), scan);
        Plan plan = rewrite(new L2DistanceApproximate(embedding, vector()), true, input);
        VectorSearchTableValuedFunction function = (VectorSearchTableValuedFunction)
                search(plan).getFunction().getCatalogFunction();
        Assertions.assertTrue(function.getSearchRequest().isPrefilter());
        Assertions.assertEquals(TSearchFilterFormat.SUBSTRAIT,
                function.getSearchRequest().getSearchFilter().getFormat());
        Assertions.assertTrue(function.getSearchRequest().getSearchFilter().getPayload().length > 0);
        Assertions.assertTrue(plan.collectToList(LogicalFilter.class::isInstance).isEmpty(), plan.treeString());
    }

    @Test
    void allAndPhraseMatchesUseTheirNativeLanceQueryTypes() {
        for (boolean phrase : new boolean[] {false, true}) {
            Expression match = phrase ? new MatchPhrase(body, new StringLiteral("apple banana"))
                    : new MatchAll(body, new StringLiteral("apple banana"));
            Plan input = new LogicalFilter<>(ImmutableSet.of(match), scan);
            FullTextSearchTableValuedFunction function = (FullTextSearchTableValuedFunction)
                    search(rewrite(new Score(), false, input)).getFunction().getCatalogFunction();
            Assertions.assertEquals(phrase ? TFtsQueryType.PHRASE
                    : TFtsQueryType.MATCH,
                    function.getSearchRequest().getSearchQuery().getFullTextSearch().getQueryType());
            if (!phrase) {
                Assertions.assertEquals(TFtsMatchOperator.AND,
                        function.getSearchRequest().getSearchQuery().getFullTextSearch().getMatchOperator());
            }
        }
    }

    @Test
    void scoreThresholdRemainsAboveSearchWithoutEvaluatingScoreFunction() {
        Plan input = new LogicalFilter<>(ImmutableSet.of(new MatchAny(body, new StringLiteral("apple")),
                new GreaterThan(new Score(), new FloatLiteral(0.5f)),
                new EqualTo(id, new BigIntLiteral(9))), scan);
        Plan plan = rewrite(new Score(), false, input);
        FullTextSearchTableValuedFunction function = (FullTextSearchTableValuedFunction)
                search(plan).getFunction().getCatalogFunction();
        Assertions.assertTrue(function.getSearchRequest().isPrefilter());
        Assertions.assertTrue(function.getSearchRequest().isSetSearchFilter());
        Assertions.assertEquals(1, plan.collectToList(LogicalFilter.class::isInstance).size());
        Assertions.assertFalse(plan.anyMatch(p -> ((Plan) p).getExpressions().stream()
                .anyMatch(e -> e.anyMatch(Score.class::isInstance))), plan.treeString());
    }

    @Test
    void indexDiscoveryUsesTheBoundVersionInsteadOfLatest() {
        LanceTableMetadata basic = LanceTableMetadata.createBasicSnapshot(
                new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()), 42,
                metadata.getSchema(), Collections.emptyList());
        Mockito.when(table.getMetadata(Mockito.any())).thenReturn(basic);
        Mockito.when(table.loadMetadataForSearch(Mockito.any())).thenReturn(metadata);
        Plan plan = rewrite(new L2DistanceApproximate(embedding, vector()), true, scan);
        search(plan);
        ArgumentCaptor<LanceRefSelector> selector =
                ArgumentCaptor.forClass(LanceRefSelector.class);
        Mockito.verify(table).loadMetadataForSearch(selector.capture());
        Assertions.assertEquals("42", selector.getValue().getSnapshot().get().getValue());
        Mockito.verify(table, Mockito.never()).loadBasicMetadata(Mockito.any());
    }

    @Test
    void rejectScoreUpperBoundsAndAscendingBm25() {
        Expression match = new MatchAny(body, new StringLiteral("apple"));
        Assertions.assertThrows(AnalysisException.class,
                () -> rewrite(new Score(), true, new LogicalFilter<>(ImmutableSet.of(match), scan)));
        Plan input = new LogicalFilter<>(ImmutableSet.of(match,
                new LessThan(new Score(), new FloatLiteral(1))), scan);
        Assertions.assertThrows(AnalysisException.class,
                () -> rewrite(new Score(), false, input));
    }

    @Test
    void doNotReintroduceTheEmbeddingProjection() {
        Plan plan = rewrite(new L2DistanceApproximate(embedding, vector()), true, scan);
        Assertions.assertEquals(ImmutableList.of("id", "_distance"), search(plan).getOutput().stream()
                .map(Slot::getName).collect(Collectors.toList()));
    }

    @Test
    void fullRewritePipelineUsesTheLanceSearchRule() {
        Alias result = new Alias(new L2DistanceApproximate(embedding, vector()), "distance");
        LogicalTopN<?> topN = new LogicalTopN<>(ImmutableList.of(new OrderKey(result.toSlot(), true, false)),
                5, 0, new LogicalProject<>(ImmutableList.of(id, result), scan));
        Plan plan = PlanChecker.from(context, topN).rewrite().getPlan();
        search(plan);
    }

    @Test
    void reservedResultColumnKeepsAnOtherwiseValidVectorQueryScalar() {
        List<Field> fields = new ArrayList<>(metadata.getSchema().getFields());
        fields.add(Field.nullable("_distance", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
        LanceTableMetadata collision = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()), 42,
                new Schema(fields), Collections.emptyList(),
                ImmutableMap.of("id", 0, "body", 1, "embedding", 2, "_distance", 3), Collections.emptyList());
        Mockito.when(table.getMetadata(Mockito.any())).thenReturn(collision);
        Assertions.assertTrue(rewrite(new L2DistanceApproximate(embedding, vector()), true, scan)
                .collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
    }

    @Test
    void nonMovableProjectionCannotBeSkippedByCandidatePruning() {
        Alias result = new Alias(new L2DistanceApproximate(embedding, vector()), "distance");
        Alias checked = new Alias(new AssertTrue(new GreaterThan(id, new BigIntLiteral(0)),
                new StringLiteral("bad")), "checked");
        LogicalProject<Plan> project = new LogicalProject<>(ImmutableList.of(id, checked, result), scan);
        LogicalTopN<?> topN = new LogicalTopN<>(ImmutableList.of(new OrderKey(result.toSlot(), true, false)),
                1, 0, project);
        Plan plan = PlanChecker.from(context, topN)
                .applyTopDown(new PushDownSearchTopNIntoLanceScan()).getPlan();
        Assertions.assertTrue(plan.collectToList(LogicalTVFRelation.class::isInstance).isEmpty(), plan.treeString());
        Assertions.assertTrue(plan.anyMatch(p -> ((Plan) p).getExpressions().stream()
                .anyMatch(e -> e.containsType(AssertTrue.class))), plan.treeString());
    }

    @Test
    void approximateFunctionsSelectNativeSearchOnNullableVectorSchemas() {
        // Approximate SQL intentionally uses the provider's candidate domain, unlike exact SQL.
        // The corresponding reader test covers exclusion of stored NULL vectors / NaN distances.
        Assertions.assertTrue(metadata.getSchema().findField("embedding").isNullable());
        search(rewrite(new L2DistanceApproximate(embedding, vector()), true, scan));
        search(rewrite(new InnerProductApproximate(embedding, vector()), false, scan));
        Assertions.assertTrue(rewrite(new L2Distance(embedding, vector()), true, scan)
                .collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
    }

    @Test
    void fullTextRewritePreservesBoundQueryWhitespace() {
        for (String query : ImmutableList.of(" foo ", " ")) {
            Plan input = new LogicalFilter<>(ImmutableSet.of(new MatchAny(body, new StringLiteral(query))), scan);
            FullTextSearchTableValuedFunction function = (FullTextSearchTableValuedFunction)
                    search(rewrite(new Score(), false, input)).getFunction().getCatalogFunction();
            Assertions.assertEquals(query, function.getSearchRequest().getSearchQuery().getFullTextSearch().getQuery());
        }
    }

    @Test
    void boundSearchFactoriesPreservePhysicalColumnWhitespace() throws Exception {
        List<Field> fields = new ArrayList<>(metadata.getSchema().getFields());
        fields.add(Field.nullable(" body ", ArrowType.Utf8.INSTANCE));
        Field vectorField = metadata.getSchema().findField("embedding");
        fields.add(new Field(" embedding ", vectorField.getFieldType(), vectorField.getChildren()));
        LanceTableMetadata padded = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()), 42,
                new Schema(fields), Collections.emptyList(),
                ImmutableMap.of("id", 0, "body", 1, "embedding", 2, " body ", 3, " embedding ", 4),
                Collections.emptyList());
        VectorSearchTableValuedFunction vectorSearch = VectorSearchTableValuedFunction.forScan(
                ImmutableMap.of("table", "catalog.db.items", "column", " embedding ", "query_vector", "[1,2]"),
                table, padded, new byte[0]);
        Assertions.assertEquals(" embedding ",
                vectorSearch.getSearchRequest().getSearchQuery().getVectorSearch().getColumn());
        FullTextSearchTableValuedFunction textSearch = FullTextSearchTableValuedFunction.forScan(
                ImmutableMap.of("table", "catalog.db.items", "column", " body ", "query", " foo "),
                table, padded, new byte[0]);
        Assertions.assertEquals(" body ",
                textSearch.getSearchRequest().getSearchQuery().getFullTextSearch().getColumn());
        Assertions.assertEquals(" foo ",
                textSearch.getSearchRequest().getSearchQuery().getFullTextSearch().getQuery());
    }

    @Test
    void unavailableFieldIdsAfterEnrichmentKeepVectorQueriesScalar() {
        LanceTableMetadata basic = LanceTableMetadata.createBasicSnapshot(
                new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()), 42,
                metadata.getSchema(), Collections.emptyList());
        LanceTableMetadata unavailable = LanceTableMetadata.createSnapshotWithUnavailableFieldIds(
                new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()), 42,
                metadata.getSchema(), Collections.emptyList(), Collections.emptyList());
        Mockito.when(table.getMetadata(Mockito.any())).thenReturn(basic);
        Mockito.when(table.loadMetadataForSearch(Mockito.any())).thenReturn(unavailable);
        Assertions.assertTrue(rewrite(new L2DistanceApproximate(embedding, vector()), true, scan)
                .collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
        Assertions.assertTrue(rewrite(new InnerProductApproximate(embedding, vector()), false, scan)
                .collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
    }

    @Test
    void nativeCandidateOverflowKeepsVectorQueriesScalar() {
        for (long[] bounds : new long[][] {{0x1_0000_0000L, 0}, {1, 0xffff_ffffL},
                {Long.MAX_VALUE, Long.MAX_VALUE}}) {
            Assertions.assertTrue(rewrite(new L2DistanceApproximate(embedding, vector()), true, scan,
                    bounds[0], bounds[1]).collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
            Assertions.assertTrue(rewrite(new InnerProductApproximate(embedding, vector()), false, scan,
                    bounds[0], bounds[1]).collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
        }
    }

    @Test
    void nativeCandidateOverflowDoesNotRejectOtherScalarFallbacks() {
        Plan filtered = new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(new Md5(body), new StringLiteral("unpushable"))), scan);
        Assertions.assertTrue(rewrite(new L2DistanceApproximate(embedding, vector()), true, filtered,
                0x1_0000_0000L, 0).collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
        List<Field> fields = new ArrayList<>(metadata.getSchema().getFields());
        Field field = fields.get(2);
        fields.set(2, new Field("embedding", FieldType.nullable(ArrowType.List.INSTANCE), field.getChildren()));
        LanceTableMetadata variableList = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()), 42,
                new Schema(fields), Collections.emptyList(), ImmutableMap.of("id", 0, "body", 1, "embedding", 2),
                Collections.emptyList());
        Mockito.when(table.getMetadata(Mockito.any())).thenReturn(variableList);
        Assertions.assertTrue(rewrite(new L2DistanceApproximate(embedding, vector()), true, scan,
                0x1_0000_0000L, 0).collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
    }

    @Test
    void largestNativeCandidateCountStillPushesVectorSearch() {
        Plan plan = rewrite(new L2DistanceApproximate(embedding, vector()), true, scan, 1, 0xffff_fffeL);
        VectorSearchTableValuedFunction function = (VectorSearchTableValuedFunction)
                search(plan).getFunction().getCatalogFunction();
        Assertions.assertEquals(0xffff_ffffL, function.getTopK());
        Assertions.assertEquals(0, function.getOffset());
        Assertions.assertEquals(0xffff_fffeL, ((LogicalTopN<?>) plan).getOffset());
    }

    @Test
    void bm25RetainsItsExplicitNativeCandidateLimitError() {
        Plan input = new LogicalFilter<>(ImmutableSet.of(new MatchAny(body, new StringLiteral("apple"))), scan);
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> rewrite(new Score(), false, input, 0x1_0000_0000L, 0));
        Assertions.assertTrue(error.getMessage().contains("limit + offset <= 4294967295"));
    }

    @Test
    void exactDistanceAndReverseOrderRemainScalar() {
        Assertions.assertTrue(rewrite(new InnerProduct(embedding, vector()), false, scan)
                .collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
        Assertions.assertTrue(rewrite(new L2Distance(embedding, vector()), true, scan)
                .collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
        Assertions.assertTrue(rewrite(new L2DistanceApproximate(embedding, vector()), false, scan)
                .collectToList(LogicalTVFRelation.class::isInstance).isEmpty());
    }
}
