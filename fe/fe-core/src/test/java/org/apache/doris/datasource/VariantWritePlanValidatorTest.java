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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.executor.Analyzer;
import org.apache.doris.nereids.rules.analysis.AnalyzeCTE;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWorkTableReference;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.ConnectorComputeVariantType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

public class VariantWritePlanValidatorTest {
    private static final String VARIANT_SOURCE = "(SELECT parse_to_variant('{\"n\":2}') AS v) source";

    @Test
    public void testUnionIfAndCaseImplicitVariantLossAreRejected() {
        List<Plan> lossyPlans = ImmutableList.of(
                lossyUnionPlan(),
                projectExpression(variant -> new Cast(variant, IntegerType.INSTANCE)),
                projectExpression(variant -> new If(
                        BooleanLiteral.TRUE,
                        new Cast(variant, IntegerType.INSTANCE),
                        new IntegerLiteral(1))),
                projectExpression(variant -> new CaseWhen(
                        ImmutableList.of(new WhenClause(
                                BooleanLiteral.TRUE,
                                new Cast(variant, IntegerType.INSTANCE))),
                        new IntegerLiteral(1))));

        for (Plan plan : lossyPlans) {
            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class,
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), plan));
            Assertions.assertTrue(exception.getMessage().contains(
                    "VARIANT write cannot safely convert input column 'payload'"), exception.getMessage());
            Assertions.assertTrue(exception.getMessage().contains("implicitly casts VARIANT to INT"),
                    exception.getMessage());
            Assertions.assertTrue(exception.getMessage().contains("use an explicit CAST if the conversion is intended"),
                    exception.getMessage());
        }
    }

    @Test
    public void testAnalyzedUnionIfAndCaseObjectAndArrayPlansAreRejected() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        List<String> sqlStatements = ImmutableList.of(
                "SELECT 1, parse_to_variant('{\"kind\":\"union\"}') "
                        + "UNION ALL SELECT 2, 1",
                "SELECT 3, parse_to_variant('[\"union\", 3]') "
                        + "UNION ALL SELECT 4, 1",
                "SELECT 3, IF(TRUE, parse_to_variant('{\"kind\":\"if\"}'), 1)",
                "SELECT 4, IF(TRUE, parse_to_variant('[\"if\", 4]'), 1)",
                "SELECT 5, CASE WHEN TRUE THEN parse_to_variant('{\"kind\":\"case\"}') "
                        + "ELSE 1 END",
                "SELECT 6, CASE WHEN TRUE THEN parse_to_variant('[\"case\", 6]') "
                        + "ELSE 1 END");

        for (String sql : sqlStatements) {
            Plan analyzedPlan = PlanChecker.from(connectContext).analyze(sql).getPlan();
            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class,
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), analyzedPlan),
                    sql);
            Assertions.assertTrue(exception.getMessage().contains("input column 'payload'"), exception.getMessage());
        }
    }

    @Test
    public void testAnalyzedExplicitVariantCastIsAccepted() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        Plan analyzedPlan = PlanChecker.from(connectContext)
                .analyze("SELECT 1, CAST(parse_to_variant('{\"explicit\":true}') AS INT)")
                .getPlan();
        VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), analyzedPlan);
    }

    @Test
    public void testAnalyzedCteProducerLossIsRejected() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        Plan analyzedPlan = PlanChecker.from(connectContext).analyze(
                "WITH source AS (SELECT CASE WHEN TRUE "
                        + "THEN parse_to_variant('{\"cte\":true}') ELSE 1 END AS payload) "
                        + "SELECT 1, payload FROM source").getPlan();
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), analyzedPlan));
        Assertions.assertTrue(exception.getMessage().contains("input column 'payload'"), exception.getMessage());
    }

    @Test
    public void testAnalyzedRecursiveUnionAndGenerateLossAreRejected() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        List<String> sqlStatements = ImmutableList.of(
                "WITH RECURSIVE source AS ("
                        + "SELECT IF(TRUE, parse_to_variant("
                        + "'{\"kind\":\"recursive\"}'), 1) AS payload "
                        + "UNION ALL SELECT CAST(1 AS DECIMAL(38, 9)) FROM source) "
                        + "SELECT 1, payload FROM source",
                "SELECT 1, generated_payload FROM (SELECT 1 AS seed) source "
                        + "LATERAL VIEW explode(ARRAY(IF(TRUE, "
                        + "parse_to_variant('{\"kind\":\"generate\"}'), 1))) generated "
                        + "AS generated_payload");

        for (String sql : sqlStatements) {
            Plan analyzedPlan = PlanChecker.from(connectContext).analyze(sql).getPlan();
            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class,
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), analyzedPlan),
                    sql);
            Assertions.assertTrue(exception.getMessage().contains("input column 'payload'"), exception.getMessage());
        }
    }

    @Test
    public void testRecursiveCteLossCarriedThroughAnotherColumnIsRejected() {
        // The recursive term writes payload from the work table's a, so from the second row on payload holds
        // what the anchor's a lost. MUTATION: tracing only the requested column of each term -> accepted.
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), recursiveCtePlan(0)));
        Assertions.assertTrue(exception.getMessage().contains("implicitly casts VARIANT to INT"),
                exception.getMessage());

        // The anchor's a never reaches payload when the recursive term writes payload from payload.
        VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), recursiveCtePlan(1));
    }

    @Test
    public void testScalarSubqueryLossIsRejected() {
        LogicalProject<?> lossyQueryPlan = projectExpression(variant -> new If(
                BooleanLiteral.TRUE,
                new Cast(variant, IntegerType.INSTANCE),
                new IntegerLiteral(1)));
        LogicalOneRowRelation lossyQuerySource = oneRow(
                new Alias(new ScalarSubquery(lossyQueryPlan), "payload"));

        ScalarSubquery coercedSubquery = (ScalarSubquery) new ScalarSubquery(variantInput())
                .withTypeCoercion(IntegerType.INSTANCE);
        LogicalOneRowRelation coercedSubquerySource = oneRow(
                new Alias(coercedSubquery, "payload"));

        for (Plan plan : ImmutableList.of(lossyQuerySource, coercedSubquerySource)) {
            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class,
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), plan));
            Assertions.assertTrue(exception.getMessage().contains("input column 'payload'"), exception.getMessage());
            Assertions.assertTrue(exception.getMessage().contains("implicitly casts VARIANT to INT"),
                    exception.getMessage());
        }
    }

    @Test
    public void testCorrelatedScalarSubqueryLossIsRejected() {
        LogicalProject<?> lossyOuterSource = projectExpression(variant -> new If(
                BooleanLiteral.TRUE,
                new Cast(variant, IntegerType.INSTANCE),
                new IntegerLiteral(1)));
        Slot correlatedSlot = lossyOuterSource.getOutput().get(0);
        LogicalOneRowRelation correlatedQuery = oneRow(
                new Alias(correlatedSlot, "correlated_payload"));
        ScalarSubquery correlatedSubquery = new ScalarSubquery(
                correlatedQuery, ImmutableList.of(correlatedSlot), false);
        LogicalProject<?> source = new LogicalProject<>(
                ImmutableList.of(new Alias(correlatedSubquery, "payload")),
                lossyOuterSource);

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), source));
        Assertions.assertTrue(exception.getMessage().contains("implicitly casts VARIANT to INT"),
                exception.getMessage());
    }

    @Test
    public void testAnalyzerCteContextIsUsedBeforeAnchorConstruction() {
        LogicalProject<?> lossyProducerChild = projectExpression(variant -> new If(
                BooleanLiteral.TRUE,
                new Cast(variant, IntegerType.INSTANCE),
                new IntegerLiteral(1)));
        LogicalSubQueryAlias<Plan> producerPlan = new LogicalSubQueryAlias<>(
                "source", lossyProducerChild);
        CTEId cteId = new CTEId(1);
        CTEContext cteContext = new CTEContext(cteId, producerPlan, new CTEContext());
        cteContext.setAnalyzedPlan(producerPlan);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                new RelationId(2), cteId, "source", producerPlan);
        LogicalOneRowRelation source = oneRow(
                new Alias(new ScalarSubquery(consumer), "payload"));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), source, cteContext));
        Assertions.assertTrue(exception.getMessage().contains("implicitly casts VARIANT to INT"),
                exception.getMessage());
    }

    @Test
    public void testWithNestedInAnalyzerContextProducerIsTraced() {
        // A leading WITH binds its sink before the outer anchors exist, so the outer producer comes from the
        // analyzer context and the WITH nested in it is not part of the traced plan. MUTATION: tracing the
        // fetched producer without collecting its nested producers -> accepted.
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        Plan nestedWith = PlanChecker.from(connectContext).analyze(
                "WITH inner_source AS (SELECT IF(TRUE, parse_to_variant('{\"lost\":true}'), 1) AS payload) "
                        + "SELECT payload FROM inner_source").getPlan();
        LogicalSubQueryAlias<Plan> producerPlan = new LogicalSubQueryAlias<>("outer_source", nestedWith);
        // Far from the ids the analyzer gave the nested WITH.
        CTEId cteId = new CTEId(1000);
        CTEContext cteContext = new CTEContext(cteId, producerPlan, new CTEContext());
        cteContext.setAnalyzedPlan(producerPlan);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                new RelationId(1000), cteId, "outer_source", producerPlan);
        LogicalOneRowRelation source = oneRow(
                new Alias(new ScalarSubquery(consumer), "payload"));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), source, cteContext));
        Assertions.assertTrue(exception.getMessage().contains("input column 'payload'"), exception.getMessage());
    }

    @Test
    public void testSharedCteProducerIsTracedOncePerRequestedOutputs() {
        // Each CTE reads the previous one twice, so walking every path would trace c0 2^30 times. MUTATION:
        // dropping the completed-trace cache -> times out.
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        StringBuilder sql = new StringBuilder("WITH c0 AS (SELECT parse_to_variant('{\"k\":1}') AS payload)");
        for (int level = 1; level <= 30; level++) {
            sql.append(String.format(", c%d AS (SELECT payload FROM c%d UNION ALL SELECT payload FROM c%d)",
                    level, level - 1, level - 1));
        }
        sql.append(" SELECT 1, payload FROM c30");
        Plan analyzedPlan = PlanChecker.from(connectContext).analyze(sql.toString()).getPlan();
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(30),
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), analyzedPlan));
    }

    @Test
    public void testCteOutputsRequestedInManySetsAreTracedOnceEach() {
        // c(i) reads c(i - 1) twice, for v0 alone and for v0 together with v(i), so the sets of outputs requested
        // from c(24 - k) double with k: a cache keyed by the set of requested outputs would trace c0 2^24 times.
        // Each output is traced once, and a loss carried by one of them only is still found. MUTATION: caching
        // the traces per set of requested outputs -> times out or runs out of memory.
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        Plan lossless = PlanChecker.from(connectContext)
                .analyze(cteChainRequestingDoublingSets(24, "parse_to_variant('{\"k\":1}')")).getPlan();
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(30),
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), lossless));

        Plan lossy = PlanChecker.from(connectContext).analyze(cteChainRequestingDoublingSets(
                24, "CAST(IF(TRUE, parse_to_variant('{\"lost\":true}'), 1) AS VARIANT)")).getPlan();
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), lossy));
        Assertions.assertTrue(exception.getMessage().contains("input column 'payload'"), exception.getMessage());
    }

    @Test
    public void testWindowAndAggregateOrderingKeysAreNotWrittenValues() {
        // The partition and order keys of a window and the ORDER BY of GROUP_CONCAT only arrange the rows whose
        // values are used, so a lossy cast there, or in a column read only by such a key, writes nothing. A lossy
        // cast in an argument of the function is written and still rejected. MUTATION: tracing the keys of a
        // window, or the ORDER BY of an aggregate function -> rejected.
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        String source = "(SELECT 1 AS id, parse_to_variant('{\"k\":1}') AS v) source";
        List<String> keyedOnly = ImmutableList.of(
                "SELECT 1, ROW_NUMBER() OVER (ORDER BY IF(TRUE, v, 1)) FROM " + source,
                "SELECT 1, COUNT(*) OVER (PARTITION BY IF(TRUE, v, 1)) FROM " + source,
                "SELECT 1, GROUP_CONCAT('x' ORDER BY IF(TRUE, v, 1)) FROM " + source,
                "SELECT 1, ROW_NUMBER() OVER (ORDER BY k) FROM (SELECT IF(TRUE, v, 1) AS k FROM " + source
                        + ") keyed");
        for (String sql : keyedOnly) {
            Plan analyzedPlan = PlanChecker.from(connectContext).analyze(sql).getPlan();
            Assertions.assertDoesNotThrow(
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), analyzedPlan),
                    sql);
        }

        List<String> written = ImmutableList.of(
                "SELECT 1, FIRST_VALUE(IF(TRUE, v, 1)) OVER (ORDER BY id) FROM " + source,
                "SELECT 1, GROUP_CONCAT(CAST(IF(TRUE, v, 1) AS STRING) ORDER BY id) FROM " + source);
        for (String sql : written) {
            Plan analyzedPlan = PlanChecker.from(connectContext).analyze(sql).getPlan();
            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class,
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), analyzedPlan),
                    sql);
            Assertions.assertTrue(exception.getMessage().contains("input column 'payload'"),
                    exception.getMessage());
        }
    }

    @Test
    public void testPrimitiveSourceAndExplicitVariantCastAreAccepted() {
        LogicalOneRowRelation primitiveSource = oneRow(
                new Alias(new IntegerLiteral(1), "payload"));
        VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), primitiveSource);

        LogicalProject<?> explicitCastSource = projectExpression(
                variant -> new Cast(variant, IntegerType.INSTANCE, true));
        VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), explicitCastSource);

        LogicalOneRowRelation arrayVariantInput = oneRow(new Alias(
                new NullLiteral(ArrayType.of(ConnectorComputeVariantType.INSTANCE)), "source_array"));
        LogicalProject<?> arrayToVariantSource = new LogicalProject<>(ImmutableList.of(
                new Alias(new Cast(
                        arrayVariantInput.getOutput().get(0),
                        ConnectorComputeVariantType.INSTANCE), "payload")),
                arrayVariantInput);
        VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), arrayToVariantSource);
    }

    @Test
    public void testLossyCastOutsideVariantTargetLineageIsIgnored() {
        LogicalOneRowRelation input = variantInput();
        Slot variantSlot = input.getOutput().get(0);
        LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                new Alias(new Cast(variantSlot, IntegerType.INSTANCE), "id"),
                new Alias(variantSlot, "payload")), input);
        List<Column> targets = ImmutableList.of(
                new Column("id", org.apache.doris.catalog.Type.INT),
                new Column("payload", ConnectorComputeVariantType.INSTANCE.toCatalogDataType()));

        VariantWritePlanValidator.validateNoLossyCoercion(targets, source);
    }

    @Test
    public void testIfAndCaseConditionsAreNotWrittenValues() {
        // The condition of an IF or of a CASE branch only chooses the value written: a Variant path compared
        // with a scalar there is implicitly cast, but nothing written is lost. MUTATION: searching the whole
        // expression tree for lossy casts -> rejected.
        List<Plan> conditionCastPlans = ImmutableList.of(
                projectExpression(variant -> new If(
                        new GreaterThan(new Cast(variant, IntegerType.INSTANCE), new IntegerLiteral(1)),
                        variant,
                        new NullLiteral(ConnectorComputeVariantType.INSTANCE))),
                projectExpression(variant -> new CaseWhen(
                        ImmutableList.of(new WhenClause(
                                new GreaterThan(new Cast(variant, IntegerType.INSTANCE), new IntegerLiteral(1)),
                                variant)),
                        new NullLiteral(ConnectorComputeVariantType.INSTANCE))));
        for (Plan plan : conditionCastPlans) {
            VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), plan);
        }

        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        List<String> sqlStatements = ImmutableList.of(
                "SELECT 1, IF(payload['kind'] = 'event', payload, NULL) "
                        + "FROM (SELECT parse_to_variant('{\"kind\":\"event\"}') AS payload) source",
                "SELECT 1, CASE WHEN payload['n'] > 1 THEN payload END "
                        + "FROM (SELECT parse_to_variant('{\"n\":2}') AS payload) source");
        for (String sql : sqlStatements) {
            Plan analyzedPlan = PlanChecker.from(connectContext).analyze(sql).getPlan();
            Assertions.assertDoesNotThrow(
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), analyzedPlan),
                    sql);
        }
    }

    @Test
    public void testColumnsReadOnlyByConditionsAreNotTraced() {
        // A column the target value reads only through a condition is not traced further: the lossy cast that
        // computes it is not written. MUTATION: tracing every input slot of the target expression -> rejected.
        LogicalOneRowRelation input = variantInput();
        Slot variantSlot = input.getOutput().get(0);
        LogicalProject<?> matched = new LogicalProject<>(ImmutableList.of(
                new Alias(variantSlot, "payload"),
                new Alias(new GreaterThan(new Cast(variantSlot, IntegerType.INSTANCE), new IntegerLiteral(1)),
                        "matched")), input);
        LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                new Alias(new If(
                        matched.getOutput().get(1),
                        matched.getOutput().get(0),
                        new NullLiteral(ConnectorComputeVariantType.INSTANCE)), "payload")), matched);
        VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), source);

        // A lossy cast the branch value reads through a column is still rejected.
        LogicalProject<?> lossyBranch = new LogicalProject<>(ImmutableList.of(
                new Alias(new Cast(variantSlot, IntegerType.INSTANCE), "narrowed")), input);
        LogicalProject<?> lossySource = new LogicalProject<>(ImmutableList.of(
                new Alias(new If(
                        BooleanLiteral.TRUE,
                        lossyBranch.getOutput().get(0),
                        new IntegerLiteral(1)), "payload")), lossyBranch);
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), lossySource));
        Assertions.assertTrue(exception.getMessage().contains("implicitly casts VARIANT to INT"),
                exception.getMessage());
    }

    @Test
    public void testLaterExceptAndIntersectInputsAreNotWrittenValues() {
        // EXCEPT and INTERSECT write rows of their first input; the second input only decides which rows stay,
        // so a lossy cast there writes nothing. MUTATION: tracing every set-operation input -> rejected.
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        String lossy = "IF(TRUE, parse_to_variant('{\"lost\":true}'), 1)";
        String numeric = "CAST(1 AS DECIMAL(38, 9))";
        for (String setOperator : ImmutableList.of("EXCEPT", "INTERSECT")) {
            Plan membershipOnly = PlanChecker.from(connectContext).analyze(
                    "SELECT 1, " + numeric + " AS payload " + setOperator + " SELECT 1, " + lossy).getPlan();
            Assertions.assertDoesNotThrow(
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), membershipOnly),
                    setOperator);

            // The first input is written, so its lossy cast is still rejected.
            Plan written = PlanChecker.from(connectContext).analyze(
                    "SELECT 1, " + lossy + " AS payload " + setOperator + " SELECT 1, " + numeric).getPlan();
            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class,
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTargetWithId(), written),
                    setOperator);
            Assertions.assertTrue(exception.getMessage().contains("input column 'payload'"),
                    exception.getMessage());
        }
    }

    @Test
    public void testRowLevelDmlRoutingColumnsUseMappedSourceOrdinal() {
        LogicalOneRowRelation input = variantInput();
        Slot variantSlot = input.getOutput().get(0);
        LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                new Alias(new IntegerLiteral(2), "operation"),
                new Alias(new IntegerLiteral(7), "row_id"),
                new Alias(new If(
                        BooleanLiteral.TRUE,
                        new Cast(variantSlot, IntegerType.INSTANCE),
                        new IntegerLiteral(1)), "payload")), input);

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(
                        variantTarget(), source, ImmutableList.of(2)));
        Assertions.assertTrue(exception.getMessage().contains("input column 'payload'"), exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("implicitly casts VARIANT to INT"),
                exception.getMessage());
    }

    @Test
    public void testDefaultMappingRequiresSameOutputWidth() {
        LogicalOneRowRelation source = oneRow(
                new Alias(new IntegerLiteral(2), "operation"),
                new Alias(new IntegerLiteral(7), "row_id"),
                new Alias(new NullLiteral(ConnectorComputeVariantType.INSTANCE), "payload"));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(variantTarget(), source));
        Assertions.assertTrue(exception.getMessage().contains("target and source columns are not aligned"),
                exception.getMessage());
    }

    @Test
    public void testLossyCastInNestedVariantTargetIsRejected() {
        ArrayType sourceType = ArrayType.of(ConnectorComputeVariantType.INSTANCE);
        LogicalOneRowRelation input = oneRow(new Alias(
                new NullLiteral(sourceType), "source_variants"));
        LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                new Alias(new Cast(
                        input.getOutput().get(0), ArrayType.of(IntegerType.INSTANCE)), "payloads")),
                input);
        List<Column> targets = ImmutableList.of(new Column(
                "payloads", sourceType.toCatalogDataType()));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(targets, source));
        Assertions.assertTrue(exception.getMessage().contains("implicitly casts VARIANT to ARRAY<INT>"),
                exception.getMessage());
    }

    @Test
    public void testScalarFieldOfStructTargetIsNotTraced() {
        // Only the parts of a value that reach a Variant leaf of the target are traced. The matched field of a
        // STRUCT<matched BOOLEAN, payload VARIANT> target is written as a BOOLEAN, so a Variant cast to a number
        // while computing it loses nothing written, however the struct gets its fields: built in place by
        // named_struct or struct, from the columns of a subquery, from the fields of a struct column, through a
        // cast between structs, an IF or a UNION. MUTATION: tracing all of a struct constructor, of the struct a
        // field is read from, of the input of a cast, of an IF branch or of a UNION input -> rejected.
        List<String> shapes = ImmutableList.of(
                "SELECT 1, named_struct('matched', %s, 'payload', %s) FROM " + VARIANT_SOURCE,
                "SELECT 1, struct(%s, %s) FROM " + VARIANT_SOURCE,
                "SELECT 1, named_struct('matched', m, 'payload', p) FROM (SELECT %s AS m, %s AS p FROM "
                        + VARIANT_SOURCE + ") projected",
                "SELECT 1, named_struct('matched', s.matched, 'payload', s.payload) FROM (SELECT named_struct("
                        + "'matched', %s, 'payload', %s) AS s FROM " + VARIANT_SOURCE + ") nested",
                "SELECT 1, CAST(named_struct('m', %s, 'p', %s) AS STRUCT<matched: BOOLEAN, payload: VARIANT>) FROM "
                        + VARIANT_SOURCE,
                "SELECT 1, IF(v['n'] > 1, named_struct('matched', %s, 'payload', %s), NULL) FROM " + VARIANT_SOURCE,
                "SELECT 1, named_struct('matched', %s, 'payload', %s) FROM " + VARIANT_SOURCE
                        + " UNION ALL SELECT 2, named_struct('matched', FALSE, 'payload', v) FROM " + VARIANT_SOURCE);
        assertAccepted(flaggedTarget(), formatEach(shapes, "IF(TRUE, v['n'], 1) > 1", "v"));

        // The same loss in the payload field reaches the Variant leaf and is still rejected.
        assertRejected(flaggedTarget(), "flagged", formatEach(shapes, "TRUE", "IF(TRUE, v, 1)"));
    }

    @Test
    public void testStructFieldIsTracedByTheTargetFieldItIsWrittenTo() {
        // A field read from a struct column is traced by the target field it is written to, not by its place in the
        // struct it is read from: written to matched, the lossy field is not traced; written to payload, it is
        // rejected, whether the field is chosen by name or by position. MUTATION: tracing all of the struct a field
        // is read from -> the first group is rejected.
        String nested = "(SELECT named_struct('lossy', IF(TRUE, v, 1), 'kept', v) AS s FROM " + VARIANT_SOURCE
                + ") nested";
        assertAccepted(flaggedTarget(), ImmutableList.of(
                "SELECT 1, named_struct('matched', s.lossy > 1, 'payload', s.kept) FROM " + nested,
                "SELECT 1, named_struct('matched', struct_element(s, 1) > 1, 'payload', struct_element(s, 2)) FROM "
                        + nested));
        assertRejected(flaggedTarget(), "flagged", ImmutableList.of(
                "SELECT 1, named_struct('matched', s.kept IS NULL, 'payload', s.lossy) FROM " + nested,
                "SELECT 1, named_struct('matched', struct_element(s, 2) IS NULL, 'payload', struct_element(s, 1)) "
                        + "FROM " + nested));
    }

    @Test
    public void testArrayElementsAndMapValuesFollowTheTargetType() {
        // An ARRAY<STRUCT<matched BOOLEAN, payload VARIANT>> target traces each element as the struct target does,
        // and a MAP<STRING, VARIANT> target traces the values of the map but not its keys. MUTATION: tracing all of
        // an array or map constructor -> the accepted statements are rejected.
        List<Column> arrayTarget = targetWithId("flagged_list", ArrayType.of(flaggedType()));
        assertAccepted(arrayTarget, ImmutableList.of(
                "SELECT 1, ARRAY(named_struct('matched', IF(TRUE, v['n'], 1) > 1, 'payload', v)) FROM "
                        + VARIANT_SOURCE));
        assertRejected(arrayTarget, "flagged_list", ImmutableList.of(
                "SELECT 1, ARRAY(named_struct('matched', TRUE, 'payload', IF(TRUE, v, 1))) FROM " + VARIANT_SOURCE));

        List<Column> mapTarget = targetWithId(
                "payload_map", MapType.of(StringType.INSTANCE, ConnectorComputeVariantType.INSTANCE));
        assertAccepted(mapTarget, ImmutableList.of("SELECT 1, map(IF(TRUE, v['k'], 1), v) FROM " + VARIANT_SOURCE));
        assertRejected(mapTarget, "payload_map", ImmutableList.of(
                "SELECT 1, map('k', IF(TRUE, v, 1)) FROM " + VARIANT_SOURCE));
    }

    @Test
    public void testValueOfAnotherShapeIsTracedWhole() {
        // The parts of a struct target do not map onto a value of another shape, so such a value is traced whole:
        // a Variant written to the struct target, and an implicit cast from a Variant to the struct. MUTATION:
        // dropping the part of the demand that does not map onto the value -> accepted.
        assertRejected(flaggedTarget(), "flagged", ImmutableList.of(
                "SELECT 1, CAST(IF(TRUE, v, 1) AS VARIANT) FROM " + VARIANT_SOURCE));

        LogicalProject<?> variantToStruct = projectExpression(variant -> new Cast(variant, flaggedType()));
        List<Column> structTarget = ImmutableList.of(new Column("payload", flaggedType().toCatalogDataType()));
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> VariantWritePlanValidator.validateNoLossyCoercion(structTarget, variantToStruct));
        Assertions.assertTrue(exception.getMessage().contains("implicitly casts VARIANT to STRUCT<"),
                exception.getMessage());
    }

    @Test
    public void testSelectingAndComparingArgumentsAreNotWrittenValues() {
        // The second argument of NULLIF is only compared with the first, the index of an array and the key of a
        // map only choose the element returned, and the cast a predicate applies to an operand only serves the
        // comparison, so a Variant lost there writes nothing. MUTATION: tracing those arguments, or the operand
        // cast of a predicate -> rejected.
        assertAccepted(variantTargetWithId(), ImmutableList.of(
                "SELECT 1, NULLIF(1, IF(TRUE, v, 1)) FROM " + VARIANT_SOURCE,
                "SELECT 1, element_at(ARRAY(v), IF(TRUE, v['n'], 1)) FROM " + VARIANT_SOURCE,
                "SELECT 1, element_at(map('k', v), IF(TRUE, v['k'], 1)) FROM " + VARIANT_SOURCE,
                "SELECT 1, v['n'] > 0 FROM " + VARIANT_SOURCE,
                "SELECT 1, v['n'] IN (1, 2) FROM " + VARIANT_SOURCE,
                "SELECT 1, v['s'] LIKE 'a%' FROM " + VARIANT_SOURCE,
                "SELECT 1, NOT v['flag'] FROM " + VARIANT_SOURCE,
                "SELECT 1, ARRAY_FILTER(ARRAY(1, 2), ARRAY(v['n'] > 0, TRUE)) FROM " + VARIANT_SOURCE));

        // A returned argument is still traced, and only the cast a predicate applies itself is skipped: the common
        // type of an IF compared afterwards is still a loss. Other functions, whose arguments may be returned, are
        // traced whole.
        assertRejected(variantTargetWithId(), "payload", ImmutableList.of(
                "SELECT 1, NULLIF(IF(TRUE, v, 1), 1) FROM " + VARIANT_SOURCE,
                "SELECT 1, element_at(ARRAY(IF(TRUE, v, 1)), 1) FROM " + VARIANT_SOURCE,
                "SELECT 1, IF(TRUE, v, 1) > 0 FROM " + VARIANT_SOURCE,
                "SELECT 1, ELT(v['n'], 'a', 'b') FROM " + VARIANT_SOURCE,
                "SELECT 1, SUBSTRING('abc', v['n']) FROM " + VARIANT_SOURCE,
                "SELECT 1, MAX_BY(1, IF(TRUE, v, 1)) FROM " + VARIANT_SOURCE));
    }

    @Test
    public void testSubqueryAndIsTrueOperandsOfPredicatesAreNotWrittenValues() {
        // When BindSink validates the source, before SubqueryToApply, a scalar subquery keeps the cast a predicate
        // applies to it as its type coercion, IN over a subquery is an InSubquery, and IS TRUE / IS FALSE are AND,
        // NOT and IS NULL over the operand cast to BOOLEAN: still only the comparison is written. MUTATION: tracing
        // the type coercion of a compared scalar subquery, or the operand of an InSubquery or an IS NULL as a
        // value -> rejected.
        String keys = "(SELECT 1 AS k UNION ALL SELECT 2) ks";
        assertAccepted(variantTargetWithId(), ImmutableList.of(
                "SELECT 1, (SELECT v['s'] FROM " + VARIANT_SOURCE + ") LIKE 'a%'",
                "SELECT 1, v['n'] IN (SELECT k FROM " + keys + ") FROM " + VARIANT_SOURCE,
                "SELECT 1, v['flag'] IS TRUE FROM " + VARIANT_SOURCE,
                "SELECT 1, v['flag'] IS NOT FALSE FROM " + VARIANT_SOURCE),
                VariantWritePlanValidatorTest::bindSource);

        // The value compared is still traced: a loss in the query of the subquery or in the operand is rejected.
        assertRejected(variantTargetWithId(), "payload", ImmutableList.of(
                "SELECT 1, (SELECT IF(TRUE, v, 1) FROM " + VARIANT_SOURCE + ") LIKE 'a%'",
                "SELECT 1, IF(TRUE, v, 1) IN (SELECT k FROM " + keys + ") FROM " + VARIANT_SOURCE,
                "SELECT 1, IF(TRUE, v, 1) IS TRUE FROM " + VARIANT_SOURCE),
                VariantWritePlanValidatorTest::bindSource);
    }

    @Test
    public void testRecursiveCteNullableOutputsKeepTheFieldsTraced() {
        // AnalyzeCTE wraps every output of both terms of a recursive CTE as Alias(Nullable(slot)), which keeps the
        // value, so the matched field is still written as a BOOLEAN. Both terms build the struct the same way: in
        // this test the analyzer cannot bind a column of r in the recursive term. MUTATION: tracing all of the
        // argument of Nullable -> the first statement is rejected.
        String recursive = "WITH RECURSIVE r AS (SELECT named_struct('matched', %1$s, 'payload', %2$s) AS s FROM "
                + VARIANT_SOURCE + " UNION ALL SELECT named_struct('matched', %1$s, 'payload', %2$s) FROM r, "
                + VARIANT_SOURCE + ") SELECT 1, s FROM r";
        assertAccepted(flaggedTarget(), formatEach(ImmutableList.of(recursive), "IF(TRUE, v['n'], 1) > 1", "v"),
                VariantWritePlanValidatorTest::bindSource);
        assertRejected(flaggedTarget(), "flagged", formatEach(ImmutableList.of(recursive), "TRUE", "IF(TRUE, v, 1)"),
                VariantWritePlanValidatorTest::bindSource);
    }

    @Test
    public void testCteOutputIsTracedAgainForMoreOfIt() {
        // flagged reads the payload field of s only, raw writes all of s into a Variant: the producer of s is traced
        // again for raw although it was traced for flagged. MUTATION: reusing the trace of a CTE output whatever
        // part of it was traced -> accepted.
        String flags = "WITH flags AS (SELECT named_struct('matched', IF(TRUE, v['n'], 1) > 1, 'payload', v) AS s "
                + "FROM " + VARIANT_SOURCE + ") ";
        assertAccepted(flaggedTarget(), ImmutableList.of(flags + "SELECT 1, s FROM flags"));

        List<Column> targets = ImmutableList.of(
                new Column("id", org.apache.doris.catalog.Type.INT),
                new Column("flagged", flaggedType().toCatalogDataType()),
                new Column("raw", ConnectorComputeVariantType.INSTANCE.toCatalogDataType()));
        assertRejected(targets, "raw", ImmutableList.of(flags + "SELECT 1, s, s AS raw FROM flags"));
    }

    private static List<String> formatEach(List<String> templates, Object... arguments) {
        return templates.stream()
                .map(template -> String.format(template, arguments))
                .collect(ImmutableList.toImmutableList());
    }

    private static void assertAccepted(List<Column> targets, List<String> sqlStatements) {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        assertAccepted(targets, sqlStatements, sql -> PlanChecker.from(connectContext).analyze(sql).getPlan());
    }

    private static void assertAccepted(
            List<Column> targets, List<String> sqlStatements, Function<String, Plan> analyzer) {
        for (String sql : sqlStatements) {
            Plan analyzedPlan = analyzer.apply(sql);
            Assertions.assertDoesNotThrow(
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(targets, analyzedPlan), sql);
        }
    }

    private static void assertRejected(List<Column> targets, String targetColumn, List<String> sqlStatements) {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        assertRejected(targets, targetColumn, sqlStatements,
                sql -> PlanChecker.from(connectContext).analyze(sql).getPlan());
    }

    private static void assertRejected(List<Column> targets, String targetColumn, List<String> sqlStatements,
            Function<String, Plan> analyzer) {
        for (String sql : sqlStatements) {
            Plan analyzedPlan = analyzer.apply(sql);
            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class,
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(targets, analyzedPlan),
                    sql);
            Assertions.assertTrue(exception.getMessage().contains("input column '" + targetColumn + "'"),
                    sql + ": " + exception.getMessage());
        }
    }

    /** The plan of {@code sql} as BindSink validates it: bound, with subqueries still expressions. */
    private static Plan bindSource(String sql) {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                MemoTestUtils.createConnectContext(), sql);
        cascadesContext.newTableCollector(true).collect();
        Analyzer.buildCustomAnalyzer(cascadesContext, ImmutableList.of(
                Analyzer.topDown(new AnalyzeCTE()),
                Analyzer.bottomUp(new BindRelation(), new CheckPolicy(), new BindExpression()))).analyze();
        return cascadesContext.getRewritePlan();
    }

    /**
     * {@code WITH RECURSIVE source(a, payload) AS (SELECT IF(TRUE, CAST(v AS INT), 1), 1 UNION ALL SELECT a, x
     * FROM source) SELECT 1, payload FROM source}, shaped as AnalyzeCTE builds it, where x is the work-table
     * column at {@code payloadSource}.
     */
    private static Plan recursiveCtePlan(int payloadSource) {
        CTEId cteId = new CTEId(1);
        LogicalOneRowRelation input = variantInput();
        Plan anchor = new LogicalRecursiveUnionAnchor<>(cteId, new LogicalProject<>(ImmutableList.of(
                new Alias(new If(
                        BooleanLiteral.TRUE,
                        new Cast(input.getOutput().get(0), IntegerType.INSTANCE),
                        new IntegerLiteral(1)), "a"),
                new Alias(new IntegerLiteral(1), "payload")), input));
        LogicalWorkTableReference workTable = new LogicalWorkTableReference(
                new RelationId(2), cteId, anchor.getOutput(), ImmutableList.of("source"));
        List<Slot> previous = workTable.getOutput();
        Plan recursive = new LogicalRecursiveUnionProducer<>("source", new LogicalProject<>(ImmutableList.of(
                new Alias(previous.get(0), "a"),
                new Alias(previous.get(payloadSource), "payload")), workTable));
        Plan union = new LogicalRecursiveUnion<>(
                "source",
                Qualifier.ALL,
                ImmutableList.of(
                        new SlotReference("a", IntegerType.INSTANCE),
                        new SlotReference("payload", IntegerType.INSTANCE)),
                ImmutableList.of(slotReferences(anchor), slotReferences(recursive)),
                ImmutableList.of(anchor, recursive));
        return new LogicalProject<>(ImmutableList.of(
                new Alias(new IntegerLiteral(1), "id"), union.getOutput().get(1)), union);
    }

    /**
     * WITH c0 (v0 ... v{levels}), then c(i) = SELECT COALESCE(v0, v(i)) AS v0, v1 ... FROM c(i - 1) UNION ALL
     * SELECT v0, v1 ... FROM c(i - 1), and SELECT 1, v0 FROM c{levels}. The last column of c0 is {@code lastValue}
     * and the others a Variant literal.
     */
    private static String cteChainRequestingDoublingSets(int levels, String lastValue) {
        StringBuilder columns = new StringBuilder();
        for (int column = 1; column <= levels; column++) {
            columns.append(", v").append(column);
        }
        StringBuilder sql = new StringBuilder("WITH c0 AS (SELECT ");
        for (int column = 0; column < levels; column++) {
            sql.append("parse_to_variant('{\"k\":1}') AS v").append(column).append(", ");
        }
        sql.append(lastValue).append(" AS v").append(levels).append(')');
        for (int level = 1; level <= levels; level++) {
            sql.append(String.format(
                    ", c%d AS (SELECT COALESCE(v0, v%d) AS v0%s FROM c%d UNION ALL SELECT v0%s FROM c%d)",
                    level, level, columns, level - 1, columns, level - 1));
        }
        return sql.append(" SELECT 1, v0 FROM c").append(levels).toString();
    }

    private static List<SlotReference> slotReferences(Plan plan) {
        return plan.getOutput().stream().map(SlotReference.class::cast).collect(ImmutableList.toImmutableList());
    }

    private static LogicalUnion lossyUnionPlan() {
        LogicalProject<?> left = projectExpression(
                variant -> new Cast(variant, IntegerType.INSTANCE));
        LogicalOneRowRelation right = oneRow(new Alias(new IntegerLiteral(1), "payload"));
        SlotReference output = new SlotReference("payload", IntegerType.INSTANCE);
        return new LogicalUnion(
                Qualifier.ALL,
                ImmutableList.of(output),
                ImmutableList.of(
                        ImmutableList.of((SlotReference) left.getOutput().get(0)),
                        ImmutableList.of((SlotReference) right.getOutput().get(0))),
                ImmutableList.of(),
                false,
                ImmutableList.of(left, right));
    }

    private static LogicalProject<?> projectExpression(
            Function<Slot, Expression> expressionFactory) {
        LogicalOneRowRelation input = variantInput();
        return new LogicalProject<>(
                ImmutableList.of(new Alias(
                        expressionFactory.apply(input.getOutput().get(0)), "payload")),
                input);
    }

    private static LogicalOneRowRelation variantInput() {
        return oneRow(new Alias(
                new NullLiteral(ConnectorComputeVariantType.INSTANCE), "source_variant"));
    }

    private static LogicalOneRowRelation oneRow(NamedExpression... expressions) {
        return new LogicalOneRowRelation(
                new RelationId(1), ImmutableList.copyOf(expressions));
    }

    private static List<Column> variantTarget() {
        return ImmutableList.of(new Column(
                "payload", ConnectorComputeVariantType.INSTANCE.toCatalogDataType()));
    }

    private static List<Column> variantTargetWithId() {
        return ImmutableList.of(
                new Column("id", org.apache.doris.catalog.Type.INT),
                new Column("payload", ConnectorComputeVariantType.INSTANCE.toCatalogDataType()));
    }

    private static StructType flaggedType() {
        return new StructType(ImmutableList.of(
                new StructField("matched", BooleanType.INSTANCE, true, ""),
                new StructField("payload", ConnectorComputeVariantType.INSTANCE, true, "")));
    }

    private static List<Column> flaggedTarget() {
        return targetWithId("flagged", flaggedType());
    }

    private static List<Column> targetWithId(String name, DataType type) {
        return ImmutableList.of(
                new Column("id", org.apache.doris.catalog.Type.INT),
                new Column(name, type.toCatalogDataType()));
    }
}
