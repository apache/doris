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
import org.apache.doris.nereids.exceptions.AnalysisException;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.ConnectorComputeVariantType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

public class VariantWritePlanValidatorTest {

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
}
