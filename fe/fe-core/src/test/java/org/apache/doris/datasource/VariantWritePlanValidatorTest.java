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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.Function;

public class VariantWritePlanValidatorTest {

    @Test
    public void testUnionIfAndCaseImplicitVariantLossAreRejectedForBothSinks() {
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

        for (String sinkName : ImmutableList.of("Iceberg", "Paimon")) {
            for (Plan plan : lossyPlans) {
                AnalysisException exception = Assert.assertThrows(
                        AnalysisException.class,
                        () -> VariantWritePlanValidator.validateNoLossyCoercion(
                                sinkName, variantTarget(), plan));
                Assert.assertTrue(exception.getMessage(),
                        exception.getMessage().contains(sinkName + " VARIANT write"));
                Assert.assertTrue(exception.getMessage(),
                        exception.getMessage().contains("input column 'payload'"));
                Assert.assertTrue(exception.getMessage(),
                        exception.getMessage().contains("implicitly casts VARIANT to INT"));
            }
        }
    }

    @Test
    public void testAnalyzedUnionIfAndCaseObjectAndArrayPlansAreRejected() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().enableVariantV2 = true;
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

        try {
            for (String sql : sqlStatements) {
                Plan analyzedPlan = PlanChecker.from(connectContext).analyze(sql).getPlan();
                for (String sinkName : ImmutableList.of("Iceberg", "Paimon")) {
                    AnalysisException exception = Assert.assertThrows(
                            AnalysisException.class,
                            () -> VariantWritePlanValidator.validateNoLossyCoercion(
                                    sinkName, variantTargetWithId(), analyzedPlan));
                    Assert.assertTrue(exception.getMessage(),
                            exception.getMessage().contains("input column 'payload'"));
                }
            }
        } finally {
            connectContext.getSessionVariable().enableVariantV2 = false;
        }
    }

    @Test
    public void testAnalyzedExplicitVariantCastIsAccepted() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().enableVariantV2 = true;
        try {
            Plan analyzedPlan = PlanChecker.from(connectContext)
                    .analyze("SELECT 1, CAST(parse_to_variant('{\"explicit\":true}') AS INT)")
                    .getPlan();
            VariantWritePlanValidator.validateNoLossyCoercion(
                    "Iceberg", variantTargetWithId(), analyzedPlan);
            VariantWritePlanValidator.validateNoLossyCoercion(
                    "Paimon", variantTargetWithId(), analyzedPlan);
        } finally {
            connectContext.getSessionVariable().enableVariantV2 = false;
        }
    }

    @Test
    public void testAnalyzedCteProducerLossIsRejected() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().enableVariantV2 = true;
        try {
            Plan analyzedPlan = PlanChecker.from(connectContext).analyze(
                    "WITH source AS (SELECT CASE WHEN TRUE "
                            + "THEN parse_to_variant('{\"cte\":true}') ELSE 1 END AS payload) "
                            + "SELECT 1, payload FROM source").getPlan();
            for (String sinkName : ImmutableList.of("Iceberg", "Paimon")) {
                AnalysisException exception = Assert.assertThrows(
                        AnalysisException.class,
                        () -> VariantWritePlanValidator.validateNoLossyCoercion(
                                sinkName, variantTargetWithId(), analyzedPlan));
                Assert.assertTrue(exception.getMessage(),
                        exception.getMessage().contains("input column 'payload'"));
            }
        } finally {
            connectContext.getSessionVariable().enableVariantV2 = false;
        }
    }

    @Test
    public void testPrimitiveSourceAndExplicitVariantCastAreAccepted() {
        LogicalOneRowRelation primitiveSource = oneRow(
                new Alias(new IntegerLiteral(1), "payload"));
        VariantWritePlanValidator.validateNoLossyCoercion(
                "Iceberg", variantTarget(), primitiveSource);
        VariantWritePlanValidator.validateNoLossyCoercion(
                "Paimon", variantTarget(), primitiveSource);

        LogicalProject<?> explicitCastSource = projectExpression(
                variant -> new Cast(variant, IntegerType.INSTANCE, true));
        VariantWritePlanValidator.validateNoLossyCoercion(
                "Iceberg", variantTarget(), explicitCastSource);
        VariantWritePlanValidator.validateNoLossyCoercion(
                "Paimon", variantTarget(), explicitCastSource);

        LogicalOneRowRelation arrayVariantInput = oneRow(new Alias(
                new NullLiteral(ArrayType.of(VariantType.COMPUTE_V2_INSTANCE)), "source_array"));
        LogicalProject<?> arrayToVariantSource = new LogicalProject<>(ImmutableList.of(
                new Alias(new Cast(
                        arrayVariantInput.getOutput().get(0),
                        VariantType.COMPUTE_V2_INSTANCE), "payload")),
                arrayVariantInput);
        VariantWritePlanValidator.validateNoLossyCoercion(
                "Iceberg", variantTarget(), arrayToVariantSource);
        VariantWritePlanValidator.validateNoLossyCoercion(
                "Paimon", variantTarget(), arrayToVariantSource);
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
                new Column("payload", VariantType.COMPUTE_V2_INSTANCE.toCatalogDataType()));

        VariantWritePlanValidator.validateNoLossyCoercion("Iceberg", targets, source);
        VariantWritePlanValidator.validateNoLossyCoercion("Paimon", targets, source);
    }

    @Test
    public void testLossyCastInNestedVariantTargetIsRejected() {
        ArrayType sourceType = ArrayType.of(VariantType.COMPUTE_V2_INSTANCE);
        LogicalOneRowRelation input = oneRow(new Alias(
                new NullLiteral(sourceType), "source_variants"));
        LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                new Alias(new Cast(
                        input.getOutput().get(0), ArrayType.of(IntegerType.INSTANCE)), "payloads")),
                input);
        List<Column> targets = ImmutableList.of(new Column(
                "payloads", sourceType.toCatalogDataType()));

        for (String sinkName : ImmutableList.of("Iceberg", "Paimon")) {
            AnalysisException exception = Assert.assertThrows(
                    AnalysisException.class,
                    () -> VariantWritePlanValidator.validateNoLossyCoercion(
                            sinkName, targets, source));
            Assert.assertTrue(exception.getMessage(),
                    exception.getMessage().contains("implicitly casts VARIANT to ARRAY<INT>"));
        }
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
                new NullLiteral(VariantType.COMPUTE_V2_INSTANCE), "source_variant"));
    }

    private static LogicalOneRowRelation oneRow(NamedExpression... expressions) {
        return new LogicalOneRowRelation(
                new RelationId(1), ImmutableList.copyOf(expressions));
    }

    private static List<Column> variantTarget() {
        return ImmutableList.of(new Column(
                "payload", VariantType.COMPUTE_V2_INSTANCE.toCatalogDataType()));
    }

    private static List<Column> variantTargetWithId() {
        return ImmutableList.of(
                new Column("id", org.apache.doris.catalog.Type.INT),
                new Column("payload", VariantType.COMPUTE_V2_INSTANCE.toCatalogDataType()));
    }
}
