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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Type;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ShortCircuitIf;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExpressionTranslatorTest {

    @Test
    void testStringCaseTranslationPreservesCommonInput() {
        withSimpleCaseEnabled(this::checkStringCaseTranslation);
    }

    private void checkStringCaseTranslation() {
        for (DataType type : ImmutableList.of(StringType.INSTANCE, VarcharType.createVarcharType(20))) {
            SlotReference input = new SlotReference("input", type, true);
            PlanTranslatorContext context = new PlanTranslatorContext();
            context.addExprIdSlotRefPair(input.getExprId(), new SlotRef(type.toCatalogDataType(), true));
            Literal key = new VarcharLiteral("key");
            CaseWhen expression = new CaseWhen(ImmutableList.of(
                    new WhenClause(new EqualTo(input, key), new IntegerLiteral(11)),
                    new WhenClause(new EqualTo(input, key), new IntegerLiteral(22))),
                    new IntegerLiteral(33));
            CaseExpr translated = (CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(expression, context);
            Assertions.assertTrue(ExprToThriftVisitor.treeToThrift(translated).getNodes().get(0)
                    .getCaseExpr().isHasCaseExpr());
            Assertions.assertInstanceOf(SlotRef.class, translated.getChild(0));
            Assertions.assertEquals(input.getDataType().toCatalogDataType(), translated.getChild(0).getType());
            Assertions.assertEquals(11, ((IntLiteral) translated.getChild(2)).getLongValue());
            Assertions.assertEquals(22, ((IntLiteral) translated.getChild(4)).getLongValue());

            CaseWhen nullKey = new CaseWhen(ImmutableList.of(new WhenClause(
                    new EqualTo(input, new NullLiteral(type)), new IntegerLiteral(1))));
            Assertions.assertFalse(((CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(nullKey, context))
                    .isHasCaseExpr());
        }
    }

    @Test
    void testDecimalCaseTranslationPreservesCastAndScale() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext connection = new ConnectContext();
        connection.getSessionVariable().enableDecimal256 = true;
        connection.getSessionVariable().enableExperimentalSimpleCase = true;
        connection.setThreadLocalInfo();
        try {
            SlotReference input = new SlotReference("input", StringType.INSTANCE, true);
            PlanTranslatorContext context = new PlanTranslatorContext();
            context.addExprIdSlotRefPair(input.getExprId(), new SlotRef(Type.STRING, true));
            for (int precision : new int[] {9, 18, 38, 76}) {
                DecimalV3Type type = DecimalV3Type.createDecimalV3TypeNotCheck256(precision, 6);
                Cast cast = new Cast(input, type);
                DecimalV3Literal key = new DecimalV3Literal(type, new BigDecimal("1.000000"));
                CaseWhen expression = new CaseWhen(ImmutableList.of(
                        new WhenClause(new EqualTo(cast, key), new IntegerLiteral(11)),
                        new WhenClause(new EqualTo(new Cast(input, type), key), new IntegerLiteral(22))),
                        new IntegerLiteral(33));
                CaseExpr translated = (CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(expression, context);
                Assertions.assertTrue(translated.isHasCaseExpr());
                Assertions.assertInstanceOf(org.apache.doris.analysis.CastExpr.class, translated.getChild(0));
                Assertions.assertEquals(type.toCatalogDataType(), translated.getChild(0).getType());
                Assertions.assertEquals(translated.getChild(0).getType(), translated.getChild(1).getType());

                DecimalV3Type otherScale = DecimalV3Type.createDecimalV3TypeNotCheck256(precision, 5);
                CaseWhen mismatch = new CaseWhen(ImmutableList.of(new WhenClause(new EqualTo(cast,
                        new DecimalV3Literal(otherScale, BigDecimal.ONE)), new IntegerLiteral(11))));
                Assertions.assertFalse(((CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(mismatch, context))
                        .isHasCaseExpr());
            }
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void testIntegerCaseTranslationUsesCommonInput() {
        withSimpleCaseEnabled(this::checkIntegerCaseTranslation);
    }

    private void withSimpleCaseEnabled(Runnable test) {
        ConnectContext previous = ConnectContext.get();
        ConnectContext connection = new ConnectContext();
        connection.getSessionVariable().enableExperimentalSimpleCase = true;
        connection.getSessionVariable().shortCircuitEvaluation = false;
        connection.setThreadLocalInfo();
        try {
            test.run();
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void testSimpleCaseFallsBackForShortCircuitEvaluation() {
        withSimpleCaseEnabled(() -> {
            ConnectContext.get().getSessionVariable().shortCircuitEvaluation = true;
            SlotReference input = new SlotReference("input", BigIntType.INSTANCE, true);
            PlanTranslatorContext context = new PlanTranslatorContext();
            context.addExprIdSlotRefPair(input.getExprId(), new SlotRef(Type.BIGINT, true));
            CaseWhen expression = new CaseWhen(ImmutableList.of(
                    new WhenClause(new EqualTo(input, new BigIntLiteral(1)), new IntegerLiteral(10)),
                    new WhenClause(new EqualTo(input, new BigIntLiteral(2)), new IntegerLiteral(20))),
                    new IntegerLiteral(30));
            CaseExpr translated = (CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(expression, context);
            TExprNode node = ExprToThriftVisitor.treeToThrift(translated).getNodes().get(0);
            Assertions.assertFalse(node.getCaseExpr().isHasCaseExpr());
            Assertions.assertTrue(node.isShortCircuitEvaluation());
            Assertions.assertEquals(5, translated.getChildren().size());
            Assertions.assertInstanceOf(org.apache.doris.analysis.BinaryPredicate.class, translated.getChild(0));
        });
    }

    @Test
    void testSimpleCaseBranchLimitAndFallbacks() {
        withSimpleCaseEnabled(() -> {
            SlotReference input = new SlotReference("input", IntegerType.INSTANCE, true);
            PlanTranslatorContext context = new PlanTranslatorContext();
            context.addExprIdSlotRefPair(input.getExprId(), new SlotRef(Type.INT, true));
            List<WhenClause> clauses = new ArrayList<>();
            for (int i = 0; i < 256; ++i) {
                clauses.add(new WhenClause(new EqualTo(input, new IntegerLiteral(i)), new IntegerLiteral(i)));
            }
            CaseExpr translated = (CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(
                    new CaseWhen(clauses), context);
            Assertions.assertTrue(translated.isHasCaseExpr());
            Assertions.assertFalse(translated.isHasElseExpr());
            Assertions.assertEquals(513, translated.getChildren().size());
            clauses.add(new WhenClause(new EqualTo(input, new IntegerLiteral(256)), new IntegerLiteral(256)));
            Assertions.assertFalse(((CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(
                    new CaseWhen(clauses), context)).isHasCaseExpr());

            // A slot key and a non-equality condition must keep searched CASE's layout.
            for (WhenClause clause : ImmutableList.of(
                    new WhenClause(new EqualTo(input, input), new IntegerLiteral(1)),
                    new WhenClause(BooleanLiteral.TRUE, new IntegerLiteral(1)),
                    new WhenClause(new EqualTo(input, new NullLiteral(IntegerType.INSTANCE)), new IntegerLiteral(1)),
                    new WhenClause(new EqualTo(input, new BigIntLiteral(1)), new IntegerLiteral(1)))) {
                Assertions.assertFalse(((CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(
                        new CaseWhen(ImmutableList.of(clause)), context)).isHasCaseExpr());
            }
        });
    }

    @Test
    void testSimpleCaseDisabledAndMissingContext() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext connection = new ConnectContext();
        try {
            SlotReference input = new SlotReference("input", BigIntType.INSTANCE, true);
            PlanTranslatorContext context = new PlanTranslatorContext();
            context.addExprIdSlotRefPair(input.getExprId(), new SlotRef(Type.BIGINT, true));
            CaseWhen expression = new CaseWhen(ImmutableList.of(new WhenClause(
                    new EqualTo(input, new BigIntLiteral(1)), new IntegerLiteral(10))));
            connection.setThreadLocalInfo();
            Assertions.assertFalse(connection.getSessionVariable().enableExperimentalSimpleCase);
            Assertions.assertFalse(((CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(expression, context))
                    .isHasCaseExpr());
            connection.getSessionVariable().enableExperimentalSimpleCase = true;
            Assertions.assertTrue(((CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(expression, context))
                    .isHasCaseExpr());
            ConnectContext.remove();
            Assertions.assertFalse(((CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(expression, context))
                    .isHasCaseExpr());
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    private void checkIntegerCaseTranslation() {
        SlotReference input = new SlotReference("input", BigIntType.INSTANCE, true);
        PlanTranslatorContext context = new PlanTranslatorContext();
        context.addExprIdSlotRefPair(input.getExprId(), new SlotRef(Type.BIGINT, true));
        CaseWhen expression = new CaseWhen(ImmutableList.of(
                new WhenClause(new EqualTo(input, new BigIntLiteral(1)), new VarcharLiteral("first")),
                new WhenClause(new EqualTo(input, new BigIntLiteral(1)), new VarcharLiteral("duplicate"))),
                new VarcharLiteral("default"));
        CaseExpr translated = (CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(expression, context);
        TExprNode node = ExprToThriftVisitor.treeToThrift(translated).getNodes().get(0);
        Assertions.assertTrue(node.getCaseExpr().isHasCaseExpr());
        Assertions.assertTrue(node.getCaseExpr().isHasElseExpr());
        Assertions.assertEquals(6, translated.getChildren().size());
        Assertions.assertEquals("first", ((org.apache.doris.analysis.StringLiteral)
                translated.getChild(2)).getStringValue());
        Assertions.assertTrue(((CaseExpr) translated.clone()).isHasCaseExpr());

        SlotReference other = new SlotReference("other", BigIntType.INSTANCE, true);
        context.addExprIdSlotRefPair(other.getExprId(), new SlotRef(Type.BIGINT, true));
        CaseWhen differentInputs = new CaseWhen(ImmutableList.of(
                new WhenClause(new EqualTo(input, new BigIntLiteral(1)), new IntegerLiteral(10)),
                new WhenClause(new EqualTo(other, new BigIntLiteral(2)), new IntegerLiteral(20))));
        Assertions.assertFalse(((CaseExpr) ExpressionTranslator.INSTANCE.visitCaseWhen(differentInputs, context))
                .isHasCaseExpr());
    }

    @Test
    public void testUnaryArithmetic() throws Exception {
        BitNot bitNot = new BitNot(new IntegerLiteral(1));
        ExpressionTranslator translator = ExpressionTranslator.INSTANCE;
        Expr actual = translator.visitUnaryArithmetic(bitNot, null);
        Expr expected = new ArithmeticExpr(Operator.BITNOT,
                new IntLiteral(1, Type.INT), null, Type.INT, NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testMatch() {
        MatchAny matchAny = new MatchAny(new VarcharLiteral("collections"), new NullLiteral());
        ExpressionTranslator translator = ExpressionTranslator.INSTANCE;
        Assertions.assertThrows(AnalysisException.class, () -> translator.visitMatch(matchAny, null));
    }

    @Test
    public void testMatchTranslationPreservesResolvedPolicyNames() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                50, "IK", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                51, "Legacy", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (Map.Entry<String, String> binding : Map.of(
                    "IK", "IK", "ik", "ik", "StAnDaRd", "standard", "LEGACY", "Legacy").entrySet()) {
                SlotReference slot = new SlotReference("content", StringType.INSTANCE, true);
                PlanTranslatorContext context = new PlanTranslatorContext();
                context.addExprIdSlotRefPair(slot.getExprId(), new SlotRef(Type.STRING, true));
                MatchAny match = new MatchAny(slot, new VarcharLiteral("abc def"), binding.getKey());
                MatchPredicate predicate = Assertions.assertInstanceOf(MatchPredicate.class,
                        ExpressionTranslator.INSTANCE.visitMatch(match, context));
                TExprNode node = new TExprNode();
                ExprToThriftVisitor.INSTANCE.visitMatchPredicate(predicate, node);
                Assertions.assertEquals(binding.getValue(), node.getMatchPredicate().getAnalyzerName());
            }
        }
    }

    @Test void testFlattenAndOrNullable() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, false);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, false);
        Or or = new Or(ImmutableList.of(a, b, c));
        ExpressionTranslator translator = ExpressionTranslator.INSTANCE;
        PlanTranslatorContext context = new PlanTranslatorContext();

        context.addExprIdSlotRefPair(a.getExprId(), new SlotRef(Type.VARCHAR, true));
        context.addExprIdSlotRefPair(b.getExprId(), new SlotRef(Type.VARCHAR, false));
        context.addExprIdSlotRefPair(c.getExprId(), new SlotRef(Type.VARCHAR, false));

        Expr actual = translator.visitOr(or, context);
        Assertions.assertTrue(actual.isNullable());
    }

    @Test
    void testRequiredShortCircuitEvaluationSurvivesTranslation() {
        ShortCircuitIf expression = new ShortCircuitIf(
                BooleanLiteral.TRUE, new IntegerLiteral(1), new IntegerLiteral(2));

        FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(
                expression, new PlanTranslatorContext());
        TExprNode thriftNode = ExprToThriftVisitor.treeToThrift(translated).getNodes().get(0);

        Assertions.assertTrue(translated.isShortCircuitEvaluation());
        Assertions.assertTrue(thriftNode.isSetShortCircuitEvaluation());
        Assertions.assertTrue(thriftNode.isShortCircuitEvaluation());
    }
}
