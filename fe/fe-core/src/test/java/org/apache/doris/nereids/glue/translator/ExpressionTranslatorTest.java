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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
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
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.thrift.TExprNode;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class ExpressionTranslatorTest {

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
}
