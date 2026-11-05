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
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.SearchDslParser;
import org.apache.doris.analysis.SearchPredicate;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PatternType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantField;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ShortCircuitIf;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.thrift.TExprNode;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
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

    @Test
    public void testVariantIndexLookupUsesLogicalKeysForMatchAndSearch() {
        for (boolean escaped : new boolean[] {false, true}) {
            VariantType type = new VariantType(ImmutableList.of(), 0, false, 10000, 1,
                    false, 0, 64, false, escaped);
            Column column = new Column("v", type.toCatalogDataType());
            OlapTable table = Mockito.mock(OlapTable.class);
            List<String> logical = ImmutableList.of("a.b", "50%");
            SlotReference seed = new SlotReference("v", StringType.INSTANCE, true);
            SlotReference slot = new SlotReference(seed.getExprId(), "v", StringType.INSTANCE, true,
                    ImmutableList.of(), table, column, table, column, logical);
            PlanTranslatorContext context = new PlanTranslatorContext();
            context.addExprIdSlotRefPair(slot.getExprId(), new SlotRef(Type.STRING, true));
            Assertions.assertNotNull(ExpressionTranslator.INSTANCE.visitMatch(
                    new MatchAny(slot, new VarcharLiteral("abc")), context));
            Mockito.verify(table).getInvertedIndex(column, logical, null);
            SearchExpression search = new SearchExpression("v.a%2Eb:abc",
                    SearchDslParser.parseDsl("v.a%2Eb:abc"), ImmutableList.of(slot));
            Assertions.assertNotNull(ExpressionTranslator.INSTANCE.visitSearchExpression(search, context));
            Mockito.verify(table).getInvertedIndex(column, logical);
        }
    }

    @Test
    public void testVariantSearchFallbackAndDirectLookupChooseTheSameIndex() {
        List<List<String>> paths = List.of(List.of("a.b"), List.of("a", "b"),
                List.of("a%2Eb"), List.of("50%"));
        List<String> storagePaths = List.of("a%2Eb", "a.b", "a%252Eb", "50%25");
        ArrayList<VariantField> fields = new ArrayList<>();
        List<Index> indexes = new ArrayList<>();
        for (int i = 0; i < storagePaths.size(); ++i) {
            fields.add(new VariantField(storagePaths.get(i), Type.STRING, "", PatternType.MATCH_NAME));
            indexes.add(new Index(i + 1, "idx_" + i, List.of("v"), IndexType.INVERTED,
                    Map.of("field_pattern", storagePaths.get(i), "parser", "english"), ""));
        }
        Column column = new Column("v", new org.apache.doris.catalog.VariantType(fields,
                Map.of("variant_escape_dots_in_keys", "true")));
        OlapTable table = Mockito.spy(new OlapTable());
        table.setIndexes(indexes);
        for (int i = 0; i < paths.size(); ++i) {
            PlanTranslatorContext context = new PlanTranslatorContext();
            TupleDescriptor tuple = context.generateTupleDesc();
            tuple.setTable(table);
            SlotReference seed = new SlotReference("v", StringType.INSTANCE, true);
            SlotReference directSlot = new SlotReference(seed.getExprId(), "v", StringType.INSTANCE, true,
                    ImmutableList.of(), table, column, table, column, paths.get(i));
            SlotDescriptor descriptor = context.createSlotDesc(tuple, directSlot);
            // CTE/projection remapping can lose original metadata while its translated slot still has it.
            SlotReference remappedSlot = new SlotReference("v", StringType.INSTANCE, true);
            context.addExprIdSlotRefPair(remappedSlot.getExprId(), new SlotRef(descriptor));
            for (SlotReference slot : List.of(directSlot, remappedSlot)) {
                SearchExpression search = new SearchExpression("v.a:abc",
                        SearchDslParser.parseDsl("v.a:abc"), List.of(slot));
                SearchPredicate predicate = (SearchPredicate) ExpressionTranslator.INSTANCE
                        .visitSearchExpression(search, context);
                Assertions.assertSame(indexes.get(i), predicate.getFieldIndexes().get(0));
            }
            Mockito.verify(table, Mockito.times(2)).getInvertedIndex(column, paths.get(i));
        }
    }
}
