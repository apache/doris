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
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    public void testMatchNoSlotReference() {
        // MATCH with no SlotReference in left operand should throw
        MatchAny matchAny = new MatchAny(new VarcharLiteral("collections"), new NullLiteral());
        ExpressionTranslator translator = ExpressionTranslator.INSTANCE;
        Assertions.assertThrows(AnalysisException.class, () -> translator.visitMatch(matchAny, null));
    }

    @Test
    public void testMatchOnSlotWithoutColumnMetadata() {
        // MATCH on an alias output slot (no originalColumn/originalTable) should NOT throw.
        // This happens when MATCH is inside an OR predicate, preventing pushdown through
        // the project, so MATCH references the alias output slot instead of the scan slot.
        SlotReference aliasSlot = new SlotReference("firstname", VarcharType.SYSTEM_DEFAULT, true, ImmutableList.of());
        MatchAny matchAny = new MatchAny(aliasSlot, new VarcharLiteral("hello"));
        ExpressionTranslator translator = ExpressionTranslator.INSTANCE;
        PlanTranslatorContext context = new PlanTranslatorContext();
        context.addExprIdSlotRefPair(aliasSlot.getExprId(), new SlotRef(Type.VARCHAR, true));

        Assertions.assertDoesNotThrow(() -> translator.visitMatch(matchAny, context));
    }

    @Test
    public void testMatchOnSlotWithoutColumnMetadataAndExplicitAnalyzer() {
        // MATCH with explicit analyzer on a slot without column metadata should throw,
        // because we cannot validate the analyzer against the inverted index.
        SlotReference aliasSlot = new SlotReference("firstname", VarcharType.SYSTEM_DEFAULT, true, ImmutableList.of());
        MatchAny matchAny = new MatchAny(aliasSlot, new VarcharLiteral("hello"), "standard");
        ExpressionTranslator translator = ExpressionTranslator.INSTANCE;
        PlanTranslatorContext context = new PlanTranslatorContext();
        context.addExprIdSlotRefPair(aliasSlot.getExprId(), new SlotRef(Type.VARCHAR, true));

        Assertions.assertThrows(AnalysisException.class, () -> translator.visitMatch(matchAny, context));
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
