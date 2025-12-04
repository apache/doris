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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class NestedCaseWhenCondToLiteralTest extends ExpressionRewriteTestHelper {

    @Test
    void testNestedCaseWhen() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NestedCaseWhenCondToLiteral.INSTANCE)
        ));

        assertRewriteAfterTypeCoercion(
                "case when a > 1 then 1"
                        + "    when a > 2 then"
                        + "               (case when a > 1 then 2"
                        + "                     when a > 2 then 3"
                        + "                     when a > 1 and a > 1 and a > 2 and a > 2 and a > 3 then 100"
                        + "                     when a > 3 then (case when a > 1 then 4"
                        + "                                           when a > 2 then 5"
                        + "                                           when a > 3 then 6"
                        + "                                      end)"
                        + "                     when a > 1 and a > 1 and a > 2 and a > 2 and a > 3 then 101"
                        + "               end)"
                        + "    when (case when a > 1 then a > 1"
                        + "               when a > 2 then a > 2"
                        + "               when a > 3 then a > 3"
                        + "               when a > 1 then a > 1"
                        + "          end) then 100"
                        + "    when a > 3 then 7"
                        + "    when a > 1 then 8"
                        + "    else (case when a > 1 then 9"
                        + "               when a > 2 then 10"
                        + "               when a > 3 then 11"
                        + "               when a > 4 then 12"
                        + "               else (case when a > 1 then 13"
                        + "                          when a > 2 then 14"
                        + "                          when a > 3 then 15"
                        + "                          when a > 4 then 16"
                        + "                          when a > 5 then (case when a > 1 then 17 when a > 5 then 18 end)"
                        + "                     end)"
                        + "          end)"
                        + " end",
                "case when a > 1 then 1"
                        + "    when a > 2 then"
                        + "               (case when false then 2"
                        + "                     when true then 3"
                        + "                     when false and false and true and true and a > 3 then 100"
                        + "                     when a > 3 then (case when false then 4"
                        + "                                           when true then 5"
                        + "                                           when true then 6"
                        + "                                      end)"
                        + "                     when false then 101"
                        + "               end)"
                        + "    when (case when false then a > 1"
                        + "               when false then a > 2"
                        + "               when a > 3 then a > 3"
                        + "               when false then a > 1"
                        + "          end) then 100"
                        + "    when a > 3 then 7"
                        + "    when false then 8"
                        + "    else (case when false then 9"
                        + "               when false then 10"
                        + "               when false then 11"
                        + "               when a > 4 then 12"
                        + "               else (case when false then 13"
                        + "                          when false then 14"
                        + "                          when false then 15"
                        + "                          when false then 16"
                        + "                          when a > 5 then (case when false then 17 when true then 18 end)"
                        + "                     end)"
                        + "          end)"
                        + " end"
        );
    }

    @Test
    void testNestedIf() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NestedCaseWhenCondToLiteral.INSTANCE)
        ));
        assertRewriteAfterTypeCoercion(
                "if("
                        + "      a > 1,"
                        + "      if("
                        + "              a > 1,"
                        + "              if("
                        + "                      a > 2,"
                        + "                      if(a > 2,a + 2,a + 3),"
                        + "                      if("
                        + "                                a > 1,"
                        + "                                if(a > 2,a + 3,a + 4),"
                        + "                                if(a > 2,a + 5,a + 6)"
                        + "                      )"
                        + "               ),"
                        + "               if(a > 1,a + 1,a + 2)"
                        + "       ),"
                        + "       if("
                        + "               a > 1,"
                        + "               a + 5,"
                        + "               if(a > 2,a + 6,a + 7)"
                        + "       )"
                        + ")",
                "if("
                        + "      a > 1,"
                        + "      if("
                        + "              true,"
                        + "              if("
                        + "                      a > 2,"
                        + "                      if(true,a + 2,a + 3),"
                        + "                      if("
                        + "                                true,"
                        + "                                if(false,a + 3,a + 4),"
                        + "                                if(false,a + 5,a + 6)"
                        + "                      )"
                        + "               ),"
                        + "               if(true,a + 1,a + 2)"
                        + "       ),"
                        + "       if("
                        + "               false,"
                        + "               a + 5,"
                        + "               if(a > 2,a + 6,a + 7)"
                        + "       )"
                        + ")"
        );
    }

    @Test
    void testNestedCaseWhenReplacer() {
        // case when a > 1 then 101
        //      when a > 2 then (case when a > 1 then 102
        //                            when a > 2 then 103
        //                            when a > 3 then 104
        //                            when a > 4 then 105
        //                       else 106
        //                       end)
        //      when a > 3 then  107
        //      when a > 4 then  108
        //      when a > 4 then  109
        //      else 110
        // end
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression c1 = new GreaterThan(a, IntegerLiteral.of(1));
        Expression c2 = new GreaterThan(a, IntegerLiteral.of(2));
        Expression c3 = new GreaterThan(a, IntegerLiteral.of(3));
        Expression c4 = new GreaterThan(a, IntegerLiteral.of(4));
        Expression i101 = IntegerLiteral.of(101);
        Expression i102 = IntegerLiteral.of(102);
        Expression i103 = IntegerLiteral.of(103);
        Expression i104 = IntegerLiteral.of(104);
        Expression i105 = IntegerLiteral.of(105);
        Expression i106 = IntegerLiteral.of(106);
        Expression i107 = IntegerLiteral.of(107);
        Expression i108 = IntegerLiteral.of(108);
        Expression i109 = IntegerLiteral.of(109);
        Expression i110 = IntegerLiteral.of(110);
        Expression innerCaseWhen = new CaseWhen(
                ImmutableList.of(
                        new WhenClause(c1, i102),
                        new WhenClause(c2, i103),
                        new WhenClause(c3, i104),
                        new WhenClause(c4, i105)),
                i106);
        Expression outerCaseWhen = new CaseWhen(
                ImmutableList.of(
                        new WhenClause(c1, i101),
                        new WhenClause(c2, innerCaseWhen),
                        new WhenClause(c3, i107),
                        new WhenClause(c4, i108),
                        new WhenClause(c4, i109)),
                i110);
        TestNestedCondReplacer replacer = new TestNestedCondReplacer();
        outerCaseWhen.accept(replacer, null);
        replacer.checkExpressionReplaceLiterals(outerCaseWhen,
                ImmutableList.of(),
                ImmutableList.of());
        replacer.checkExpressionReplaceLiterals(i101,
                ImmutableList.of(c1),
                ImmutableList.of());
        replacer.checkExpressionReplaceLiterals(i102,
                ImmutableList.of(c2),
                ImmutableList.of(c1));
        replacer.checkExpressionReplaceLiterals(i103,
                ImmutableList.of(c2),
                ImmutableList.of(c1));
        replacer.checkExpressionReplaceLiterals(i104,
                ImmutableList.of(c2, c3),
                ImmutableList.of(c1));
        replacer.checkExpressionReplaceLiterals(i105,
                ImmutableList.of(c2, c4),
                ImmutableList.of(c1, c3));
        replacer.checkExpressionReplaceLiterals(i106,
                ImmutableList.of(c2),
                ImmutableList.of(c1, c3, c4));
        replacer.checkExpressionReplaceLiterals(i107,
                ImmutableList.of(c3),
                ImmutableList.of(c1, c2));
        replacer.checkExpressionReplaceLiterals(i108,
                ImmutableList.of(c4),
                ImmutableList.of(c1, c2, c3));
        replacer.checkExpressionReplaceLiterals(i109,
                ImmutableList.of(),
                ImmutableList.of(c1, c2, c3, c4));
        replacer.checkExpressionReplaceLiterals(i110,
                ImmutableList.of(),
                ImmutableList.of(c1, c2, c3, c4));

        // after rewrite, the condition literals should clear
        Assertions.assertEquals(Maps.newHashMap(), replacer.conditionLiterals);
    }

    @Test
    void testNestedIfReplacer() {
        // if(a > 1,
        //      if(a > 2,
        //              if(a > 3, 301, 302),
        //              if(a > 4, 303, 304)
        //         ),
        //      if(a > 5, 305, 306)
        // )
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression c1 = new GreaterThan(a, IntegerLiteral.of(1));
        Expression c2 = new GreaterThan(a, IntegerLiteral.of(2));
        Expression c3 = new GreaterThan(a, IntegerLiteral.of(3));
        Expression c4 = new GreaterThan(a, IntegerLiteral.of(4));
        Expression c5 = new GreaterThan(a, IntegerLiteral.of(5));
        Expression i301 = IntegerLiteral.of(301);
        Expression i302 = IntegerLiteral.of(302);
        Expression i303 = IntegerLiteral.of(303);
        Expression i304 = IntegerLiteral.of(304);
        Expression i305 = IntegerLiteral.of(305);
        Expression i306 = IntegerLiteral.of(306);
        Expression innerIf1 = new If(c3, i301, i302);
        Expression innerIf2 = new If(c4, i303, i304);
        Expression innerIf = new If(c2, innerIf1, innerIf2);
        Expression outerIf = new If(c1, innerIf, new If(c5, i305, i306));
        TestNestedCondReplacer replacer = new TestNestedCondReplacer();
        outerIf.accept(replacer, null);
        replacer.checkExpressionReplaceLiterals(outerIf,
                ImmutableList.of(),
                ImmutableList.of());
        replacer.checkExpressionReplaceLiterals(innerIf,
                ImmutableList.of(c1),
                ImmutableList.of());
        replacer.checkExpressionReplaceLiterals(innerIf1,
                ImmutableList.of(c1, c2),
                ImmutableList.of());
        replacer.checkExpressionReplaceLiterals(i301,
                ImmutableList.of(c1, c2, c3),
                ImmutableList.of());
        replacer.checkExpressionReplaceLiterals(i302,
                ImmutableList.of(c1, c2),
                ImmutableList.of(c3));
        replacer.checkExpressionReplaceLiterals(innerIf2,
                ImmutableList.of(c1),
                ImmutableList.of(c2));

        // after rewrite, the condition literals should clear
        Assertions.assertEquals(Maps.newHashMap(), replacer.conditionLiterals);
    }

    private static class TestNestedCondReplacer extends NestedCaseWhenCondToLiteral.NestedCondReplacer {
        private final Map<Expression, Map<Expression, BooleanLiteral>> expressionReplaceMap = Maps.newHashMap();

        @Override
        public Expression visit(Expression expr, Void context) {
            recordReplaceLiteral(expr);
            return super.visit(expr, context);
        }

        @Override
        public Expression visitCaseWhen(CaseWhen caseWhen, Void context) {
            recordReplaceLiteral(caseWhen);
            return super.visitCaseWhen(caseWhen, context);
        }

        @Override
        public Expression visitIf(If ifExpr, Void context) {
            recordReplaceLiteral(ifExpr);
            return super.visitIf(ifExpr, context);
        }

        private void recordReplaceLiteral(Expression expr) {
            expressionReplaceMap.put(expr, Maps.newHashMap(conditionLiterals));
        }

        private void checkExpressionReplaceLiterals(Expression expression,
                List<Expression> trueConditions, List<Expression> falseConditions) {
            Map<Expression, BooleanLiteral> expectedReplaceMap = Maps.newHashMap();
            for (Expression trueCondition : trueConditions) {
                expectedReplaceMap.put(trueCondition, BooleanLiteral.TRUE);
            }
            for (Expression falseCondition : falseConditions) {
                expectedReplaceMap.put(falseCondition, BooleanLiteral.FALSE);
            }
            Assertions.assertEquals(expectedReplaceMap, expressionReplaceMap.get(expression));
        }
    }
}
