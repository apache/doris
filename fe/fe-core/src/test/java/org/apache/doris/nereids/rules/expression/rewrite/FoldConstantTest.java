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

package org.apache.doris.nereids.rules.expression.rewrite;

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.rewrite.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.rules.expression.rewrite.rules.TypeCoercion;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntervalLiteral.TimeUnit;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class FoldConstantTest {

    private static final NereidsParser PARSER = new NereidsParser();
    private ExpressionRuleExecutor executor;

    @Test
    public void testCaseWhenFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewrite("case when 1 = 2 then 1 when '1' < 2 then 2 else 3 end", "2");
        assertRewrite("case when 1 = 2 then 1 when '1' > 2 then 2 end", "null");
        assertRewrite("case when (1 + 5) / 2 > 2 then 4  when '1' < 2 then 2 else 3 end", "4");
        assertRewrite("case when not 1 = 2 then 1 when '1' > 2 then 2 end", "1");
        assertRewrite("case when 1 = 2 then 1 when 3 in ('1',2 + 8 / 2,3,4) then 2 end", "2");
        assertRewrite("case when TA = 2 then 1 when 3 in ('1',2 + 8 / 2,3,4) then 2 end", "CASE  WHEN (TA = 2) THEN 1 ELSE 2 END");
        assertRewrite("case when TA = 2 then 5 when 3 in (2,3,4) then 2 else 4 end", "CASE  WHEN (TA = 2) THEN 5 ELSE 2 END");
        assertRewrite("case when TA = 2 then 1 when TB in (2,3,4) then 2 else 4 end", "CASE  WHEN (TA = 2) THEN 1 WHEN TB IN (2, 3, 4) THEN 2 ELSE 4 END");
        assertRewrite("case when null = 2 then 1 when 3 in (2,3,4) then 2 else 4 end", "2");
        assertRewrite("case when null = 2 then 1 else 4 end", "4");
        assertRewrite("case when null = 2 then 1 end", "null");
        assertRewrite("case when TA = TB then 1 when TC is null then 2 end", "CASE  WHEN (TA = TB) THEN 1 WHEN TC IS NULL THEN 2 ELSE NULL END");
    }

    @Test
    public void testInFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewrite("1 in (1,2,3,4)", "true");
        // Type Coercion trans all to string.
        assertRewrite("3 in ('1',2 + 8 / 2,3,4)", "true");
        assertRewrite("4 / 2 * 1 - (5/2) in ('1',2 + 8 / 2,3,4)", "false");
        assertRewrite("null in ('1',2 + 8 / 2,3,4)", "null");
        assertRewrite("3 in ('1',null,3,4)", "true");
        assertRewrite("TA in (1,null,3,4)", "TA in (1, null, 3, 4)");
        assertRewrite("IA in (IB,IC,null)", "IA in (IB,IC,null)");
    }

    @Test
    public void testLogicalFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewrite("10 + 1 > 1 and 1 > 2", "false");
        assertRewrite("10 + 1 > 1 and 1 < 2", "true");
        assertRewrite("null + 1 > 1 and 1 < 2", "null");
        assertRewrite("10 < 3 and 1 > 2", "false");
        assertRewrite("6 / 2 - 10 * (6 + 1) > 2 and 10 > 3 and 1 > 2", "false");

        assertRewrite("10 + 1 > 1 or 1 > 2", "true");
        assertRewrite("null + 1 > 1 or 1 > 2", "null");
        assertRewrite("6 / 2 - 10 * (6 + 1) > 2 or 10 > 3 or 1 > 2", "true");

        assertRewrite("(1 > 5 and 8 < 10 or 1 = 3) or (1 > 8 + 9 / (10 * 2) or ( 10 = 3))", "false");
        assertRewrite("(TA > 1 and 8 < 10 or 1 = 3) or (1 > 3 or ( 10 = 3))", "TA > 1");

        assertRewrite("false or false", "false");
        assertRewrite("false or true", "true");
        assertRewrite("true or false", "true");
        assertRewrite("true or true", "true");

        assertRewrite("true and true", "true");
        assertRewrite("false and true", "false");
        assertRewrite("true and false", "false");
        assertRewrite("false and false", "false");

        assertRewrite("true and null", "null");
        assertRewrite("false and null", "false");
        assertRewrite("true or null", "true");
        assertRewrite("false or null", "null");

        assertRewrite("null and null", "null");
    }

    @Test
    public void testIsNullFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewrite("100 is null", "false");
        assertRewrite("null is null", "true");
        assertRewrite("null is not null", "false");
        assertRewrite("100 is not null", "true");
        assertRewrite("IA is not null", "IA is not null");
        assertRewrite("IA is null", "IA is null");
    }

    @Test
    public void testNotFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewrite("not 1 > 2", "true");
        assertRewrite("not null + 1 > 2", "null");
        assertRewrite("not (1 + 5) / 2 + (10 - 1) * 3 > 3 * 5 + 1", "false");
    }

    @Test
    public void testCastFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));

        // cast '1' as tinyint
        Cast c = new Cast(Literal.of("1"), TinyIntType.INSTANCE);
        Expression rewritten = executor.rewrite(c);
        Literal expected = Literal.of((byte) 1);
        Assertions.assertEquals(rewritten, expected);
    }

    @Test
    public void testCompareFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewrite("'1' = 2", "false");
        assertRewrite("1 = 2", "false");
        assertRewrite("1 != 2", "true");
        assertRewrite("2 > 2", "false");
        assertRewrite("3 * 10 + 1 / 2 >= 2", "true");
        assertRewrite("3 < 2", "false");
        assertRewrite("3 <= 2", "false");
        assertRewrite("3 <= null", "null");
        assertRewrite("3 >= null", "null");
        assertRewrite("null <=> null", "true");
        assertRewrite("2 <=> null", "false");
        assertRewrite("2 <=> 2", "true");
    }

    @Test
    public void testArithmeticFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewrite("1 + 1", Literal.of((short) 2));
        assertRewrite("1 - 1", Literal.of((short) 0));
        assertRewrite("100 + 100", Literal.of((short) 200));
        assertRewrite("1 - 2", Literal.of((short) -1));
        assertRewrite("1 - 2 > 1", "false");
        assertRewrite("1 - 2 + 1 > 1 + 1 - 100", "true");
        assertRewrite("10 * 2 / 1 + 1 > (1 + 1) - 100", "true");

        // a + 1 > 2
        Slot a = SlotReference.of("a", IntegerType.INSTANCE);
        Expression e1 = new Add(a, Literal.of(1L));
        Expression e2 = new Add(new Cast(a, BigIntType.INSTANCE), Literal.of(1L));
        assertRewrite(e1, e2);

        // a > (1 + 10) / 2 * (10 + 1)
        Expression e3 = PARSER.parseExpression("(1 + 10) / 2 * (10 + 1)");
        Expression e4 = new GreaterThan(a, e3);
        Expression e5 = new GreaterThan(new Cast(a, DoubleType.INSTANCE), Literal.of(60.5D));
        assertRewrite(e4, e5);

        // a > 1
        Expression e6 = new GreaterThan(a, Literal.of(1));
        assertRewrite(e6, e6);
        assertRewrite(a, a);

        // a
        assertRewrite(a, a);

        // 1
        Literal one = Literal.of(1);
        assertRewrite(one, one);
    }

    @Test
    public void testTimestampFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        String interval = "'1991-05-01' - interval 1 day";
        Expression e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        Expression e8 = new DateTimeLiteral(1991, 4, 30, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "'1991-05-01' + interval '1' day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 2, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "'1991-05-01' + interval 1+1 day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 3, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "date '1991-05-01' + interval 10 / 2 + 1 day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 7, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval '1' day + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 2, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval '3' month + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 8, 1, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 + 1 month + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 9, 1, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 + 1 year + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1995, 5, 1, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 + 3 / 2 hour + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 1, 14, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 * 2 / 3 minute + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 1, 10, 2, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 / 2 + 1 second + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 1, 10, 0, 2);
        assertRewrite(e7, e8);

        // a + interval 1 day
        Slot a = SlotReference.of("a", DateTimeType.INSTANCE);
        TimestampArithmetic arithmetic = new TimestampArithmetic(Operator.ADD, a, Literal.of(1), TimeUnit.DAY, false);
        Expression process = process(arithmetic);
        assertRewrite(process, process);
    }

    public Expression process(TimestampArithmetic arithmetic) {
        String funcOpName;
        if (arithmetic.getFuncName() == null) {
            funcOpName = String.format("%sS_%s", arithmetic.getTimeUnit(),
                    (arithmetic.getOp() == Operator.ADD) ? "ADD" : "SUB");
        } else {
            funcOpName = arithmetic.getFuncName();
        }
        return arithmetic.withFuncName(funcOpName.toLowerCase(Locale.ROOT));
    }

    private void assertRewrite(String expression, String expected) {
        Map<String, Slot> mem = Maps.newHashMap();
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        needRewriteExpression = typeCoercion(replaceUnboundSlot(needRewriteExpression, mem));
        Expression expectedExpression = PARSER.parseExpression(expected);
        expectedExpression = typeCoercion(replaceUnboundSlot(expectedExpression, mem));
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private void assertRewrite(String expression, Expression expectedExpression) {
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private void assertRewrite(Expression expression, Expression expectedExpression) {
        Expression rewrittenExpression = executor.rewrite(expression);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private Expression replaceUnboundSlot(Expression expression, Map<String, Slot> mem) {
        List<Expression> children = Lists.newArrayList();
        boolean hasNewChildren = false;
        for (Expression child : expression.children()) {
            Expression newChild = replaceUnboundSlot(child, mem);
            if (newChild != child) {
                hasNewChildren = true;
            }
            children.add(newChild);
        }
        if (expression instanceof UnboundSlot) {
            String name = ((UnboundSlot) expression).getName();
            mem.putIfAbsent(name, SlotReference.of(name, getType(name.charAt(0))));
            return mem.get(name);
        }
        return hasNewChildren ? expression.withChildren(children) : expression;
    }

    private Expression typeCoercion(Expression expression) {
        return TypeCoercion.INSTANCE.visit(expression, null);
    }

    private DataType getType(char t) {
        switch (t) {
            case 'T':
                return TinyIntType.INSTANCE;
            case 'I':
                return IntegerType.INSTANCE;
            case 'D':
                return DoubleType.INSTANCE;
            case 'S':
                return StringType.INSTANCE;
            case 'V':
                return VarcharType.INSTANCE;
            case 'B':
                return BooleanType.INSTANCE;
            default:
                return BigIntType.INSTANCE;
        }
    }
}
