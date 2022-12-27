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
import org.apache.doris.nereids.trees.expressions.literal.Interval.TimeUnit;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

public class FoldConstantTest extends ExpressionRewriteTestHelper {

    @Test
    public void testCaseWhenFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("case when 1 = 2 then 1 when '1' < 2 then 2 else 3 end", "2");
        assertRewriteAfterTypeCoercion("case when 1 = 2 then 1 when '1' > 2 then 2 end", "null");
        assertRewriteAfterTypeCoercion("case when (1 + 5) / 2 > 2 then 4  when '1' < 2 then 2 else 3 end", "4");
        assertRewriteAfterTypeCoercion("case when not 1 = 2 then 1 when '1' > 2 then 2 end", "1");
        assertRewriteAfterTypeCoercion("case when 1 = 2 then 1 when 3 in ('1',2 + 8 / 2,3,4) then 2 end", "2");
        assertRewriteAfterTypeCoercion("case when TA = 2 then 1 when 3 in ('1',2 + 8 / 2,3,4) then 2 end", "CASE  WHEN (TA = 2) THEN 1 ELSE 2 END");
        assertRewriteAfterTypeCoercion("case when TA = 2 then 5 when 3 in (2,3,4) then 2 else 4 end", "CASE  WHEN (TA = 2) THEN 5 ELSE 2 END");
        assertRewriteAfterTypeCoercion("case when TA = 2 then 1 when TB in (2,3,4) then 2 else 4 end", "CASE  WHEN (TA = 2) THEN 1 WHEN TB IN (2, 3, 4) THEN 2 ELSE 4 END");
        assertRewriteAfterTypeCoercion("case when null = 2 then 1 when 3 in (2,3,4) then 2 else 4 end", "2");
        assertRewriteAfterTypeCoercion("case when null = 2 then 1 else 4 end", "4");
        assertRewriteAfterTypeCoercion("case when null = 2 then 1 end", "null");
        assertRewriteAfterTypeCoercion("case when TA = TB then 1 when TC is null then 2 end", "CASE WHEN (TA = TB) THEN 1 WHEN TC IS NULL THEN 2 END");
    }

    @Test
    public void testInFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("1 in (1,2,3,4)", "true");
        // Type Coercion trans all to string.
        assertRewriteAfterTypeCoercion("3 in ('1',2 + 8 / 2,3,4)", "true");
        assertRewriteAfterTypeCoercion("4 / 2 * 1 - (5/2) in ('1',2 + 8 / 2,3,4)", "false");
        assertRewriteAfterTypeCoercion("null in ('1',2 + 8 / 2,3,4)", "null");
        assertRewriteAfterTypeCoercion("3 in ('1',null,3,4)", "true");
        assertRewriteAfterTypeCoercion("TA in (1,null,3,4)", "TA in (1, null, 3, 4)");
        assertRewriteAfterTypeCoercion("IA in (IB,IC,null)", "IA in (IB,IC,null)");
    }

    @Test
    public void testLogicalFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("10 + 1 > 1 and 1 > 2", "false");
        assertRewriteAfterTypeCoercion("10 + 1 > 1 and 1 < 2", "true");
        assertRewriteAfterTypeCoercion("null + 1 > 1 and 1 < 2", "null");
        assertRewriteAfterTypeCoercion("10 < 3 and 1 > 2", "false");
        assertRewriteAfterTypeCoercion("6 / 2 - 10 * (6 + 1) > 2 and 10 > 3 and 1 > 2", "false");

        assertRewriteAfterTypeCoercion("10 + 1 > 1 or 1 > 2", "true");
        assertRewriteAfterTypeCoercion("null + 1 > 1 or 1 > 2", "null");
        assertRewriteAfterTypeCoercion("6 / 2 - 10 * (6 + 1) > 2 or 10 > 3 or 1 > 2", "true");

        assertRewriteAfterTypeCoercion("(1 > 5 and 8 < 10 or 1 = 3) or (1 > 8 + 9 / (10 * 2) or ( 10 = 3))", "false");
        assertRewriteAfterTypeCoercion("(TA > 1 and 8 < 10 or 1 = 3) or (1 > 3 or ( 10 = 3))", "TA > 1");

        assertRewriteAfterTypeCoercion("false or false", "false");
        assertRewriteAfterTypeCoercion("false or true", "true");
        assertRewriteAfterTypeCoercion("true or false", "true");
        assertRewriteAfterTypeCoercion("true or true", "true");

        assertRewriteAfterTypeCoercion("true and true", "true");
        assertRewriteAfterTypeCoercion("false and true", "false");
        assertRewriteAfterTypeCoercion("true and false", "false");
        assertRewriteAfterTypeCoercion("false and false", "false");

        assertRewriteAfterTypeCoercion("true and null", "null");
        assertRewriteAfterTypeCoercion("false and null", "false");
        assertRewriteAfterTypeCoercion("true or null", "true");
        assertRewriteAfterTypeCoercion("false or null", "null");

        assertRewriteAfterTypeCoercion("null and null", "null");
    }

    @Test
    public void testIsNullFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("100 is null", "false");
        assertRewriteAfterTypeCoercion("null is null", "true");
        assertRewriteAfterTypeCoercion("null is not null", "false");
        assertRewriteAfterTypeCoercion("100 is not null", "true");
        assertRewriteAfterTypeCoercion("IA is not null", "IA is not null");
        assertRewriteAfterTypeCoercion("IA is null", "IA is null");
    }

    @Test
    public void testNotFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("not 1 > 2", "true");
        assertRewriteAfterTypeCoercion("not null + 1 > 2", "null");
        assertRewriteAfterTypeCoercion("not (1 + 5) / 2 + (10 - 1) * 3 > 3 * 5 + 1", "false");
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
        assertRewriteAfterTypeCoercion("'1' = 2", "false");
        assertRewriteAfterTypeCoercion("1 = 2", "false");
        assertRewriteAfterTypeCoercion("1 != 2", "true");
        assertRewriteAfterTypeCoercion("2 > 2", "false");
        assertRewriteAfterTypeCoercion("3 * 10 + 1 / 2 >= 2", "true");
        assertRewriteAfterTypeCoercion("3 < 2", "false");
        assertRewriteAfterTypeCoercion("3 <= 2", "false");
        assertRewriteAfterTypeCoercion("3 <= null", "null");
        assertRewriteAfterTypeCoercion("3 >= null", "null");
        assertRewriteAfterTypeCoercion("null <=> null", "true");
        assertRewriteAfterTypeCoercion("2 <=> null", "false");
        assertRewriteAfterTypeCoercion("2 <=> 2", "true");
    }

    @Test
    public void testArithmeticFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRuleOnFE.INSTANCE));
        assertRewrite("1 + 1", Literal.of((short) 2));
        assertRewrite("1 - 1", Literal.of((short) 0));
        assertRewrite("100 + 100", Literal.of((short) 200));
        assertRewrite("1 - 2", Literal.of((short) -1));
        assertRewriteAfterTypeCoercion("1 - 2 > 1", "false");
        assertRewriteAfterTypeCoercion("1 - 2 + 1 > 1 + 1 - 100", "true");
        assertRewriteAfterTypeCoercion("10 * 2 / 1 + 1 > (1 + 1) - 100", "true");

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
}
