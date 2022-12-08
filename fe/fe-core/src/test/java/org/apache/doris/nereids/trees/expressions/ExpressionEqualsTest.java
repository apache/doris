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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExpressionEqualsTest {
    private final ExprId exprId = new ExprId(1);
    private final Expression child1 = new SlotReference(exprId, "child",
            IntegerType.INSTANCE, false, Lists.newArrayList());
    private final Expression left1 = new SlotReference(exprId, "left",
            IntegerType.INSTANCE, false, Lists.newArrayList());
    private final Expression right1 = new SlotReference(exprId, "right",
            IntegerType.INSTANCE, false, Lists.newArrayList());

    private final Expression child2 = new SlotReference(exprId, "child",
            IntegerType.INSTANCE, false, Lists.newArrayList());
    private final Expression left2 = new SlotReference(exprId, "left",
            IntegerType.INSTANCE, false, Lists.newArrayList());
    private final Expression right2 = new SlotReference(exprId, "right",
            IntegerType.INSTANCE, false, Lists.newArrayList());

    @Test
    public void testComparisonPredicate() {
        // less than
        LessThan lessThan1 = new LessThan(left1, right1);
        LessThan lessThan2 = new LessThan(left2, right2);
        Assertions.assertEquals(lessThan1, lessThan2);
        Assertions.assertEquals(lessThan1.hashCode(), lessThan2.hashCode());

        // greater than
        GreaterThan greaterThan1 = new GreaterThan(left1, right1);
        GreaterThan greaterThan2 = new GreaterThan(left2, right2);
        Assertions.assertEquals(greaterThan1, greaterThan2);
        Assertions.assertEquals(greaterThan1.hashCode(), greaterThan2.hashCode());

        // null safe equal
        NullSafeEqual nullSafeEqual1 = new NullSafeEqual(left1, right1);
        NullSafeEqual nullSafeEqual2 = new NullSafeEqual(left2, right2);
        Assertions.assertEquals(nullSafeEqual1, nullSafeEqual2);
        Assertions.assertEquals(nullSafeEqual1.hashCode(), nullSafeEqual2.hashCode());

        // equal to
        EqualTo equalTo1 = new EqualTo(left1, right1);
        EqualTo equalTo2 = new EqualTo(left2, right2);
        Assertions.assertEquals(equalTo1, equalTo2);
        Assertions.assertEquals(equalTo1.hashCode(), equalTo2.hashCode());

        // less than equal
        LessThanEqual lessThanEqual1 = new LessThanEqual(left1, right1);
        LessThanEqual lessThanEqual2 = new LessThanEqual(left2, right2);
        Assertions.assertEquals(lessThanEqual1, lessThanEqual2);
        Assertions.assertEquals(lessThanEqual1.hashCode(), lessThanEqual2.hashCode());

        // greater than equal
        GreaterThanEqual greaterThanEqual1 = new GreaterThanEqual(left1, right1);
        GreaterThanEqual greaterThanEqual2 = new GreaterThanEqual(left2, right2);
        Assertions.assertEquals(greaterThanEqual1, greaterThanEqual2);
        Assertions.assertEquals(greaterThanEqual1.hashCode(), greaterThanEqual2.hashCode());
    }

    @Test
    public void testBetween() {
        Between between1 = new Between(child1, left1, right1);
        Between between2 = new Between(child2, left2, right2);
        Assertions.assertEquals(between1, between2);
        Assertions.assertEquals(between1.hashCode(), between2.hashCode());
    }

    @Test
    public void testNot() {
        Not not1 = new Not(child1);
        Not not2 = new Not(child2);
        Assertions.assertEquals(not1, not2);
        Assertions.assertEquals(not1.hashCode(), not2.hashCode());
    }

    @Test
    public void testStringRegexPredicate() {
        Like like1 = new Like(left1, right1);
        Like like2 = new Like(left2, right2);
        Assertions.assertEquals(like1, like2);
        Assertions.assertEquals(like1.hashCode(), like2.hashCode());

        Regexp regexp1 = new Regexp(left1, right1);
        Regexp regexp2 = new Regexp(left2, right2);
        Assertions.assertEquals(regexp1, regexp2);
        Assertions.assertEquals(regexp1.hashCode(), regexp2.hashCode());
    }

    @Test
    public void testCompoundPredicate() {
        And and1 = new And(left1, right1);
        And and2 = new And(left2, right2);
        Assertions.assertEquals(and1, and2);
        Assertions.assertEquals(and1.hashCode(), and2.hashCode());

        Or or1 = new Or(left1, right1);
        Or or2 = new Or(left2, right2);
        Assertions.assertEquals(or1, or2);
        Assertions.assertEquals(or1.hashCode(), or2.hashCode());
    }

    @Test
    public void testArithmetic() {
        Divide divide1 = new Divide(left1, right1);
        Divide divide2 = new Divide(left2, right2);
        Assertions.assertEquals(divide1, divide2);
        Assertions.assertEquals(divide1.hashCode(), divide2.hashCode());

        Multiply multiply1 = new Multiply(left1, right1);
        Multiply multiply2 = new Multiply(left2, right2);
        Assertions.assertEquals(multiply1, multiply2);
        Assertions.assertEquals(multiply1.hashCode(), multiply2.hashCode());

        Subtract subtract1 = new Subtract(left1, right1);
        Subtract subtract2 = new Subtract(left2, right2);
        Assertions.assertEquals(subtract1, subtract2);
        Assertions.assertEquals(subtract1.hashCode(), subtract2.hashCode());

        Mod mod1 = new Mod(left1, right1);
        Mod mod2 = new Mod(left2, right2);
        Assertions.assertEquals(mod1, mod2);
        Assertions.assertEquals(mod1.hashCode(), mod2.hashCode());

        Add add1 = new Add(left1, right1);
        Add add2 = new Add(left2, right2);
        Assertions.assertEquals(add1, add2);
        Assertions.assertEquals(add1.hashCode(), add2.hashCode());
    }

    @Test
    public void testUnboundFunction() {
        UnboundFunction unboundFunction1 = new UnboundFunction("name", false, false, Lists.newArrayList(child1));
        UnboundFunction unboundFunction2 = new UnboundFunction("name", false, false, Lists.newArrayList(child2));
        Assertions.assertEquals(unboundFunction1, unboundFunction2);
        Assertions.assertEquals(unboundFunction1.hashCode(), unboundFunction2.hashCode());
    }

    @Test
    public void testBoundFunction() {
        Sum sum1 = new Sum(child1);
        Sum sum2 = new Sum(child2);
        Assertions.assertEquals(sum1, sum2);
        Assertions.assertEquals(sum1.hashCode(), sum2.hashCode());
    }

    @Test
    public void testAggregateFunction() {
        Count count1 = new Count();
        Count count2 = new Count();
        Assertions.assertEquals(count1, count2);
        Assertions.assertEquals(count1.hashCode(), count2.hashCode());

        Count count3 = new Count(AggregateParam.distinctAndFinalPhase(), child1);
        Count count4 = new Count(AggregateParam.distinctAndFinalPhase(), child2);
        Assertions.assertEquals(count3, count4);
        Assertions.assertEquals(count3.hashCode(), count4.hashCode());

        // bad case
        Count count5 = new Count(AggregateParam.distinctAndFinalPhase(), child1);
        Count count6 = new Count(child2);
        Assertions.assertNotEquals(count5, count6);
        Assertions.assertNotEquals(count5.hashCode(), count6.hashCode());
    }

    @Test
    public void testNamedExpression() {
        ExprId aliasId = new ExprId(2);
        Alias alias1 = new Alias(aliasId, child1, "alias");
        Alias alias2 = new Alias(aliasId, child2, "alias");
        Assertions.assertEquals(alias1, alias2);
        Assertions.assertEquals(alias1.hashCode(), alias2.hashCode());

        UnboundAlias unboundAlias1 = new UnboundAlias(child1);
        UnboundAlias unboundAlias2 = new UnboundAlias(child2);
        Assertions.assertEquals(unboundAlias1, unboundAlias2);
        Assertions.assertEquals(unboundAlias1.hashCode(), unboundAlias2.hashCode());

        UnboundStar unboundStar1 = new UnboundStar(Lists.newArrayList("a"));
        UnboundStar unboundStar2 = new UnboundStar(Lists.newArrayList("a"));
        Assertions.assertEquals(unboundStar1, unboundStar2);
        Assertions.assertEquals(unboundStar1.hashCode(), unboundStar2.hashCode());
    }
}
