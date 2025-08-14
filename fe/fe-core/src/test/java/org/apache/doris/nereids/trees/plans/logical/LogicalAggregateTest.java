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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LogicalAggregateTest {

    @Test
    void testAdjustAggNullableWithEmptyGroupBy() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, false);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, false);

        LogicalOneRowRelation oneRowRelation = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of(a, b));

        // agg with empty group by
        NamedExpression originOutput1 = new Alias(new Add(new Sum(a), new IntegerLiteral(1)));
        NamedExpression originOutput2 = new Alias(new WindowExpression(
                new Sum(false, true, new Add(new Sum(b), new IntegerLiteral(1))),
                ImmutableList.of(a),
                ImmutableList.of(new OrderExpression(new OrderKey(b, true, true)))));
        Assertions.assertFalse(originOutput1.nullable());
        LogicalAggregate<LogicalOneRowRelation> agg = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(originOutput1, originOutput2), oneRowRelation);
        NamedExpression output1 = agg.getOutputs().get(0);
        NamedExpression output2 = agg.getOutputs().get(1);
        Assertions.assertNotEquals(originOutput1, output1);
        Assertions.assertNotEquals(originOutput2, output2);
        Assertions.assertTrue(output1.nullable());
        Expression expectOutput1Child = new Add(new Sum(false, true, a), new IntegerLiteral(1));
        Expression expectOutput2Child = new WindowExpression(
                new Sum(false, true, new Add(new Sum(false, true, b), new IntegerLiteral(1))),
                ImmutableList.of(a),
                ImmutableList.of(new OrderExpression(new OrderKey(b, true, true))));
        Assertions.assertEquals(expectOutput1Child, output1.child(0));
        Assertions.assertEquals(expectOutput2Child, output2.child(0));
    }

    @Test
    void testAdjustAggNullableWithNotEmptyGroupBy() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, false);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, false);

        LogicalOneRowRelation oneRowRelation = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of(a, b));

        // agg with not empty group by
        NamedExpression originOutput1 = new Alias(new Add(new Sum(false, true, a), new IntegerLiteral(1)));
        NamedExpression originOutput2 = new Alias(new WindowExpression(
                new Sum(false, true, new Add(new Sum(false, true, b), new IntegerLiteral(1))),
                ImmutableList.of(a),
                ImmutableList.of(new OrderExpression(new OrderKey(b, true, true)))));
        Assertions.assertTrue(originOutput1.nullable());
        LogicalAggregate<LogicalOneRowRelation> agg = new LogicalAggregate<>(
                ImmutableList.of(new TinyIntLiteral((byte) 1)), ImmutableList.of(originOutput1, originOutput2), oneRowRelation);
        NamedExpression output1 = agg.getOutputs().get(0);
        NamedExpression output2 = agg.getOutputs().get(1);
        Assertions.assertNotEquals(originOutput1, output1);
        Assertions.assertNotEquals(originOutput2, output2);
        Assertions.assertFalse(output1.nullable());
        Expression expectOutput1Child = new Add(new Sum(false, false, a), new IntegerLiteral(1));
        Expression expectOutput2Child = new WindowExpression(
                new Sum(false, true, new Add(new Sum(false, false, b), new IntegerLiteral(1))),
                ImmutableList.of(a),
                ImmutableList.of(new OrderExpression(new OrderKey(b, true, true))));
        Assertions.assertEquals(expectOutput1Child, output1.child(0));
        Assertions.assertEquals(expectOutput2Child, output2.child(0));
    }
}
