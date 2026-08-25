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

import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PredicateRewriteForPartitionPruneTest {

    @Test
    void testRewriteTimeStampNsDateInPredicateWithExactBounds() {
        SlotReference slot = new SlotReference("ts", TimeStampNsType.INSTANCE, true);
        DateV2Literal minDate = new DateV2Literal("1677-09-21");
        DateV2Literal normalDate = new DateV2Literal("1970-01-01");
        DateV2Literal maxDate = new DateV2Literal("2262-04-11");

        Expression rewritten = PredicateRewriteForPartitionPrune.rewrite(
                new InPredicate(new Date(slot), ImmutableList.of(minDate, normalDate, maxDate)), null);

        Expression expected = ExpressionUtils.or(ImmutableList.of(
                new And(new GreaterThanEqual(slot, TimeStampNsLiteral.getMinValue()),
                        new LessThanEqual(slot,
                                new TimeStampNsLiteral("1677-09-21 23:59:59.999999999"))),
                new And(new GreaterThanEqual(slot,
                                new TimeStampNsLiteral("1970-01-01 00:00:00.000000000")),
                        new LessThanEqual(slot,
                                new TimeStampNsLiteral("1970-01-01 23:59:59.999999999"))),
                new And(new GreaterThanEqual(slot,
                                new TimeStampNsLiteral("2262-04-11 00:00:00.000000000")),
                        new LessThanEqual(slot, TimeStampNsLiteral.getMaxValue()))));
        Assertions.assertEquals(expected, rewritten);
    }

    @Test
    void testRewriteTimeStampNsDateEqualityAfterSingletonInSimplification() {
        SlotReference slot = new SlotReference("ts", TimeStampNsType.INSTANCE, true);

        Expression rewritten = PredicateRewriteForPartitionPrune.rewrite(
                new EqualTo(new Date(slot), new DateV2Literal("1969-12-31")), null);

        Expression expected = new And(
                new GreaterThanEqual(slot,
                        new TimeStampNsLiteral("1969-12-31 00:00:00.000000000")),
                new LessThanEqual(slot,
                        new TimeStampNsLiteral("1969-12-31 23:59:59.999999999")));
        Assertions.assertEquals(expected, rewritten);
    }

    @Test
    void testRewriteDateTimeV2CastToTimeStampNsForPruning() {
        SlotReference slot = new SlotReference("dt", DateTimeV2Type.of(6), false);
        Cast cast = new Cast(slot, TimeStampNsType.INSTANCE);
        TimeStampNsLiteral first = new TimeStampNsLiteral("2024-01-01 00:00:00.000001000");
        TimeStampNsLiteral second = new TimeStampNsLiteral("2024-01-02 00:00:00.000002000");

        Expression rewrittenIn = PredicateRewriteForPartitionPrune.rewrite(
                new InPredicate(cast, ImmutableList.of(first, second)), null);
        Expression expectedIn = new InPredicate(slot, ImmutableList.of(
                new DateTimeV2Literal(DateTimeV2Type.of(6), "2024-01-01 00:00:00.000001"),
                new DateTimeV2Literal(DateTimeV2Type.of(6), "2024-01-02 00:00:00.000002")));
        Assertions.assertEquals(expectedIn, rewrittenIn);

        Expression unreachable = PredicateRewriteForPartitionPrune.rewrite(
                new EqualTo(cast,
                        new TimeStampNsLiteral("2024-01-02 00:00:00.000002001")), null);
        Assertions.assertEquals(BooleanLiteral.FALSE, unreachable);
    }
}
