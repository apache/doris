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

package org.apache.doris.nereids.util;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BooleanType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * InferMarkSlotNotNullMapTest.
 */
public class InferMarkSlotNotNullMapTest extends ExpressionRewriteTestHelper {

    @Test
    public void testSingleMarkSlotAndOr() {
        MarkJoinSlotReference markSlot1 = new MarkJoinSlotReference("markSlot1");

        // pair.first is based on the simplified predicate (non-mark-slot children in And/Or
        // are replaced by true/false): true when it taking false or null always evaluates
        // to false or null; pair.second is based on the original predicate: true when it
        // taking false or null always evaluates to false or null
        assertMarkSlotPair(new And(BooleanLiteral.FALSE, markSlot1), markSlot1, true, true);
        assertMarkSlotPair(new And(BooleanLiteral.TRUE, markSlot1), markSlot1, true, true);
        assertMarkSlotPair(new And(NullLiteral.INSTANCE, markSlot1), markSlot1, true, true);
        // or(true, markSlot1): after simplification the child true is replaced by false, so
        // the simplified predicate or(false, markSlot1) taking false or null evaluates to
        // false or null, making pair.first true; the original or(true, markSlot1) taking
        // false evaluates to true, making pair.second false
        assertMarkSlotPair(new Or(BooleanLiteral.TRUE, markSlot1), markSlot1, true, false);
        assertMarkSlotPair(new Or(BooleanLiteral.FALSE, markSlot1), markSlot1, true, true);
        assertMarkSlotPair(new Or(NullLiteral.INSTANCE, markSlot1), markSlot1, true, true);
    }

    @Test
    public void testSingleMarkSlotIsNullIsNotNull() {
        MarkJoinSlotReference markSlot1 = new MarkJoinSlotReference("markSlot1");

        // is null: taking false returns false while taking null returns true, which is
        // neither false nor null, and taking true returns false, so both fields are false
        assertMarkSlotPair(new IsNull(markSlot1), markSlot1, false, false);
        // is not null: taking false returns true, which is neither false nor null, so
        // pair.first is false; the original predicate taking false also evaluates to
        // true, so pair.second is false too
        assertMarkSlotPair(new Not(new IsNull(markSlot1)), markSlot1, false, false);
        // markSlot1 and is not null(markSlot1): the predicate is equivalent to
        // markSlot1 being true
        assertMarkSlotPair(new And(markSlot1, new Not(new IsNull(markSlot1))),
                markSlot1, true, true);
    }

    @Test
    public void testSingleMarkSlotNvl() {
        MarkJoinSlotReference markSlot1 = new MarkJoinSlotReference("markSlot1");

        // nvl(markSlot1, false) is equivalent to markSlot1 being true
        assertMarkSlotPair(new Nvl(markSlot1, BooleanLiteral.FALSE), markSlot1, true, true);
        // nvl(markSlot1, true): taking null returns true, which is neither false nor null,
        // so both pair.first and pair.second are false
        assertMarkSlotPair(new Nvl(markSlot1, BooleanLiteral.TRUE), markSlot1, false, false);
        // nvl(markSlot1, null): taking null returns null, which is treated as same as false,
        // and taking true returns true, so both fields are true
        assertMarkSlotPair(new Nvl(markSlot1, NullLiteral.INSTANCE), markSlot1, true, true);
    }

    @Test
    public void testMultiMarkSlot() {
        MarkJoinSlotReference markSlot1 = new MarkJoinSlotReference("markSlot1");
        MarkJoinSlotReference markSlot2 = new MarkJoinSlotReference("markSlot2");

        // or(markSlot1, markSlot2): when the other mark slot is true, the target slot taking
        // false or null evaluates to true, which is neither false nor null, so both
        // pair.first and pair.second are false
        assertMarkSlotPair(new Or(markSlot1, markSlot2), markSlot1, false, false);
        assertMarkSlotPair(new Or(markSlot1, markSlot2), markSlot2, false, false);
        // and(markSlot1, markSlot2): the target slot taking false or null always evaluates
        // to false or null in both the simplified and the original predicate
        assertMarkSlotPair(new And(markSlot1, markSlot2), markSlot1, true, true);
        assertMarkSlotPair(new And(markSlot1, markSlot2), markSlot2, true, true);

        // and(or(markSlot1, markSlot2), false): after simplification the non-mark-slot child
        // false is replaced by true, so the simplified predicate taking false can evaluate
        // to true when the other mark slot is true, making pair.first false; the original
        // predicate always evaluates to false or null, making pair.second true
        assertMarkSlotPair(new And(new Or(markSlot1, markSlot2), BooleanLiteral.FALSE),
                markSlot1, false, true);
        assertMarkSlotPair(new And(new Or(markSlot1, markSlot2), BooleanLiteral.FALSE),
                markSlot2, false, true);

        // markSlot1 taking false or null always evaluates to false or null in both the
        // simplified and the original predicate, while markSlot2 taking false evaluates
        // to true when markSlot1 is true, so for markSlot2 both pair.first and pair.second
        // are false
        Expression predicate = new And(new Nvl(markSlot1, BooleanLiteral.FALSE),
                new Or(markSlot1, markSlot2));
        Map<MarkJoinSlotReference, Pair<Boolean, Boolean>> result =
                ExpressionUtils.inferMarkSlotNotNullMap(predicate, context);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(Pair.of(Boolean.TRUE, Boolean.TRUE), result.get(markSlot1));
        Assertions.assertEquals(Pair.of(Boolean.FALSE, Boolean.FALSE), result.get(markSlot2));
    }

    @Test
    public void testSimplifyWithNonMarkSlot() {
        SlotReference slot = new SlotReference("slot", BooleanType.INSTANCE);
        MarkJoinSlotReference markSlot1 = new MarkJoinSlotReference("markSlot1");

        // and(markSlot1, slot): the non-mark-slot child is replaced by true after
        // simplification, so the simplified predicate and(markSlot1, true) taking false
        // or null evaluates to false or null, making pair.first true; the original
        // predicate and(markSlot1, slot) taking null evaluates to the slot, which is
        // neither false nor null, making pair.second false
        assertMarkSlotPair(new And(markSlot1, slot), markSlot1, true, false);

        // or(markSlot1, slot): the non-mark-slot child is replaced by false after
        // simplification, so the simplified predicate or(markSlot1, false) taking false
        // or null evaluates to false or null, making pair.first true; the original
        // predicate or(markSlot1, slot) taking false evaluates to the slot, which is
        // neither false nor null, so pair.second is false
        assertMarkSlotPair(new Or(markSlot1, slot), markSlot1, true, false);
    }

    @Test
    public void testMarkFreeSubtreeUnderNullObservingWrapper() {
        MarkJoinSlotReference markSlot1 = new MarkJoinSlotReference("markSlot1");
        SlotReference slot = new SlotReference("slot", BooleanType.INSTANCE);

        // nvl(markSlot1, slot or false): the mark-free subtree (slot or false) is observed
        // by nvl when the mark slot is null, so TrySimplifyPredicateWithMarkJoinSlot must
        // NOT simplify it to false. otherwise the simplified predicate nvl(markSlot1, false)
        // would make pair.first true, but in the original predicate markSlot1 taking null
        // with slot = true evaluates to true (the filter keeps the row) while markSlot1
        // taking false evaluates to false, so both fields must be false
        assertMarkSlotPair(new Nvl(markSlot1, new Or(slot, BooleanLiteral.FALSE)),
                markSlot1, false, false);

        // sanity check: the same mark-free subtree at the top level of an Or IS simplified
        // (it is in a boolean predicate position), so pair.first stays true
        assertMarkSlotPair(new Or(markSlot1, new Or(slot, BooleanLiteral.FALSE)),
                markSlot1, true, false);
    }

    @Test
    public void testNoneMovableFunctionFencesMarkJoinElimination() {
        MarkJoinSlotReference markSlot1 = new MarkJoinSlotReference("markSlot1");
        SlotReference guard = new SlotReference("guard", BooleanType.INSTANCE);

        // ifnull(ifnull(M, false) and assert_true(guard, 'bad'), false)
        Expression predicate = new Nvl(
                new And(new Nvl(markSlot1, BooleanLiteral.FALSE),
                        new AssertTrue(guard, new VarcharLiteral("bad"))),
                BooleanLiteral.FALSE);

        // although M = false and M = null both fold the whole predicate to false (so the
        // row-truth proof on the original predicate would make pair.second true), dropping
        // the mark join changes which rows reach assert_true, a NoneMovableFunction: the
        // semi join prunes the unmatched rows before the filter, so assert_true is no
        // longer evaluated on them and its error is suppressed, violating the
        // NoneMovableFunction contract. pair.second must therefore be fenced to false
        assertMarkSlotPair(predicate, markSlot1, true, false);
    }

    @Test
    public void testMarkSlotCountLimit() {
        MarkJoinSlotReference markSlot1 = new MarkJoinSlotReference("markSlot1");
        MarkJoinSlotReference markSlot2 = new MarkJoinSlotReference("markSlot2");
        MarkJoinSlotReference markSlot3 = new MarkJoinSlotReference("markSlot3");
        MarkJoinSlotReference markSlot4 = new MarkJoinSlotReference("markSlot4");
        MarkJoinSlotReference markSlot5 = new MarkJoinSlotReference("markSlot5");

        // no mark slot -> empty map
        Assertions.assertTrue(
                ExpressionUtils.inferMarkSlotNotNullMap(BooleanLiteral.TRUE, context).isEmpty());

        // 4 mark slots is within the limit
        List<Expression> withinLimitList = Lists.newArrayList(
                markSlot1, markSlot2, markSlot3, markSlot4);
        Map<MarkJoinSlotReference, Pair<Boolean, Boolean>> withinLimit = ExpressionUtils
                .inferMarkSlotNotNullMap(new Or(withinLimitList), context);
        Assertions.assertEquals(4, withinLimit.size());

        // 5 mark slots exceeds the limit -> empty map
        List<Expression> exceedLimitList = Lists.newArrayList(
                markSlot1, markSlot2, markSlot3, markSlot4, markSlot5);
        Assertions.assertTrue(ExpressionUtils.inferMarkSlotNotNullMap(
                new Or(exceedLimitList), context).isEmpty());
    }

    private void assertMarkSlotPair(Expression predicate, MarkJoinSlotReference markSlot,
            boolean expectedFirst, boolean expectedSecond) {
        Map<MarkJoinSlotReference, Pair<Boolean, Boolean>> result =
                ExpressionUtils.inferMarkSlotNotNullMap(predicate, context);
        Assertions.assertTrue(result.containsKey(markSlot));
        Assertions.assertEquals(Pair.of(Boolean.valueOf(expectedFirst), Boolean.valueOf(expectedSecond)),
                result.get(markSlot));
    }
}
