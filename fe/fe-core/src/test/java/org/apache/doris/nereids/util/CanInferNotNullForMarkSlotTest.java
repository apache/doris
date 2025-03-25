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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.BooleanType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * CanInferNotNullForMarkSlotTest.
 */
public class CanInferNotNullForMarkSlotTest extends ExpressionRewriteTestHelper {

    @Test
    public void test() {
        SlotReference slot = new SlotReference("slot", BooleanType.INSTANCE);
        MarkJoinSlotReference markSlot1 = new MarkJoinSlotReference("markSlot1");
        MarkJoinSlotReference markSlot2 = new MarkJoinSlotReference("markSlot2");
        MarkJoinSlotReference markSlot3 = new MarkJoinSlotReference("markSlot1");
        MarkJoinSlotReference markSlot4 = new MarkJoinSlotReference("markSlot2");
        MarkJoinSlotReference markSlot5 = new MarkJoinSlotReference("markSlot1");

        List<Expression> markJoinSlotReferenceList1 = Lists.newArrayList(markSlot1, markSlot2, markSlot3, markSlot4);
        List<Expression> markJoinSlotReferenceList2 = Lists.newArrayList(markSlot1, markSlot2, markSlot3, markSlot4,
                markSlot5);

        Assertions.assertTrue(
                ExpressionUtils.canInferNotNullForMarkSlot(new And(BooleanLiteral.TRUE, markSlot1), context));
        Assertions.assertTrue(
                ExpressionUtils.canInferNotNullForMarkSlot(new And(BooleanLiteral.FALSE, markSlot1), context));
        Assertions.assertTrue(
                ExpressionUtils.canInferNotNullForMarkSlot(new And(NullLiteral.INSTANCE, markSlot1), context));
        Assertions.assertTrue(
                ExpressionUtils.canInferNotNullForMarkSlot(new Or(BooleanLiteral.TRUE, markSlot1), context));
        Assertions.assertTrue(
                ExpressionUtils.canInferNotNullForMarkSlot(new Or(BooleanLiteral.FALSE, markSlot1), context));
        Assertions.assertTrue(
                ExpressionUtils.canInferNotNullForMarkSlot(new Or(NullLiteral.INSTANCE, markSlot1), context));
        Assertions.assertTrue(ExpressionUtils
                .canInferNotNullForMarkSlot(new And(new Or(BooleanLiteral.TRUE, markSlot2), markSlot1), context));
        Assertions.assertTrue(ExpressionUtils
                .canInferNotNullForMarkSlot(new And(new Or(BooleanLiteral.FALSE, markSlot2), markSlot1), context));
        Assertions.assertTrue(ExpressionUtils
                .canInferNotNullForMarkSlot(new And(new Or(NullLiteral.INSTANCE, markSlot2), markSlot1), context));
        Assertions.assertTrue(ExpressionUtils
                .canInferNotNullForMarkSlot(new Or(new And(BooleanLiteral.TRUE, markSlot2), markSlot1), context));
        Assertions.assertTrue(ExpressionUtils
                .canInferNotNullForMarkSlot(new Or(new And(BooleanLiteral.FALSE, markSlot2), markSlot1), context));
        Assertions.assertTrue(ExpressionUtils
                .canInferNotNullForMarkSlot(new Or(new And(NullLiteral.INSTANCE, markSlot2), markSlot1), context));
        Assertions.assertTrue(ExpressionUtils.canInferNotNullForMarkSlot(
                new And(new Or(BooleanLiteral.FALSE, markSlot2), new IsNull(markSlot1)), context));
        Assertions.assertTrue(ExpressionUtils.canInferNotNullForMarkSlot(
                new And(new Or(NullLiteral.INSTANCE, markSlot2), new IsNull(markSlot1)), context));
        Assertions.assertTrue(
                ExpressionUtils.canInferNotNullForMarkSlot(new And(new IsNull(markSlot2), markSlot1), context));
        Assertions.assertTrue(ExpressionUtils.canInferNotNullForMarkSlot(new Or(markJoinSlotReferenceList1), context));

        Assertions.assertFalse(
                ExpressionUtils.canInferNotNullForMarkSlot(new Or(new IsNull(markSlot2), markSlot1), context));
        Assertions.assertFalse(ExpressionUtils.canInferNotNullForMarkSlot(
                new And(new Or(BooleanLiteral.TRUE, markSlot2), new IsNull(markSlot1)), context));
        Assertions.assertFalse(ExpressionUtils.canInferNotNullForMarkSlot(
                new Or(new And(BooleanLiteral.TRUE, markSlot2), new IsNull(markSlot1)), context));
        Assertions.assertFalse(ExpressionUtils.canInferNotNullForMarkSlot(
                new Or(new And(BooleanLiteral.FALSE, markSlot2), new IsNull(markSlot1)), context));
        Assertions.assertFalse(ExpressionUtils.canInferNotNullForMarkSlot(
                new Or(new And(NullLiteral.INSTANCE, markSlot2), new IsNull(markSlot1)), context));
        Assertions.assertFalse(ExpressionUtils.canInferNotNullForMarkSlot(new And(slot, markSlot1), context));
        Assertions.assertFalse(ExpressionUtils.canInferNotNullForMarkSlot(new Or(markJoinSlotReferenceList2), context));
    }
}
