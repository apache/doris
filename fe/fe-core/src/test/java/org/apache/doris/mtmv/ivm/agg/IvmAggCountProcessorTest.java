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

package org.apache.doris.mtmv.ivm.agg;

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.types.BigIntType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class IvmAggCountProcessorTest extends IvmAggProcessorTestBase {
    @Test
    void testCountStarUsesDeltaGroupCount() {
        IvmAggCountProcessor processor = new IvmAggCountProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new Count()));
        Assertions.assertEquals(IvmAggFunctionKind.COUNT, processor.handledFunctionKind());
        Assertions.assertTrue(processor.targetArguments(new Count()).isEmpty());

        IvmAggTarget target = target(0, IvmAggFunctionKind.COUNT, "cnt", BigIntType.INSTANCE,
                ImmutableMap.of(), ImmutableList.of());
        Assertions.assertTrue(deltaOutputs(processor, target).isEmpty());

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("cnt", BigIntType.INSTANCE)), ImmutableMap.of(),
                slot("delta_group_count", BigIntType.INSTANCE));
        Assertions.assertEquals("delta_group_count", slotName(finalByName.get("cnt")));
    }

    @Test
    void testCountExprUsesSignedNonNullCount() {
        IvmAggCountProcessor processor = new IvmAggCountProcessor();
        Assertions.assertEquals(ImmutableList.of(value), processor.targetArguments(new Count(value)));

        IvmAggTarget target = target(1, IvmAggFunctionKind.COUNT, "cnt_v", BigIntType.INSTANCE,
                ImmutableMap.of(), valueArg());
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        Assertions.assertEquals(1, outputs.size());
        Assertions.assertTrue(outputs.get(0).child(0) instanceof Sum);
        Assertions.assertTrue(outputs.get(0).child(0).anyMatch(node -> node instanceof If));

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("cnt_v", BigIntType.INSTANCE)),
                mappedDeltaSlots(processor, target, outputs), slot("delta_group_count", BigIntType.INSTANCE));
        Expression visible = finalByName.get("cnt_v");
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof AssertTrue));
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof Add));
    }
}
