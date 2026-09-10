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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Greatest;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class IvmAggMaxProcessorTest extends IvmAggProcessorTestBase {
    @Test
    void testMaxUsesDeleteFallbackGuard() {
        IvmAggMaxProcessor processor = new IvmAggMaxProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new Max(value)));
        Assertions.assertEquals(IvmAggFunctionKind.MAX, processor.handledFunctionKind());
        Assertions.assertEquals(ImmutableList.of(IvmAggStateKey.COUNT), processor.hiddenStateKeys(new Max(value)));

        IvmAggTarget target = target(3, IvmAggFunctionKind.MAX, "max_v", IntegerType.INSTANCE,
                ImmutableMap.of(IvmAggStateKey.COUNT, slot("__ivm_count", BigIntType.INSTANCE)), valueArg());
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        Assertions.assertEquals(3, outputs.size());
        Assertions.assertTrue(outputs.get(0).child(0) instanceof Max);
        Assertions.assertTrue(outputs.get(1).getName().contains("DELMAX"));
        Assertions.assertTrue(outputs.get(2).child(0) instanceof Sum);

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("max_v", IntegerType.INSTANCE), slot("__ivm_count", BigIntType.INSTANCE)),
                mappedDeltaSlots(processor, target, outputs), slot("delta_group_count", BigIntType.INSTANCE));
        Expression visible = finalByName.get("max_v");
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof AssertTrue));
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof LessThan));
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof Greatest));
    }
}
