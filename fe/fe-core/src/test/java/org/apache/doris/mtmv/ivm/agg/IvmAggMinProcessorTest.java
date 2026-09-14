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
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Least;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class IvmAggMinProcessorTest extends IvmAggProcessorTestBase {
    @Test
    void testMinUsesDeleteFallbackGuard() {
        IvmAggMinProcessor processor = new IvmAggMinProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new Min(value)));
        Assertions.assertEquals(IvmAggFunctionKind.MIN, processor.handledFunctionKind());
        Assertions.assertEquals(ImmutableList.of(IvmAggStateKey.COUNT), processor.hiddenStateKeys(new Min(value)));

        IvmAggTarget target = target(2, IvmAggFunctionKind.MIN, "min_v", IntegerType.INSTANCE,
                ImmutableMap.of(IvmAggStateKey.COUNT, slot("__ivm_count", BigIntType.INSTANCE)), valueArg());
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        Assertions.assertEquals(3, outputs.size());
        Assertions.assertTrue(outputs.get(0).child(0) instanceof Min);
        Assertions.assertTrue(outputs.get(1).getName().contains("DELMIN"));
        Assertions.assertTrue(outputs.get(2).child(0) instanceof Sum);

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("min_v", IntegerType.INSTANCE), slot("__ivm_count", BigIntType.INSTANCE)),
                mappedDeltaSlots(processor, target, outputs), slot("delta_group_count", BigIntType.INSTANCE));
        Expression visible = finalByName.get("min_v");
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof AssertTrue));
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof GreaterThan));
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof Least));
    }
}
