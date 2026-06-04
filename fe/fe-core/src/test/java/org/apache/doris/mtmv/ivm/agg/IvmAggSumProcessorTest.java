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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
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

class IvmAggSumProcessorTest extends IvmAggProcessorTestBase {
    @Test
    void testSumKeepsHiddenCountState() {
        IvmAggSumProcessor processor = new IvmAggSumProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new Sum(value)));
        Assertions.assertEquals(IvmAggFunctionKind.SUM, processor.handledFunctionKind());
        Assertions.assertEquals(ImmutableList.of(IvmAggStateKey.COUNT), processor.hiddenStateKeys(new Sum(value)));

        IvmAggTarget target = target(0, IvmAggFunctionKind.SUM, "sum_v", BigIntType.INSTANCE,
                ImmutableMap.of(IvmAggStateKey.COUNT, slot("__ivm_count", BigIntType.INSTANCE)), valueArg());
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        assertTwoSumDeltaOutputs(outputs);

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("sum_v", BigIntType.INSTANCE), slot("__ivm_count", BigIntType.INSTANCE)),
                mappedDeltaSlots(processor, target, outputs), slot("delta_group_count", BigIntType.INSTANCE));
        Assertions.assertTrue(finalByName.get("sum_v").anyMatch(node -> node instanceof If));
        Assertions.assertTrue(finalByName.get("__ivm_count").anyMatch(node -> node instanceof AssertTrue));
    }
}
