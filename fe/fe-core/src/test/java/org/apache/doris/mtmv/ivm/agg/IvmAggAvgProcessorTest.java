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
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Truncate;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class IvmAggAvgProcessorTest extends IvmAggProcessorTestBase {
    @Test
    void testAvgKeepsHiddenSumAndCountState() {
        IvmAggAvgProcessor processor = new IvmAggAvgProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new Avg(value)));
        Assertions.assertEquals(IvmAggFunctionKind.AVG, processor.handledFunctionKind());
        Assertions.assertEquals(ImmutableList.of(IvmAggStateKey.SUM, IvmAggStateKey.COUNT),
                processor.hiddenStateKeys(new Avg(value)));

        IvmAggTarget target = target(1, IvmAggFunctionKind.AVG, "avg_v", DoubleType.INSTANCE,
                ImmutableMap.of(
                        IvmAggStateKey.SUM, slot("__ivm_sum", BigIntType.INSTANCE),
                        IvmAggStateKey.COUNT, slot("__ivm_count", BigIntType.INSTANCE)),
                valueArg());
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        assertTwoSumDeltaOutputs(outputs);

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(
                        slot("avg_v", DoubleType.INSTANCE),
                        slot("__ivm_sum", BigIntType.INSTANCE),
                        slot("__ivm_count", BigIntType.INSTANCE)),
                mappedDeltaSlots(processor, target, outputs), slot("delta_group_count", BigIntType.INSTANCE));
        Assertions.assertTrue(finalByName.get("avg_v").anyMatch(node -> node instanceof If));
        Assertions.assertTrue(finalByName.get("__ivm_sum") instanceof Add);
        Assertions.assertTrue(finalByName.get("__ivm_count").anyMatch(node -> node instanceof AssertTrue));
    }

    @Test
    void testAvgGuardsCoercedDivisorInsideDivideForDecimal() {
        IvmAggAvgProcessor processor = new IvmAggAvgProcessor();
        DecimalV3Type visibleType = DecimalV3Type.createDecimalV3Type(18, 4);
        DecimalV3Type sumType = DecimalV3Type.createDecimalV3Type(28, 4);
        IvmAggTarget target = target(1, IvmAggFunctionKind.AVG, "avg_v", visibleType,
                ImmutableMap.of(
                        IvmAggStateKey.SUM, slot("__ivm_sum", sumType),
                        IvmAggStateKey.COUNT, slot("__ivm_count", BigIntType.INSTANCE)),
                ImmutableList.of(slot("v", visibleType)));

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(
                        slot("avg_v", visibleType),
                        slot("__ivm_sum", sumType),
                        slot("__ivm_count", BigIntType.INSTANCE)),
                mappedDeltaSlots(processor, target, deltaOutputs(processor, target)),
                slot("delta_group_count", BigIntType.INSTANCE));

        Expression avgValue = finalByName.get("avg_v");
        List<Divide> divides = avgValue.collectToList(node -> node instanceof Divide);
        List<Truncate> truncates = avgValue.collectToList(node -> node instanceof Truncate);
        Assertions.assertFalse(avgValue instanceof If);
        Assertions.assertEquals(1, divides.size());
        Assertions.assertTrue(divides.get(0).right() instanceof If);
        Assertions.assertEquals(1, truncates.size());
        Assertions.assertTrue(truncates.get(0).getDataType() instanceof DecimalV3Type);
        Assertions.assertEquals(visibleType.getScale(),
                ((DecimalV3Type) truncates.get(0).getDataType()).getScale());
        Assertions.assertEquals(4, ((IntegerLiteral) truncates.get(0).child(1)).getValue());

        If guardedCount = (If) divides.get(0).right();
        Assertions.assertEquals(guardedCount.getTrueValue().getDataType(),
                guardedCount.getFalseValue().getDataType());
    }
}
