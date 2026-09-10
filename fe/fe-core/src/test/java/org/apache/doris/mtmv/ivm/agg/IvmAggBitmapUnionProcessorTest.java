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
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class IvmAggBitmapUnionProcessorTest extends IvmAggProcessorTestBase {
    @Test
    void testBitmapUnionMergesVisibleBitmapState() {
        IvmAggBitmapUnionProcessor processor = new IvmAggBitmapUnionProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new BitmapUnion(bitmap)));
        Assertions.assertEquals(IvmAggFunctionKind.BITMAP_UNION, processor.handledFunctionKind());

        IvmAggTarget target = target(0, IvmAggFunctionKind.BITMAP_UNION, "bu", BitmapType.INSTANCE,
                ImmutableMap.of(), bitmapArg());
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        assertTwoBitmapDeltaOutputs(outputs);

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("bu", BitmapType.INSTANCE)),
                mappedDeltaSlots(processor, target, outputs), slot("delta_group_count", BigIntType.INSTANCE));
        Expression visible = finalByName.get("bu");
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof AssertTrue));
        assertBitmapApplyGuard(visible);
    }
}
