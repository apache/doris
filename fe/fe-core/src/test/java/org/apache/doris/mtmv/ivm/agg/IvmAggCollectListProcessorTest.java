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
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectList;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayExceptAll;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class IvmAggCollectListProcessorTest extends IvmAggProcessorTestBase {
    @Test
    void testCollectListUsesNullFilteringIdiomAndMultisetApply() {
        IvmAggCollectListProcessor processor = new IvmAggCollectListProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new CollectList(value)));
        // two-argument LIMIT variant is not incrementally maintainable
        Assertions.assertFalse(processor.supportsOriginalFunction(
                new CollectList(value, new IntegerLiteral(10))));
        Assertions.assertEquals(IvmAggFunctionKind.COLLECT_LIST, processor.handledFunctionKind());
        Assertions.assertTrue(processor.hiddenStateKeys(new CollectList(value)).isEmpty());

        IvmAggTarget target = target(0, IvmAggFunctionKind.COLLECT_LIST, "arr",
                ArrayType.of(IntegerType.INSTANCE), ImmutableMap.of(), valueArg());
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        Assertions.assertEquals(2, outputs.size());
        Assertions.assertTrue(outputs.get(0).child(0) instanceof CollectList);
        Assertions.assertTrue(outputs.get(1).child(0) instanceof CollectList);
        Assertions.assertTrue(outputs.get(0).child(0).anyMatch(node -> node instanceof If));
        Assertions.assertTrue(outputs.get(1).child(0).anyMatch(node -> node instanceof If));

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("arr", ArrayType.of(IntegerType.INSTANCE))),
                mappedDeltaSlots(processor, target, outputs),
                slot("delta_group_count", IntegerType.INSTANCE));
        Expression visible = finalByName.get("arr");
        Assertions.assertNotNull(visible);
        Assertions.assertTrue(visible instanceof ArrayExceptAll);
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof ArrayConcat));
    }
}
