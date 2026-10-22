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

import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayExceptAll;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFilter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class IvmAggArrayAggProcessorTest extends IvmAggProcessorTestBase {
    @Test
    void testArrayAggPacksStructPairsAndSplitsPolarityViews() {
        IvmAggArrayAggProcessor processor = new IvmAggArrayAggProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new ArrayAgg(value)));
        Assertions.assertEquals(IvmAggFunctionKind.ARRAY_AGG, processor.handledFunctionKind());
        Assertions.assertTrue(processor.hiddenStateKeys(new ArrayAgg(value)).isEmpty());

        IvmAggTarget target = target(0, IvmAggFunctionKind.ARRAY_AGG, "arr", ArrayType.of(IntegerType.INSTANCE),
                ImmutableMap.of(), valueArg());

        // The delta aggregate packs every change row into one struct array; NULL elements stay packed
        // inside the structs, so no conditional (NULL-filtering) aggregate is involved.
        List<NamedExpression> aggOutputs = deltaOutputs(processor, target);
        Assertions.assertEquals(1, aggOutputs.size());
        Assertions.assertTrue(aggOutputs.get(0).child(0) instanceof ArrayAgg);
        Assertions.assertTrue(aggOutputs.get(0).child(0).anyMatch(node -> node instanceof CreateStruct));
        Assertions.assertFalse(aggOutputs.get(0).anyMatch(node -> node instanceof ElementAt));

        // The insert/delete polarity columns are derived above the aggregate, mirroring
        // IvmAggDeltaHandler.buildDeltaSubPlan: the handler builds the top project from the delta
        // aggregate's output slots, then asks every processor to append its derived outputs -- so
        // here the agg outputs plus the derived ones must be resolvable by name exactly like the
        // top project's columns, each polarity view a map over a filter on the factor field.
        Map<String, Slot> aggOutputByName = new HashMap<>();
        for (NamedExpression output : aggOutputs) {
            aggOutputByName.put(output.getName(), output.toSlot());
        }
        List<NamedExpression> topOutputs = new ArrayList<>(aggOutputs);
        processor.appendDeltaTopProjectOutputs(target, aggOutputByName, topOutputs,
                IvmAggExpressionBuilder.INSTANCE);
        Assertions.assertEquals(3, topOutputs.size());
        NamedExpression ins = topOutputs.get(1);
        NamedExpression del = topOutputs.get(2);
        Assertions.assertTrue(ins.getName().contains("ARRAY_INS"));
        Assertions.assertTrue(del.getName().contains("ARRAY_DEL"));
        Assertions.assertNotEquals(ins.getName(), del.getName());
        for (NamedExpression polarity : ImmutableList.of(ins, del)) {
            Assertions.assertTrue(polarity.child(0) instanceof ArrayMap);
            Assertions.assertTrue(polarity.child(0).anyMatch(node -> node instanceof ArrayFilter));
            Assertions.assertTrue(polarity.child(0).anyMatch(node -> node instanceof ElementAt));
        }

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("arr", ArrayType.of(IntegerType.INSTANCE))),
                mappedDeltaSlots(processor, target, topOutputs),
                slot("delta_group_count", IntegerType.INSTANCE));
        Expression visible = finalByName.get("arr");
        Assertions.assertNotNull(visible);
        Assertions.assertTrue(visible instanceof ArrayExceptAll);
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof ArrayConcat));
        // old, ins and del sides each merge NULL into an empty array
        Assertions.assertEquals(3, visible.collect(node -> node instanceof Coalesce).size());
    }

    @Test
    void testArrayAggRejectsVariantAndJsonbElementsWithPreciseReason() {
        IvmAggArrayAggProcessor processor = new IvmAggArrayAggProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new ArrayAgg(value)));
        // struct(dml_factor, elem) cannot carry JSONB/VARIANT fields, so ARRAY_AGG over those
        // element types must be rejected with the precise reason (and that reason must reach the
        // user when CREATE MATERIALIZED VIEW analyzes the query) instead of returning false, which
        // would only produce the registry's generic "unsupported aggregate" error.
        for (Slot variantSlot : ImmutableList.of(
                new SlotReference("jv", VariantType.INSTANCE, true),
                new SlotReference("jb", JsonType.INSTANCE, true))) {
            IvmException ex = Assertions.assertThrows(IvmException.class,
                    () -> processor.supportsOriginalFunction(new ArrayAgg(variantSlot)));
            Assertions.assertTrue(ex.getMessage().contains("not incrementally maintainable"),
                    "unexpected message: " + ex.getMessage());
        }
    }
}
