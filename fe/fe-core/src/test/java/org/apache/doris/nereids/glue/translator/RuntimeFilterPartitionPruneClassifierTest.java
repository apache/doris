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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RuntimeFilterPartitionPruneClassifierTest {
    @Test
    void testRejectInputSlotOutsideMonotonicChild() {
        SlotReference slot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        DateTrunc dateTrunc = new DateTrunc(slot, slot);
        Monotonic monotonic = dateTrunc;

        Assertions.assertEquals(1, dateTrunc.getInputSlots().size());
        Assertions.assertFalse(RuntimeFilterPartitionPruneClassifier.hasInputSlotOnlyInMonotonicChild(
                dateTrunc, monotonic.getMonotonicFunctionChildIndex()));
    }

    @Test
    void testAcceptInputSlotOnlyInMonotonicChild() {
        SlotReference slot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        DateTrunc dateTrunc = new DateTrunc(slot, new VarcharLiteral("day"));
        Monotonic monotonic = dateTrunc;

        Assertions.assertTrue(RuntimeFilterPartitionPruneClassifier.hasInputSlotOnlyInMonotonicChild(
                dateTrunc, monotonic.getMonotonicFunctionChildIndex()));
    }
}
