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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class LazySlotPruningTest {

    @Test
    public void testFilterKeepsMaterializedPredicateSlot() {
        SlotReference orderKey = new SlotReference("order_key", IntegerType.INSTANCE);
        SlotReference lazyPredicateSlot = new SlotReference("lazy_predicate", IntegerType.INSTANCE);
        SlotReference lazyOutputSlot = new SlotReference("lazy_output", IntegerType.INSTANCE);
        SlotReference rowId = new SlotReference("row_id", IntegerType.INSTANCE);

        List<Slot> filterOutput = LazySlotPruning.computeFilterOutput(
                ImmutableList.of(orderKey, lazyPredicateSlot, rowId),
                ImmutableList.of(lazyPredicateSlot, lazyOutputSlot));

        Assertions.assertEquals(ImmutableList.of(orderKey, rowId), filterOutput);
    }
}
