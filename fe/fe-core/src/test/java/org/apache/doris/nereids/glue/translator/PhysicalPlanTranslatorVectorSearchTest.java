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

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.tablefunction.TableValuedFunctionIf;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class PhysicalPlanTranslatorVectorSearchTest {
    @Test
    public void distanceOnlyProjectionKeepsCapturedPrefilterWithIndependentSlotIds() {
        VectorSearchTableValuedFunction function = Mockito.mock(VectorSearchTableValuedFunction.class);
        SlotReference id = new SlotReference("id", BigIntType.INSTANCE);
        SlotReference distance = new SlotReference("_distance", FloatType.INSTANCE);
        Mockito.when(function.getPrefilterSlots()).thenReturn(Collections.singletonList(id));
        List<Slot> first = PhysicalPlanTranslator.withVectorSearchPrefilterSlots(
                Collections.singletonList(distance), function);
        List<Slot> second = PhysicalPlanTranslator.withVectorSearchPrefilterSlots(
                Collections.singletonList(distance), function);
        Assertions.assertEquals(2, first.size());
        Assertions.assertSame(distance, first.get(0));
        Assertions.assertEquals("id", first.get(1).getName());
        Assertions.assertNotEquals(id.getExprId(), first.get(1).getExprId());
        Assertions.assertNotEquals(first.get(1).getExprId(), second.get(1).getExprId());
    }

    @Test
    public void requiredColumnAlreadyProjectedIsNotDuplicated() {
        VectorSearchTableValuedFunction function = Mockito.mock(VectorSearchTableValuedFunction.class);
        SlotReference id = new SlotReference("id", BigIntType.INSTANCE);
        Mockito.when(function.getPrefilterSlots()).thenReturn(Collections.singletonList(id));
        List<Slot> slots = PhysicalPlanTranslator.withVectorSearchPrefilterSlots(
                Collections.singletonList(id.withName("ID")), function);
        Assertions.assertEquals(1, slots.size());
        List<Slot> ordinary = Collections.singletonList(id);
        Assertions.assertSame(ordinary, PhysicalPlanTranslator.withVectorSearchPrefilterSlots(ordinary,
                Mockito.mock(TableValuedFunctionIf.class)));
    }
}
