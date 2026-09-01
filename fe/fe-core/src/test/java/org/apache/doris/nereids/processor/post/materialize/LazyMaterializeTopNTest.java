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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class LazyMaterializeTopNTest {

    @Test
    public void testAliasIsRequiredWhenItsBaseSlotIsMaterialized() {
        SlotReference baseSlot = new SlotReference("base", IntegerType.INSTANCE);
        Slot aliasSlot = new Alias(baseSlot, "alias").toSlot();
        SlotReference independentBaseSlot = new SlotReference("independent", IntegerType.INSTANCE);
        Slot independentAliasSlot = new Alias(independentBaseSlot, "independent_alias").toSlot();
        Relation relation = Mockito.mock(Relation.class);
        Map<Slot, MaterializeSource> materializeMap = ImmutableMap.of(
                aliasSlot, new MaterializeSource(relation, baseSlot),
                independentAliasSlot, new MaterializeSource(relation, independentBaseSlot));

        List<Slot> requiredOutputSlots = LazyMaterializeTopN.collectRequiredOutputSlots(
                materializeMap, ImmutableSet.of(), ImmutableSet.of(baseSlot));

        Assertions.assertEquals(ImmutableList.of(aliasSlot), requiredOutputSlots);
    }
}
