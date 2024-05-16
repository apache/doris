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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class FuncDepsTest {
    Slot s1 = new SlotReference("1", IntegerType.INSTANCE, false);
    Slot s2 = new SlotReference("2", IntegerType.INSTANCE, false);
    Slot s3 = new SlotReference("3", IntegerType.INSTANCE, false);
    Slot s4 = new SlotReference("4", IntegerType.INSTANCE, false);

    @Test
    void testOneEliminate() {
        Set<Slot> slotSet = Sets.newHashSet(s1, s2, s3, s4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        Set<Slot> slots = funcDeps.eliminateDeps(slotSet);
        Assertions.assertEquals(Sets.newHashSet(s1, s3, s4), slots);
    }

    @Test
    void testChainEliminate() {
        Set<Slot> slotSet = Sets.newHashSet(s1, s2, s3, s4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s2), Sets.newHashSet(s3));
        funcDeps.addFuncItems(Sets.newHashSet(s3), Sets.newHashSet(s4));
        Set<Slot> slots = funcDeps.eliminateDeps(slotSet);
        Assertions.assertEquals(Sets.newHashSet(s1), slots);
    }

    @Test
    void testTreeEliminate() {
        Set<Slot> slotSet = Sets.newHashSet(s1, s2, s3, s4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s3));
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s4));
        Set<Slot> slots = funcDeps.eliminateDeps(slotSet);
        Assertions.assertEquals(Sets.newHashSet(s1), slots);
    }

    @Test
    void testCircleEliminate1() {
        Set<Slot> slotSet = Sets.newHashSet(s1, s2, s3, s4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s2), Sets.newHashSet(s1));
        Set<Slot> slots = funcDeps.eliminateDeps(slotSet);
        Assertions.assertEquals(Sets.newHashSet(s2, s3, s4), slots);
    }

    @Test
    void testCircleEliminate2() {
        Set<Slot> slotSet = Sets.newHashSet(s1, s2, s3, s4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s2), Sets.newHashSet(s3));
        funcDeps.addFuncItems(Sets.newHashSet(s3), Sets.newHashSet(s4));
        funcDeps.addFuncItems(Sets.newHashSet(s4), Sets.newHashSet(s1));
        Set<Slot> slots = funcDeps.eliminateDeps(slotSet);
        Assertions.assertEquals(Sets.newHashSet(s3), slots);
    }

    @Test
    void testGraphEliminate1() {
        Set<Slot> slotSet = Sets.newHashSet(s1, s2, s3, s4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s3));
        funcDeps.addFuncItems(Sets.newHashSet(s3), Sets.newHashSet(s4));
        Set<Slot> slots = funcDeps.eliminateDeps(slotSet);
        Assertions.assertEquals(Sets.newHashSet(s1), slots);
    }
}
