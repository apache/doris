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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

class FuncDepsTest {
    Slot s1 = new SlotReference("1", IntegerType.INSTANCE, false);
    Slot s2 = new SlotReference("2", IntegerType.INSTANCE, false);
    Slot s3 = new SlotReference("3", IntegerType.INSTANCE, false);
    Slot s4 = new SlotReference("4", IntegerType.INSTANCE, false);
    Set<Slot> set1 = Sets.newHashSet(s1);
    Set<Slot> set2 = Sets.newHashSet(s2);
    Set<Slot> set3 = Sets.newHashSet(s3);
    Set<Slot> set4 = Sets.newHashSet(s4);

    @Test
    void testOneEliminate() {
        Set<Set<Slot>> slotSet = Sets.newHashSet(set1, set2, set3, set4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        Set<Set<Slot>> slots = funcDeps.eliminateDeps(slotSet, ImmutableSet.of());
        Set<Set<Slot>> expected = new HashSet<>();
        expected.add(set1);
        expected.add(set3);
        expected.add(set4);
        Assertions.assertEquals(expected, slots);
    }

    @Test
    void testChainEliminate() {
        Set<Set<Slot>> slotSet = Sets.newHashSet(set1, set2, set3, set4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s2), Sets.newHashSet(s3));
        funcDeps.addFuncItems(Sets.newHashSet(s3), Sets.newHashSet(s4));
        Set<Set<Slot>> slots = funcDeps.eliminateDeps(slotSet, ImmutableSet.of());
        Set<Set<Slot>> expected = new HashSet<>();
        expected.add(set1);
        Assertions.assertEquals(expected, slots);
    }

    @Test
    void testTreeEliminate() {
        Set<Set<Slot>> slotSet = Sets.newHashSet(set1, set2, set3, set4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s3));
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s4));
        Set<Set<Slot>> slots = funcDeps.eliminateDeps(slotSet, ImmutableSet.of());
        Set<Set<Slot>> expected = new HashSet<>();
        expected.add(set1);
        Assertions.assertEquals(expected, slots);
    }

    @Test
    void testCircleEliminate1() {
        Set<Set<Slot>> slotSet = Sets.newHashSet(set1, set2, set3, set4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s2), Sets.newHashSet(s1));
        Set<Set<Slot>> slots = funcDeps.eliminateDeps(slotSet, ImmutableSet.of());
        Set<Set<Slot>> expected = new HashSet<>();
        expected.add(set1);
        expected.add(set3);
        expected.add(set4);
        Assertions.assertEquals(expected, slots);
    }

    @Test
    void testCircleEliminate2() {
        Set<Set<Slot>> slotSet = Sets.newHashSet(set1, set2, set3, set4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s2), Sets.newHashSet(s3));
        funcDeps.addFuncItems(Sets.newHashSet(s3), Sets.newHashSet(s4));
        funcDeps.addFuncItems(Sets.newHashSet(s4), Sets.newHashSet(s1));
        Set<Set<Slot>> slots = funcDeps.eliminateDeps(slotSet, ImmutableSet.of());
        Set<Set<Slot>> expected = new HashSet<>();
        expected.add(set1);
        Assertions.assertEquals(expected, slots);
    }

    @Test
    void testGraphEliminate1() {
        Set<Set<Slot>> slotSet = Sets.newHashSet(set1, set2, set3, set4);
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s2));
        funcDeps.addFuncItems(Sets.newHashSet(s1), Sets.newHashSet(s3));
        funcDeps.addFuncItems(Sets.newHashSet(s3), Sets.newHashSet(s4));
        Set<Set<Slot>> slots = funcDeps.eliminateDeps(slotSet, ImmutableSet.of());
        Set<Set<Slot>> expected = new HashSet<>();
        expected.add(set1);
        Assertions.assertEquals(expected, slots);
    }
}
