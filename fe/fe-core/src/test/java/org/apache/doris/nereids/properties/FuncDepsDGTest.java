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

import java.util.HashMap;
import java.util.Map;

class FuncDepsDGTest {
    @Test
    void testBasic() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s1, s2));
        Assertions.assertEquals(1, res.size());
    }

    @Test
    void testTrans() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s3 = new SlotReference("s3", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s3));
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s1, s3));
        System.out.println(res);
        Assertions.assertEquals(1, res.size());
    }

    @Test
    void testCircle() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s1));
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s1, s2));
        Assertions.assertEquals(2, res.size());
    }

    @Test
    void testTree() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s3 = new SlotReference("s3", IntegerType.INSTANCE);
        Slot s4 = new SlotReference("s4", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s3));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s4));
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s1, s4, s3));
        Assertions.assertEquals(2, res.size());
    }

    @Test
    void testPruneTrans() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s3 = new SlotReference("s3", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s3));
        dg.removeNotContain(Sets.newHashSet(s1, s3));
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s1, s3));
        System.out.println(res);
        Assertions.assertEquals(1, res.size());
    }

    @Test
    void testPruneCircle() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s3 = new SlotReference("s3", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s3));
        dg.addDeps(Sets.newHashSet(s3), Sets.newHashSet(s1));
        dg.removeNotContain(Sets.newHashSet(s1, s3));
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s1, s3));
        Assertions.assertEquals(2, res.size());
    }

    @Test
    void testPruneTree() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s3 = new SlotReference("s3", IntegerType.INSTANCE);
        Slot s4 = new SlotReference("s4", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s3));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s4));
        dg.removeNotContain(Sets.newHashSet(s1, s4, s3));
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s1, s4, s3));
        Assertions.assertEquals(2, res.size());
    }

    @Test
    void testReplaceTrans() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s3 = new SlotReference("s3", IntegerType.INSTANCE);
        Slot s5 = new SlotReference("s5", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s3));
        Map<Slot, Slot> replaceMap = new HashMap<>();
        replaceMap.put(s1, s5);
        dg.replace(replaceMap);
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s5, s3));
        System.out.println(res);
        Assertions.assertEquals(1, res.size());
    }

    @Test
    void testReplaceCircle() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s5 = new SlotReference("s5", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s1));
        Map<Slot, Slot> replaceMap = new HashMap<>();
        replaceMap.put(s1, s5);
        dg.replace(replaceMap);
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s5, s2));
        Assertions.assertEquals(2, res.size());
    }

    @Test
    void testReplaceTree() {
        FuncDepsDG.Builder dg = new FuncDepsDG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s3 = new SlotReference("s3", IntegerType.INSTANCE);
        Slot s4 = new SlotReference("s4", IntegerType.INSTANCE);
        Slot s5 = new SlotReference("s5", IntegerType.INSTANCE);
        dg.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s3));
        dg.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s4));
        Map<Slot, Slot> replaceMap = new HashMap<>();
        replaceMap.put(s1, s5);
        dg.replace(replaceMap);
        FuncDeps res = dg.build().findValidFuncDeps(Sets.newHashSet(s5, s4, s3));
        Assertions.assertEquals(2, res.size());
    }
}
