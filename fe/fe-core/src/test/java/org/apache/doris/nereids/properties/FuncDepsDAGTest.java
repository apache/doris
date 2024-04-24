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

class FuncDepsDAGTest {
    @Test
    void testBasic() {
        FuncDepsDAG.Builder dag = new FuncDepsDAG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        dag.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        FuncDeps res = dag.build().findValidFuncDeps(Sets.newHashSet(s1, s2));
        Assertions.assertEquals(1, res.size());
    }

    @Test
    void testTrans() {
        FuncDepsDAG.Builder dag = new FuncDepsDAG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s3 = new SlotReference("s3", IntegerType.INSTANCE);
        dag.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dag.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s3));
        FuncDeps res = dag.build().findValidFuncDeps(Sets.newHashSet(s1, s3));
        Assertions.assertEquals(1, res.size());
    }

    @Test
    void testCircle() {
        FuncDepsDAG.Builder dag = new FuncDepsDAG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        dag.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dag.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s1));
        FuncDeps res = dag.build().findValidFuncDeps(Sets.newHashSet(s1, s2));
        Assertions.assertEquals(2, res.size());
    }

    @Test
    void testTree() {
        FuncDepsDAG.Builder dag = new FuncDepsDAG.Builder();
        Slot s1 = new SlotReference("s1", IntegerType.INSTANCE);
        Slot s2 = new SlotReference("s2", IntegerType.INSTANCE);
        Slot s3 = new SlotReference("s3", IntegerType.INSTANCE);
        Slot s4 = new SlotReference("s4", IntegerType.INSTANCE);
        dag.addDeps(Sets.newHashSet(s1), Sets.newHashSet(s2));
        dag.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s3));
        dag.addDeps(Sets.newHashSet(s2), Sets.newHashSet(s4));
        FuncDeps res = dag.build().findValidFuncDeps(Sets.newHashSet(s1, s4, s3));
        Assertions.assertEquals(2, res.size());
    }
}
