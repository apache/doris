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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class ScopeTest {

    @Test
    void testFindRelationQualifiersIgnoreCase() {
        List<String> qualifier1 = ImmutableList.of("internal", "db", "t1");
        List<String> qualifier2 = ImmutableList.of("internal", "db", "t2");
        Scope scope = new Scope(ImmutableList.of(
                new SlotReference(new ExprId(1), "c1", IntegerType.INSTANCE, true, qualifier1),
                new SlotReference(new ExprId(2), "c2", IntegerType.INSTANCE, true, qualifier1),
                new SlotReference(new ExprId(3), "c1", IntegerType.INSTANCE, true, qualifier2),
                new SlotReference(new ExprId(4), "unqualified", IntegerType.INSTANCE, true, ImmutableList.of())));

        Assertions.assertEquals(ImmutableList.of(qualifier1),
                ImmutableList.copyOf(scope.findRelationQualifiersIgnoreCase("T1")));
        Assertions.assertEquals(ImmutableList.of(qualifier2),
                ImmutableList.copyOf(scope.findRelationQualifiersIgnoreCase("t2")));
        Assertions.assertTrue(scope.findRelationQualifiersIgnoreCase("unqualified").isEmpty());
    }
}
