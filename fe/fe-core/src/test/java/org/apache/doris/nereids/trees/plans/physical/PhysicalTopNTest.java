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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.BigIntType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PhysicalTopNTest {
    @Test
    public void testEquals() {
        LogicalOneRowRelation oneRowRelation
                = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(new Alias(Literal.of(1))));
        SlotReference a = new SlotReference(new ExprId(0), "a",
                BigIntType.INSTANCE, true, Lists.newArrayList());
        List<OrderKey> orderKeysA = Lists.newArrayList();
        orderKeysA.add(new OrderKey(a, true, true));
        PhysicalTopN topn1 = new PhysicalTopN(orderKeysA, 1, 1, SortPhase.LOCAL_SORT,
                null, oneRowRelation);
        PhysicalTopN topn2 = new PhysicalTopN(orderKeysA, 1, 1, SortPhase.GATHER_SORT,
                null, oneRowRelation);
        Assertions.assertNotEquals(topn1, topn2);

        SlotReference b = new SlotReference(new ExprId(0), "b",
                BigIntType.INSTANCE, true, Lists.newArrayList());
        List<OrderKey> orderKeysB = Lists.newArrayList();
        orderKeysB.add(new OrderKey(b, true, true));
        PhysicalTopN topn3 = new PhysicalTopN(orderKeysB, 1, 1, SortPhase.LOCAL_SORT,
                null, oneRowRelation);
        Assertions.assertNotEquals(topn2, topn3);
    }
}
