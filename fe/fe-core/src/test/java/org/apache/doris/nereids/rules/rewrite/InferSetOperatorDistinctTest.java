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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class InferSetOperatorDistinctTest {

    @Test
    void testInferDistinctForEachChildByNdv() {
        SlotReference leftSlot = new SlotReference("a", IntegerType.INSTANCE);
        LogicalOneRowRelation left = relationWithStats(0, leftSlot, 1000, 10);
        SlotReference rightSlot = new SlotReference("a", IntegerType.INSTANCE);
        LogicalOneRowRelation right = relationWithStats(1, rightSlot, 1000, 950);

        LogicalIntersect intersect = intersect(left, leftSlot, right, rightSlot);
        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), intersect)
                .applyTopDown(new InferSetOperatorDistinct())
                .getPlan();

        Assertions.assertInstanceOf(LogicalIntersect.class, rewritten);
        Assertions.assertInstanceOf(LogicalAggregate.class, rewritten.child(0));
        Assertions.assertInstanceOf(LogicalOneRowRelation.class, rewritten.child(0).child(0));
        Assertions.assertInstanceOf(LogicalOneRowRelation.class, rewritten.child(1));
    }

    private LogicalIntersect intersect(
            Plan left, SlotReference leftSlot, Plan right, SlotReference rightSlot) {
        List<NamedExpression> outputs = ImmutableList.of(leftSlot);
        List<List<SlotReference>> childrenOutputs = ImmutableList.of(
                ImmutableList.of(leftSlot),
                ImmutableList.of(rightSlot));
        return new LogicalIntersect(Qualifier.DISTINCT, outputs, childrenOutputs, ImmutableList.of(left, right));
    }

    private LogicalOneRowRelation relationWithStats(
            int relationId, SlotReference slot, double rowCount, double ndv) {
        LogicalOneRowRelation relation = new LogicalOneRowRelation(
                new RelationId(relationId), ImmutableList.of(slot));
        relation.setStatistics(new Statistics(rowCount, ImmutableMap.of(slot,
                new ColumnStatisticBuilder(rowCount).setNdv(ndv).setAvgSizeByte(4).build())));
        return relation;
    }
}
