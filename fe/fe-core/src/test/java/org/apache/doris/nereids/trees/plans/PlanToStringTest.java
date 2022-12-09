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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PlanToStringTest {

    @Test
    public void testLogicalLimit(@Mocked Plan child) {
        LogicalLimit<Plan> plan = new LogicalLimit<>(0, 0, child);

        Assertions.assertEquals("LogicalLimit ( limit=0, offset=0 )", plan.toString());
    }

    @Test
    public void testLogicalAggregate(@Mocked Plan child) {
        LogicalAggregate<Plan> plan = new LogicalAggregate<>(Lists.newArrayList(), ImmutableList.of(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())), child);

        Assertions.assertTrue(plan.toString()
                .matches("LogicalAggregate \\( phase=LOCAL, outputExpr=\\[a#\\d+], groupByExpr=\\[], hasRepeat=false \\)"));
    }

    @Test
    public void testLogicalFilter(@Mocked Plan child) {
        LogicalFilter<Plan> plan = new LogicalFilter<>(new EqualTo(Literal.of(1), Literal.of(1)), child);

        Assertions.assertEquals("LogicalFilter ( predicates=(1 = 1) )", plan.toString());
    }

    @Test
    public void testLogicalJoin(@Mocked Plan left, @Mocked Plan right) {
        LogicalJoin<Plan, Plan> plan = new LogicalJoin<>(JoinType.INNER_JOIN, Lists.newArrayList(
                new EqualTo(new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList()),
                        new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList()))),
                left, right);
        Assertions.assertTrue(plan.toString().matches(
                "LogicalJoin \\( type=INNER_JOIN, hashJoinConjuncts=\\[\\(a#\\d+ = b#\\d+\\)], otherJoinConjuncts=\\[] \\)"));
    }

    @Test
    public void testLogicalOlapScan() {
        LogicalOlapScan plan = PlanConstructor.newLogicalOlapScan(0, "table", 0);
        Assertions.assertTrue(
                plan.toString().matches("LogicalOlapScan \\( qualified=db\\.table, "
                        + "output=\\[id#\\d+, name#\\d+], candidateIndexIds=\\[], selectedIndexId=-1, preAgg=ON, pushAgg=NONE \\)"));
    }

    @Test
    public void testLogicalProject(@Mocked Plan child) {
        LogicalProject<Plan> plan = new LogicalProject<>(ImmutableList.of(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())), child);

        Assertions.assertTrue(plan.toString().matches("LogicalProject \\( projects=\\[a#\\d+], excepts=\\[] \\)"));
    }

    @Test
    public void testLogicalSort(@Mocked Plan child) {
        List<OrderKey> orderKeyList = Lists.newArrayList(
                new OrderKey(new SlotReference("col1", IntegerType.INSTANCE), true, true),
                new OrderKey(new SlotReference("col2", IntegerType.INSTANCE), true, true));

        LogicalSort<Plan> plan = new LogicalSort<>(orderKeyList, child);
        Assertions.assertTrue(plan.toString().matches("LogicalSort \\( orderKeys=\\[col1#\\d+, col2#\\d+] \\)"));
    }
}
