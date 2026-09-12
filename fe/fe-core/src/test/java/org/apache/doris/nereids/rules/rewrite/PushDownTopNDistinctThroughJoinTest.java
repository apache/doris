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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

class PushDownTopNDistinctThroughJoinTest {
    private static final PushDownTopNDistinctThroughJoin RULE = new PushDownTopNDistinctThroughJoin();
    private static final LogicalOlapScan LEFT_SCAN = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan RIGHT_SCAN = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @BeforeEach
    void setUp() {
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void rejectNonUniquePartialOrderKeys() {
        Assertions.assertTrue(pushedOrderKeys(LEFT_SCAN, RIGHT_SCAN,
                LEFT_SCAN.getOutput().get(0), RIGHT_SCAN.getOutput().get(0)).isEmpty());
        Assertions.assertTrue(pushedOrderKeys(RIGHT_SCAN, LEFT_SCAN,
                RIGHT_SCAN.getOutput().get(0), LEFT_SCAN.getOutput().get(0)).isEmpty());
    }

    @Test
    void pushCompleteOrderKeysFromOneSide() {
        List<OrderKey> pushed = pushedOrderKeys(LEFT_SCAN, RIGHT_SCAN,
                LEFT_SCAN.getOutput().get(0), LEFT_SCAN.getOutput().get(1));
        Assertions.assertEquals(2, pushed.size());
    }

    @Test
    void pushPartialOrderKeysCoveringDistinctChildOutput() {
        List<OrderKey> pushed = pushedOrderKeys(LEFT_SCAN, RIGHT_SCAN,
                LEFT_SCAN.getOutput().get(0), LEFT_SCAN.getOutput().get(1), RIGHT_SCAN.getOutput().get(0));
        Assertions.assertEquals(2, pushed.size());
    }

    @Test
    void pushPartialOrderKeysFunctionallyDeterminingDistinctChildOutput() {
        NamedExpression id = LEFT_SCAN.getOutput().get(0);
        Alias idCopy = new Alias(id, "id_copy");
        LogicalProject<LogicalOlapScan> left = new LogicalProject<>(ImmutableList.of(id, idCopy), LEFT_SCAN);

        List<OrderKey> pushed = pushedOrderKeys(left, RIGHT_SCAN,
                left.getOutput().get(0), RIGHT_SCAN.getOutput().get(0));
        Assertions.assertEquals(1, pushed.size());
    }

    private List<OrderKey> pushedOrderKeys(Plan child, Plan otherChild, Expression... orderExpressions) {
        Set<Slot> groupBySlots = ImmutableSet.<Slot>builder()
                .addAll(child.getOutput())
                .addAll(otherChild.getOutput())
                .build();
        List<OrderKey> orderKeys = ImmutableList.copyOf(orderExpressions).stream()
                .map(expression -> new OrderKey(expression, true, true))
                .collect(ImmutableList.toImmutableList());
        return RULE.getPushedOrderKeys(groupBySlots, child, orderKeys);
    }
}
