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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

public class AddDefaultOrderByForLimitTest implements MemoPatternMatchSupported {
    @Test
    public void testAddDefaultOrderBy() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        LogicalPlan plan = new LogicalLimit<>(10, 5, LimitPhase.ORIGIN,
                new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.TRUE), scan));
        PlanChecker.from(MemoTestUtils.createCascadesContext(plan))
                .applyBottomUp(new AddDefaultOrderByForLimit())
                .matches(
                        logicalLimit(
                                logicalSort(
                                        logicalFilter(logicalOlapScan())
                                ).when(sort -> sort.getOrderKeys().size() == 2
                                        && ((Slot) sort.getOrderKeys().get(0).getExpr()).getName().equals("id")
                                        && ((Slot) sort.getOrderKeys().get(1).getExpr()).getName().equals("name"))
                        )
                );

        plan = new LogicalLimit<>(10, 5, LimitPhase.ORIGIN, scan);
        PlanChecker.from(MemoTestUtils.createCascadesContext(plan))
                .applyBottomUp(new AddDefaultOrderByForLimit())
                .matches(
                        logicalLimit(
                                logicalSort(
                                        logicalOlapScan()
                                ).when(sort -> sort.getOrderKeys().size() == 2
                                        && ((Slot) sort.getOrderKeys().get(0).getExpr()).getName().equals("id")
                                        && ((Slot) sort.getOrderKeys().get(1).getExpr()).getName().equals("name"))
                        )
                );
    }
}
