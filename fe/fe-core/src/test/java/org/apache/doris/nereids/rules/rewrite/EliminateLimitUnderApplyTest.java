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

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

class EliminateLimitUnderApplyTest {

    @Test
    public void testEliminatePositiveLimitForCorrelatedExists() {
        assertLimitEliminated(1, 0, LogicalApply.SubQueryType.EXITS_SUBQUERY, false);
        assertLimitEliminated(5, 0, LogicalApply.SubQueryType.EXITS_SUBQUERY, false);
    }

    @Test
    public void testEliminatePositiveLimitForCorrelatedNotExists() {
        assertLimitEliminated(1, 0, LogicalApply.SubQueryType.EXITS_SUBQUERY, true);
    }

    @Test
    public void testKeepLimitZero() {
        assertLimitKept(0, 0, LogicalApply.SubQueryType.EXITS_SUBQUERY, false);
    }

    @Test
    public void testKeepLimitWithOffset() {
        assertLimitKept(1, 1, LogicalApply.SubQueryType.EXITS_SUBQUERY, false);
    }

    @Test
    public void testKeepLimitForInSubquery() {
        assertLimitKept(1, 0, LogicalApply.SubQueryType.IN_SUBQUERY, false);
    }

    @Test
    public void testKeepLimitForScalarSubquery() {
        assertLimitKept(1, 0, LogicalApply.SubQueryType.SCALAR_SUBQUERY, false);
    }

    @Test
    public void testKeepLimitForUncorrelatedApply() {
        LogicalApply<?, ?> apply = newApply(1, 0, LogicalApply.SubQueryType.EXITS_SUBQUERY, false, false);
        Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext(), apply)
                .applyTopDown(new EliminateLimitUnderApply())
                .getPlan();
        Assertions.assertTrue(plan instanceof LogicalApply);
        Assertions.assertTrue(((LogicalApply<?, ?>) plan).right() instanceof LogicalLimit);
    }

    private void assertLimitEliminated(long limit, long offset, LogicalApply.SubQueryType type, boolean isNot) {
        Plan plan = rewrite(limit, offset, type, isNot);
        Assertions.assertTrue(plan instanceof LogicalApply);
        Assertions.assertFalse(((LogicalApply<?, ?>) plan).right() instanceof LogicalLimit);
    }

    private void assertLimitKept(long limit, long offset, LogicalApply.SubQueryType type, boolean isNot) {
        Plan plan = rewrite(limit, offset, type, isNot);
        Assertions.assertTrue(plan instanceof LogicalApply);
        Assertions.assertTrue(((LogicalApply<?, ?>) plan).right() instanceof LogicalLimit);
    }

    private Plan rewrite(long limit, long offset, LogicalApply.SubQueryType type, boolean isNot) {
        LogicalApply<?, ?> apply = newApply(limit, offset, type, isNot, true);
        return PlanChecker.from(MemoTestUtils.createConnectContext(), apply)
                .applyTopDown(new EliminateLimitUnderApply())
                .getPlan();
    }

    private LogicalApply<?, ?> newApply(long limit, long offset, LogicalApply.SubQueryType type,
            boolean isNot, boolean correlated) {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        List<Slot> leftSlots = left.getOutput();
        List<Slot> rightSlots = right.getOutput();
        LogicalLimit<LogicalOlapScan> rightLimit = new LogicalLimit<>(limit, offset, LimitPhase.ORIGIN, right);
        EqualTo equalTo = new EqualTo(leftSlots.get(0), rightSlots.get(0));
        List<Slot> correlationSlots = correlated
                ? ImmutableList.of(leftSlots.get(0), rightSlots.get(0))
                : Collections.emptyList();
        Optional<Expression> compareExpr =
                type == LogicalApply.SubQueryType.IN_SUBQUERY ? Optional.of(equalTo) : Optional.empty();
        return new LogicalApply<>(correlationSlots, type, isNot, compareExpr, Optional.empty(),
                Optional.of(equalTo), Optional.empty(), false, false, left, rightLimit);
    }
}
