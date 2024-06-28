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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class NormalizeRepeatTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    public void testKeepNullableAfterNormalizeRepeat() {
        Slot id = scan1.getOutput().get(0);
        Slot idNotNull = id.withNullable(true);
        Slot name = scan1.getOutput().get(1);
        Alias alias = new Alias(new Sum(name), "sum(name)");
        Plan plan = new LogicalRepeat<>(
                ImmutableList.of(ImmutableList.of(id), ImmutableList.of(name)),
                ImmutableList.of(idNotNull, alias),
                scan1
        );
        PlanChecker.from(MemoTestUtils.createCascadesContext(plan))
                .applyTopDown(new NormalizeRepeat())
                .matches(
                        logicalRepeat().when(repeat -> repeat.getOutputExpressions().get(0).nullable())
                );
    }

    @Test
    public void testEliminateRepeat() {
        Slot id = scan1.getOutput().get(0);
        Slot idNotNull = id.withNullable(true);
        Slot name = scan1.getOutput().get(1);
        Alias alias = new Alias(new Sum(name), "sum(name)");
        Plan plan = new LogicalRepeat<>(
                ImmutableList.of(ImmutableList.of(id)),
                ImmutableList.of(idNotNull, alias),
                scan1
        );
        PlanChecker.from(MemoTestUtils.createCascadesContext(plan))
                .applyTopDown(new NormalizeRepeat())
                .matchesFromRoot(
                        logicalAggregate(logicalOlapScan())
                );
    }

    @Test
    public void testNoEliminateRepeat() {
        Slot id = scan1.getOutput().get(0);
        Slot idNotNull = id.withNullable(true);
        Slot name = scan1.getOutput().get(1);
        Alias alias = new Alias(new GroupingId(name), "grouping_id(name)");
        Plan plan = new LogicalRepeat<>(
                ImmutableList.of(ImmutableList.of(id)),
                ImmutableList.of(idNotNull, alias),
                scan1
        );
        PlanChecker.from(MemoTestUtils.createCascadesContext(plan))
                .applyTopDown(new NormalizeRepeat())
                .matchesFromRoot(
                        logicalAggregate(logicalRepeat(logicalOlapScan()))
                );
    }
}
