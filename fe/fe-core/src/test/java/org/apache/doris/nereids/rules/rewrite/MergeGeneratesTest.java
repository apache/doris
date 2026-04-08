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

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MergeGenerates}.
 */
class MergeGeneratesTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testMergeGenerates() {
        // bottom generate
        SlotReference bottomOut = new SlotReference("g1", IntegerType.INSTANCE);
        Unnest bottomGen = new Unnest(new IntegerLiteral(1));
        LogicalGenerate<LogicalPlan> bottom = new LogicalGenerate<>(
                ImmutableList.of(bottomGen), ImmutableList.of(bottomOut), scan);

        // top generate (does not reference bottom output and has empty conjuncts)
        SlotReference topOut = new SlotReference("g2", IntegerType.INSTANCE);
        Unnest topGen = new Unnest(new IntegerLiteral(2));
        LogicalGenerate<LogicalPlan> top = new LogicalGenerate<>(
                ImmutableList.of(topGen), ImmutableList.of(topOut), bottom);

        PlanChecker.from(new ConnectContext(), top).applyBottomUp(new MergeGenerates())
                .matches(logicalGenerate().when(g -> g.getGenerators().size() == 2
                        && g.getGeneratorOutput().size() == 2
                        && !(g.child(0) instanceof LogicalGenerate)));
    }

    @Test
    void testDoNotMergeWhenTopHasConjuncts() {
        SlotReference bottomOut = new SlotReference("g1", IntegerType.INSTANCE);
        Unnest bottomGen = new Unnest(new IntegerLiteral(1));
        LogicalGenerate<LogicalPlan> bottom = new LogicalGenerate<>(
                ImmutableList.of(bottomGen), ImmutableList.of(bottomOut), scan);

        // top has a non-empty conjuncts list -> should NOT merge
        SlotReference topOut = new SlotReference("g2", IntegerType.INSTANCE);
        Unnest topGen = new Unnest(new IntegerLiteral(2));
        LogicalGenerate<LogicalPlan> top = new LogicalGenerate<>(
                ImmutableList.of(topGen), ImmutableList.of(topOut), ImmutableList.of(ImmutableList.of("xx")),
                ImmutableList.of(BooleanLiteral.TRUE), bottom);

        PlanChecker.from(new ConnectContext(), top).applyBottomUp(new MergeGenerates())
                .printlnAllTree()
                .matches(logicalGenerate(logicalGenerate()));
    }

    @Test
    void testDoNotMergeWhenBottomHasConjuncts() {
        SlotReference bottomOut = new SlotReference("g1", IntegerType.INSTANCE);
        Unnest bottomGen = new Unnest(new IntegerLiteral(1));
        LogicalGenerate<LogicalPlan> bottom = new LogicalGenerate<>(
                ImmutableList.of(bottomGen), ImmutableList.of(bottomOut), ImmutableList.of(ImmutableList.of("xx")),
                ImmutableList.of(BooleanLiteral.TRUE), scan);

        // top has a non-empty conjuncts list -> should NOT merge
        SlotReference topOut = new SlotReference("g2", IntegerType.INSTANCE);
        Unnest topGen = new Unnest(new IntegerLiteral(2));
        LogicalGenerate<LogicalPlan> top = new LogicalGenerate<>(
                ImmutableList.of(topGen), ImmutableList.of(topOut), bottom);

        PlanChecker.from(new ConnectContext(), top).applyBottomUp(new MergeGenerates())
                .printlnAllTree()
                .matches(logicalGenerate(logicalGenerate()));
    }

    @Test
    void testDoNotMergeWhenTopUsesBottomOutput() {
        // bottom generate defines an output slot
        SlotReference bottomOut = new SlotReference("g_shared", IntegerType.INSTANCE);
        Unnest bottomGen = new Unnest(new IntegerLiteral(1));
        LogicalGenerate<LogicalPlan> bottom = new LogicalGenerate<>(
                ImmutableList.of(bottomGen), ImmutableList.of(bottomOut), scan);

        // top generator references bottom's output as its argument -> should NOT merge
        SlotReference topOut = new SlotReference("g2", IntegerType.INSTANCE);
        Unnest topGen = new Unnest(bottomOut);
        LogicalGenerate<LogicalPlan> top = new LogicalGenerate<>(
                ImmutableList.of(topGen), ImmutableList.of(topOut), bottom);

        PlanChecker.from(new ConnectContext(), top).applyBottomUp(new MergeGenerates())
                .matches(logicalGenerate(logicalGenerate()));
    }
}
