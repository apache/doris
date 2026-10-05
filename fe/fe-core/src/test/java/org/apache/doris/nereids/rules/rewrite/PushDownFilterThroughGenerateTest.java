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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PushDownFilterThroughGenerate}.
 */
class PushDownFilterThroughGenerateTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    /** Deterministic filter whose slots only reference generate's child output should be pushed down. */
    @Test
    void testPushDownDeterministicFilter() {
        SlotReference idSlot = (SlotReference) scan.getOutput().get(0);
        SlotReference genOut = new SlotReference("g1", IntegerType.INSTANCE);
        Unnest gen = new Unnest(new IntegerLiteral(1));
        LogicalGenerate<LogicalPlan> generate = new LogicalGenerate<>(
                ImmutableList.of(gen), ImmutableList.of(genOut), scan);

        Expression predicate = new EqualTo(idSlot, new IntegerLiteral(1));
        LogicalFilter<LogicalPlan> filter = new LogicalFilter<>(ImmutableSet.of(predicate), generate);

        PlanChecker.from(new ConnectContext(), filter)
                .applyTopDown(new PushDownFilterThroughGenerate())
                .matchesFromRoot(
                        logicalGenerate(
                                logicalFilter(logicalOlapScan())
                                        .when(f -> f.getConjuncts().contains(predicate))
                        )
                );
    }

    /** Filter containing a unique (non-idempotent) function must NOT be pushed through generate. */
    @Test
    void testDoNotPushUniqueFunctionThroughGenerate() {
        SlotReference idSlot = (SlotReference) scan.getOutput().get(0);
        SlotReference genOut = new SlotReference("g1", IntegerType.INSTANCE);
        Unnest gen = new Unnest(new IntegerLiteral(1));
        LogicalGenerate<LogicalPlan> generate = new LogicalGenerate<>(
                ImmutableList.of(gen), ImmutableList.of(genOut), scan);

        // random() depends only on idSlot-like pattern but contains a UniqueFunction.
        // Use EqualTo(id, random()) so the conjunct's input slots are a subset of scan output
        // (would otherwise satisfy the push-down condition).
        Expression predicate = new EqualTo(idSlot, new Random());
        LogicalFilter<LogicalPlan> filter = new LogicalFilter<>(ImmutableSet.of(predicate), generate);

        PlanChecker.from(new ConnectContext(), filter)
                .applyTopDown(new PushDownFilterThroughGenerate())
                .matchesFromRoot(
                        logicalFilter(
                                logicalGenerate(logicalOlapScan())
                        ).when(f -> f.getConjuncts().contains(predicate))
                );
    }
}
