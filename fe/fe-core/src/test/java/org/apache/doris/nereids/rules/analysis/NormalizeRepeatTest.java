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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

public class NormalizeRepeatTest implements PatternMatchSupported {
    @Mocked
    private CascadesContext cascadesContext;
    @Mocked
    private GroupPlan groupPlan;

    @Test
    public void testKeepNullableAfterNormalizeRepeat() {
        SlotReference slot1 = new SlotReference("a", IntegerType.INSTANCE, false);
        SlotReference slot2 = slot1.withNullable(true);
        SlotReference slot3 = new SlotReference("b", IntegerType.INSTANCE, true);
        Alias alias = new Alias(new Sum(slot3), "sum(b)");
        Plan plan = new LogicalRepeat<>(
                ImmutableList.of(ImmutableList.of(slot1)),
                ImmutableList.of(slot2, alias),
                groupPlan
        );
        PlanChecker.from(cascadesContext)
                .applyBottomUp(new NormalizeRepeat())
                .matchesFromRoot(
                        logicalRepeat().when(repeat -> repeat.getOutputExpressions().stream().allMatch(
                                ExpressionTrait::nullable))
                );
    }
}
