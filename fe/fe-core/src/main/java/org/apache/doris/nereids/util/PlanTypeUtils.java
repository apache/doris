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

package org.apache.doris.nereids.util;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.plans.logical.*;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Judgment for plan type.
 */
public class PlanTypeUtils {
    private static final Set<Class<? extends LogicalPlan>> SPJ_PLAN = ImmutableSet.of(
        LogicalRelation.class,
        LogicalJoin.class,
        LogicalFilter.class,
        LogicalProject.class,
        LogicalSubQueryAlias.class // FIXME
    );
    private static final Set<Class<? extends LogicalPlan>> SUPPORTED_PLAN = ImmutableSet.of(
        // TODO: Set related ops
        LogicalRelation.class,
        LogicalJoin.class,
        LogicalFilter.class,
        LogicalProject.class,
        LogicalWindow.class,
        LogicalAggregate.class,
        LogicalHaving.class,
        LogicalSort.class,
        LogicalLimit.class,
        LogicalSubQueryAlias.class
    );

    public static boolean isSpj(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> SPJ_PLAN.stream().anyMatch(c -> c.isInstance(p)));
    }

    public static boolean isSpj(List<LogicalPlan> plans) {
        return plans.stream().anyMatch(p -> SPJ_PLAN.stream().anyMatch(c -> c.isInstance(p)));
    }

    public static boolean isSupportedPlan(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> SUPPORTED_PLAN.stream().anyMatch(c -> c.isInstance(p)));
    }

    public static boolean isSupportedPlan(List<LogicalPlan> plans) {
        return plans.stream().anyMatch(p -> SUPPORTED_PLAN.stream().anyMatch(c -> c.isInstance(p)));
    }
    public static LogicalFilter getSpjFilter(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalFilter.class::isInstance));
        return ((LogicalFilter) plans.get(0));
    }

    public static boolean hasFrom(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalRelation);
    }

    public static boolean hasJoin(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalJoin);
    }

    public static boolean hasFilter(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalFilter);
    }
    public static boolean hasAggr(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalAggregate);
    }

    public static boolean hasHaving(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalHaving);
    }

    public static boolean hasSetOp(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalSetOperation);
    }

    public static boolean hasOrderBy(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalSort);
    }

    public static boolean hasLimit(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalLimit);
    }

    public static boolean hasWinfunc(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalWindow);
    }

    public static boolean hasDistinct(LogicalPlan rootPlan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(rootPlan.collect(LogicalLimit.class::isInstance));
        return plans.stream().anyMatch(p -> p instanceof LogicalProject && ((LogicalProject) p).isDistinct());
    }
}
