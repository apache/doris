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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import java.util.Set;

/** Applies the shared runtime filter pushdown rules before dispatching to each physical operator. */
abstract class AbstractRuntimeFilterPushDownVisitor<C> extends PlanVisitor<Boolean, C> {

    /** Starts or continues pushdown. Every recursive step must use this entry point. */
    public final boolean pushDown(Plan plan, C context) {
        return !blocksRuntimeFilterPushDown(plan, getProbeExpression(context)) && plan.accept(this, context);
    }

    protected abstract Expression getProbeExpression(C context);

    private static boolean blocksRuntimeFilterPushDown(Plan plan, Expression probe) {
        // Filtering the input could hide a cardinality error or change which rows are selected.
        if (plan instanceof PhysicalAssertNumRows || plan instanceof PhysicalLimit || plan instanceof PhysicalTopN) {
            return true;
        }
        Set<Slot> probeSlots = probe.getInputSlots();
        if (plan instanceof PhysicalPartitionTopN) {
            PhysicalPartitionTopN<?> topN = (PhysicalPartitionTopN<?>) plan;
            // Removing a whole partition preserves the choices of the other partitions.
            if (topN.hasGlobalLimit() || !topN.getPartitionKeys().containsAll(probeSlots)) {
                return true;
            }
        }
        if (plan instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<?> aggregate = (PhysicalHashAggregate<?>) plan;
            if (aggregate.getGroupByExpressions().isEmpty()
                    || !aggregate.getGroupByExpressions().containsAll(probeSlots)) {
                return true;
            }
        }
        if (plan instanceof PhysicalRepeat
                && !((PhysicalRepeat<?>) plan).getCommonGroupingSetExpressions().containsAll(probeSlots)) {
            // Repeat erases keys absent from a grouping set to NULL.
            return true;
        }

        return hasSensitiveExpressions(plan)
                || (plan instanceof PhysicalRelation && probe.containsVolatileOrNoneMovableExpression());
    }

    static boolean hasSensitiveExpressions(Plan plan) {
        // A scan filter can remove rows before any expression on this operator is evaluated.
        // For example, assert_true on an unrelated Project output must still see those rows.
        return plan.getExpressions().stream().anyMatch(Expression::containsVolatileOrNoneMovableExpression);
    }

    @Override
    public final Boolean visitPhysicalStorageLayerAggregate(PhysicalStorageLayerAggregate aggregate, C context) {
        return pushDown(aggregate.getRelation(), context);
    }
}
