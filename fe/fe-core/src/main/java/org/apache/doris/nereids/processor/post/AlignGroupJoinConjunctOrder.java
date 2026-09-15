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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.GroupJoinFusionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Reorder the equi-join conjuncts of a group-join-fusable Aggregate(HashJoin) so that conjunct i
 * produces the group-by key at position i.
 * <p>
 * The fused GroupJoin operator groups rows by the shared hash key and materializes one
 * grouping-key column per equi-join conjunct: the BE writes the j-th conjunct's key into the
 * j-th output tuple slot, and the FE creates the output tuple slots from the aggregate's
 * group-by expressions in list order. The returned columns are therefore correct iff the
 * conjuncts are listed in exactly the group-by order. Since GROUP BY is unordered semantically
 * while the join's conjunct order has no upstream positional consumers (runtime filters are
 * generated after this processor, so their expr_order follows the reordered list), any GROUP BY
 * that merely permutes the join keys can still be fused - the join's own conjunct list is
 * reordered here, once, at plan level. Eligibility is decided by
 * {@link GroupJoinFusionUtils#alignedConjunctsForGroupJoin}, the same single source of truth the
 * translator's fusion decision uses, so only joins that will actually fuse are reordered. This
 * processor runs before runtime-filter generation; the translator then emits the conjuncts in
 * the join's (already aligned) order and its fusion gate keeps any shape this processor did not
 * align on the regular HashJoinNode + AggregationNode path.
 */
public class AlignGroupJoinConjunctOrder extends PlanPostProcessor {

    @Override
    public Plan visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> aggregate,
            CascadesContext ctx) {
        Plan rewritten = super.visit(aggregate, ctx);
        if (!(rewritten instanceof PhysicalHashAggregate)) {
            return rewritten;
        }
        aggregate = (PhysicalHashAggregate<? extends Plan>) rewritten;

        // Registered only when enable_group_join_fusion is on; fusion is additionally disabled
        // when spill is on, so no reordering is needed in that case either.
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null || connectContext.getSessionVariable().enableSpill) {
            return aggregate;
        }

        // Mirror the fusion shape: aggregate directly over a hash join (optionally through one
        // project).
        Plan child = aggregate.child();
        PhysicalProject<? extends Plan> project = null;
        if (child instanceof PhysicalProject) {
            project = (PhysicalProject<? extends Plan>) child;
            child = child.child(0);
        }
        if (!(child instanceof PhysicalHashJoin)) {
            return aggregate;
        }
        PhysicalHashJoin<?, ?> join = (PhysicalHashJoin<?, ?>) child;

        // Same eligibility as the fusion decision; when the GROUP BY merely permutes the join
        // keys, align the join's conjunct order with it. The intermediate project (if any) is
        // passed through so the shared gate enforces the same pure-passthrough rule as the
        // translator: an aligned-but-computing project would not fuse anyway.
        List<Expression> alignedConjuncts = GroupJoinFusionUtils.alignedConjunctsForGroupJoin(
                aggregate, project, join);
        if (alignedConjuncts == null
                || GroupJoinFusionUtils.sameConjunctOrder(
                        alignedConjuncts, join.getHashJoinConjuncts())) {
            return aggregate;
        }

        PhysicalHashJoin<Plan, Plan> reorderedJoin = copyWithConjuncts(join, alignedConjuncts);
        Plan newChild = reorderedJoin;
        if (project != null) {
            newChild = project.withChildren(ImmutableList.of(newChild));
        }
        return aggregate.withChildren(ImmutableList.of(newChild));
    }

    /** Rebuild the join with the reordered conjunct list, keeping id, properties and stats. */
    private static PhysicalHashJoin<Plan, Plan> copyWithConjuncts(
            PhysicalHashJoin<?, ?> join, List<Expression> reorderedConjuncts) {
        PhysicalHashJoin<Plan, Plan> reorderedJoin = AbstractPlan.copyWithSameId(join,
                () -> new PhysicalHashJoin<>(
                        join.getJoinType(), reorderedConjuncts, join.getOtherJoinConjuncts(),
                        join.getMarkJoinConjuncts(), join.getDistributeHint(),
                        join.getMarkJoinSlotReference(), join.getLogicalProperties(),
                        join.left(), join.right()));
        return (PhysicalHashJoin<Plan, Plan>) reorderedJoin.copyStatsAndGroupIdFrom(
                (AbstractPhysicalJoin) join);
    }
}
