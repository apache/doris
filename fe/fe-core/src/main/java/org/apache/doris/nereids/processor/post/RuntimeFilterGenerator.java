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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * generate runtime filter
 */
public class RuntimeFilterGenerator extends PlanPostProcessor {
    private static final ImmutableSet<JoinType> deniedJoinType = ImmutableSet.of(
            JoinType.LEFT_ANTI_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_OUTER_JOIN
    );
    private final IdGenerator<RuntimeFilterId> generator = RuntimeFilterId.createGenerator();

    /**
     * the runtime filter generator run at the phase of post process and plan translation of nereids planner.
     * post process:
     * first step: if encounter supported join type, generate nereids runtime filter for all the hash conjunctions
     * and make association from exprId of the target slot references to the runtime filter. or delete the runtime
     * filter whose target slot reference is one of the output slot references of the left child of the physical join as
     * the runtime filter.
     * second step: if encounter project, collect the association of its child and it for pushing down through
     * the project node.
     * plan translation:
     * third step: generate nereids runtime filter target at olap scan node fragment.
     * forth step: generate legacy runtime filter target and runtime filter at hash join node fragment.
     */
    // TODO: current support inner join, cross join, right outer join, and will support more join type.
    @Override
    public PhysicalPlan visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<RelationId, NamedExpression>> aliasTransferMap = ctx.getAliasTransferMap();
        join.right().accept(this, context);
        join.left().accept(this, context);
        if (deniedJoinType.contains(join.getJoinType())) {
            // copy to avoid bug when next call of getOutputSet()
            Set<Slot> slots = join.getOutputSet();
            slots.forEach(aliasTransferMap::remove);
        } else {
            List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values())
                    .filter(type -> (type.getValue() & ctx.getSessionVariable().getRuntimeFilterType()) > 0)
                    .collect(Collectors.toList());
            AtomicInteger cnt = new AtomicInteger();
            join.getHashJoinConjuncts().stream()
                    .map(EqualTo.class::cast)
                    // TODO: some complex situation cannot be handled now, see testPushDownThroughJoin.
                    // TODO: we will support it in later version.
                    .forEach(expr -> legalTypes.forEach(type -> {
                        Pair<Expression, Expression> normalizedChildren = checkAndMaybeSwapChild(expr, join);
                        // aliasTransMap doesn't contain the key, means that the path from the olap scan to the join
                        // contains join with denied join type. for example: a left join b on a.id = b.id
                        if (normalizedChildren == null
                                || !aliasTransferMap.containsKey((Slot) normalizedChildren.first)) {
                            return;
                        }
                        Pair<Slot, Slot> slots = Pair.of(
                                aliasTransferMap.get((Slot) normalizedChildren.first).second.toSlot(),
                                ((Slot) normalizedChildren.second));
                        RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                                slots.second, slots.first, type,
                                cnt.getAndIncrement(), join);
                        ctx.setTargetExprIdToFilter(slots.first.getExprId(), filter);
                        ctx.setTargetsOnScanNode(
                                aliasTransferMap.get((Slot) normalizedChildren.first).first,
                                slots.first);
                    }));
        }
        return join;
    }

    // TODO: support src key is agg slot.
    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        project.child().accept(this, context);
        Map<NamedExpression, Pair<RelationId, NamedExpression>> aliasTransferMap
                = context.getRuntimeFilterContext().getAliasTransferMap();
        // change key when encounter alias.
        project.getProjects().stream().filter(Alias.class::isInstance)
                .map(Alias.class::cast)
                .filter(alias -> alias.child() instanceof NamedExpression
                        && aliasTransferMap.containsKey((NamedExpression) alias.child()))
                .forEach(alias -> {
                    NamedExpression child = ((NamedExpression) alias.child());
                    aliasTransferMap.put(alias.toSlot(), aliasTransferMap.remove(child));
                });
        return project;
    }

    @Override
    public PhysicalOlapScan visitPhysicalOlapScan(PhysicalOlapScan scan, CascadesContext context) {
        // add all the slots in map.
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        scan.getOutput().forEach(slot -> ctx.getAliasTransferMap().put(slot, Pair.of(scan.getId(), slot)));
        return scan;
    }

    private static Pair<Expression, Expression> checkAndMaybeSwapChild(EqualTo expr,
            PhysicalHashJoin<? extends Plan, ? extends Plan> join) {
        if (expr.child(0).equals(expr.child(1))
                || !expr.children().stream().allMatch(SlotReference.class::isInstance)) {
            return null;
        }
        // current we assume that there are certainly different slot reference in equal to.
        // they are not from the same relation.
        List<Expression> children = JoinUtils.swapEqualToForChildrenOrder(expr, join.left().getOutputSet()).children();
        return Pair.of(children.get(0), children.get(1));
    }
}
