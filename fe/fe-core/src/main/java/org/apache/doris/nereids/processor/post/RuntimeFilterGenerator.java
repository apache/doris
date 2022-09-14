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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * generate runtime filter
 */
public class RuntimeFilterGenerator extends PlanPostProcessor {

    private final IdGenerator<RuntimeFilterId> generator = RuntimeFilterId.createGenerator();

    private final ImmutableSet<JoinType> deniedJoinType = ImmutableSet.of(
            JoinType.LEFT_ANTI_JOIN,
            JoinType.RIGHT_ANTI_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_OUTER_JOIN
    );

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
        if (deniedJoinType.contains(join.getJoinType())) {
            /* TODO: translate left outer join to inner join if there are inner join ancestors
             * if it has encountered inner join, like
             *                       a=b
             *                      /   \
             *                     /     \
             *                    /       \
             *                   /         \
             *      left join-->a=c         b
             *                  / \
             *                 /   \
             *                /     \
             *               /       \
             *              a         c
             * runtime filter whose src expr is b can take effect on c.
             * but now checking the inner join is unsupported. we may support it at later version.
             */
            join.getOutput().forEach(slot -> ctx.removeFilters(slot.getExprId()));
        } else {
            List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values()).filter(type ->
                    (type.getValue() & ctx.getSessionVariable().getRuntimeFilterType()) > 0)
                    .collect(Collectors.toList());
            AtomicInteger cnt = new AtomicInteger();
            join.getHashJoinConjuncts().stream()
                    .map(EqualTo.class::cast)
                    .peek(expr -> {
                        // target is always the expr at the two side of equal of hash conjunctions.
                        List<SlotReference> slots = expr.children().stream().filter(SlotReference.class::isInstance)
                                .map(SlotReference.class::cast).collect(Collectors.toList());
                        if (slots.size() != 2
                                || !(ctx.checkExistValue(ctx.getTargetExprIdToFilter(), slots.get(0).getExprId())
                                || ctx.checkExistValue(ctx.getTargetExprIdToFilter(), slots.get(1).getExprId()))) {
                            return;
                        }
                        int tag = ctx.checkExistValue(ctx.getTargetExprIdToFilter(), slots.get(0).getExprId()) ? 0 : 1;
                        ctx.setKVInNormalMap(ctx.getTargetExprIdToFilter(), slots.get(tag ^ 1).getExprId(),
                                ctx.getFiltersByTargetExprId(slots.get(tag).getExprId()).stream()
                                        .map(filter -> new RuntimeFilter(generator.getNextId(), filter.getSrcExpr(),
                                        slots.get(tag ^ 1), filter.getType(), filter.getExprOrder()))
                                        .collect(Collectors.toList()));
                    })
                    .forEach(expr -> legalTypes.stream()
                            .map(type -> RuntimeFilter.createRuntimeFilter(generator.getNextId(), expr,
                                    type, cnt.getAndIncrement(), join))
                            .filter(Objects::nonNull)
                            .forEach(filter -> ctx.setTargetExprIdToFilters(
                                    filter.getTargetExpr().getExprId(), filter)));
        }
        join.left().accept(this, context);
        join.right().accept(this, context);
        return join;
    }

    // TODO: support src key is agg slot.
    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        project.getProjects().stream().filter(Alias.class::isInstance)
                .map(Alias.class::cast)
                .filter(expr -> expr.child() instanceof SlotReference)
                .forEach(expr -> ctx.setKVInNormalMap(ctx.getAliasChildToSelf(), ((SlotReference) expr.child()), expr));
        project.child().accept(this, context);
        return project;
    }

    @Override
    public PhysicalOlapScan visitPhysicalOlapScan(PhysicalOlapScan scan, CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        scan.getOutput().stream()
                .filter(slot -> ctx.getSlotListOfTheSameSlotAtOlapScanNode(slot).stream()
                        .filter(expr -> ctx.checkExistValue(ctx.getTargetExprIdToFilter(), expr.getExprId()))
                        .peek(expr -> {
                            List<RuntimeFilter> filters = ctx.getFiltersByTargetExprId(expr.getExprId());
                            ctx.removeFilters(expr.getExprId());
                            filters.forEach(filter -> filter.setTargetSlot(slot));
                            ctx.setKVInNormalMap(ctx.getTargetExprIdToFilter(), slot.getExprId(), filters);
                            ctx.setKVInNormalMap(ctx.getHashJoinExprToOlapScanSlot(), expr.getExprId(), slot);
                        })
                        .count() > 0)
                .forEach(slot -> ctx.setTargetsOnScanNode(scan.getId(), slot));
        return scan;
    }
}
