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

import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.RuntimeFilterGenerator.FilterSizeLimits;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * generate runtime filter
 */
public class RuntimeFilterGenerator extends PlanPostprocessor {

    private final IdGenerator<RuntimeFilterId> generator = RuntimeFilterId.createGenerator();

    private Map<ExprId, List<RuntimeFilter>> filtersByExprId = Maps.newHashMap();

    private final Map<ExprId, List<RuntimeFilter.RuntimeFilterTarget>> filterTargetByExprId = Maps.newHashMap();

    private final List<org.apache.doris.planner.RuntimeFilter> origFilters = Lists.newArrayList();

    private final Map<ExprId, SlotRef> exprIdToOlapScanNodeSlotRef = Maps.newHashMap();

    private final Map<ExprId, ExprId> aliasToChild = Maps.newHashMap();

    private final SessionVariable sessionVariable;

    private final FilterSizeLimits limits;

    private final ImmutableSet<JoinType> deniedJoinType = ImmutableSet.of(
            JoinType.LEFT_ANTI_JOIN,
            JoinType.RIGHT_ANTI_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_OUTER_JOIN
    );

    public RuntimeFilterGenerator(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
        this.limits = new FilterSizeLimits(sessionVariable);
    }

    // TODO: current support inner join, cross join, right outer join, and will support more join type.
    @Override
    public PhysicalPlan visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> join, CascadesContext ctx) {
        Plan left = join.left();
        Plan right = join.right();
        if (!deniedJoinType.contains(join.getJoinType())) {
            List<EqualTo> eqPreds = join.getHashJoinConjuncts().stream()
                    .map(EqualTo.class::cast).collect(Collectors.toList());
            List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values()).filter(type ->
                    (type.getValue() & sessionVariable.getRuntimeFilterType()) > 0).collect(Collectors.toList());
            List<RuntimeFilter> runtimeFilters = Lists.newArrayList();
            AtomicInteger cnt = new AtomicInteger();
            final PhysicalHashJoin<Plan, Plan> joinReplica = join;
            eqPreds.forEach(expr -> {
                runtimeFilters.addAll(legalTypes.stream()
                        .map(type -> RuntimeFilter.createRuntimeFilter(generator.getNextId(), expr,
                                type, cnt.get(), joinReplica))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
                cnt.getAndIncrement();
            });
            runtimeFilters.forEach(filter -> {
                filtersByExprId.computeIfAbsent(filter.getTargetExpr().getExprId(), k -> new ArrayList<>()).add(filter);
            });
            left = left.accept(this, ctx);
            right = right.accept(this, ctx);
        } else {
            join.getOutput().forEach(slot -> filtersByExprId.remove(slot.getExprId()));
            left = join.left().accept(this, ctx);
            right = join.right().accept(this, ctx);
        }
        return join.withChildren(ImmutableList.of(left, right));
    }

    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext ctx) {
        project.getProjects().stream().filter(Alias.class::isInstance)
                .map(Alias.class::cast)
                .forEach(expr -> aliasToChild.put(((SlotReference) expr.child()).getExprId(), expr.getExprId()));
        return project.withChildren(project.children().stream()
                .map(plan -> plan.accept(this, ctx)).collect(Collectors.toList()));
    }

    /**
     * s
     * @param join s
     * @param node s
     * @param ctx s
     */
    public void translateRuntimeFilter(PhysicalHashJoin<Plan, Plan> join,
            HashJoinNode node, PlanTranslatorContext ctx) {
        if (!sessionVariable.isEnableNereidsRuntimeFilter()) {
            return;
        }
        join.getHashJoinConjuncts().forEach(expr -> {
            ExprId exprId = ((SlotReference) expr.child(0)).getExprId();
            TupleId tid = ctx.findSlotRef(exprId).getDesc().getParent().getId();
            if (filterTargetByExprId.containsKey(exprId) && filtersByExprId.containsKey(exprId)) {
                List<RuntimeFilter.RuntimeFilterTarget> targets = filterTargetByExprId.get(exprId);
                origFilters.addAll(filtersByExprId.get(exprId).stream()
                        .filter(RuntimeFilter::isUninitialized)
                        .map(filter -> {
                            SlotRef src = ctx.findSlotRef(filter.getSrcExpr().getExprId());
                            SlotRef target = exprIdToOlapScanNodeSlotRef.get(filter.getTargetExpr().getExprId());
                            org.apache.doris.planner.RuntimeFilter origFilter
                                    = org.apache.doris.planner.RuntimeFilter.fromNereidsRuntimeFilter(
                                    filter.getId(), node, src, filter.getExprOrder(), target,
                                    ImmutableMap.of(tid, ImmutableList.of(target.getSlotId())),
                                    filter.getType(), limits
                            );
                            targets.stream()
                                    .map(nereidsTarget -> nereidsTarget.toOriginRuntimeFilterTarget(
                                            node,
                                            exprIdToOlapScanNodeSlotRef.get(nereidsTarget.getExpr().getExprId())))
                                    .forEach(origFilter::addTarget);
                            origFilter.setIsBroadcast(node.getDistributionMode() == DistributionMode.BROADCAST);
                            origFilter.markFinalized();
                            origFilter.assignToPlanNodes();
                            origFilter.extractTargetsPosition();
                            filter.setFinalized();
                            return origFilter;
                        }).collect(Collectors.toList()));
            }
        });
    }

    /**
     * s
     * @param scan s
     * @param node s
     * @param ctx s
     */
    public void translateRuntimeFilterTarget(PhysicalOlapScan scan, OlapScanNode node, PlanTranslatorContext ctx) {
        scan.getOutput().stream()
                .map(slot -> Pair.of(((SlotReference) slot), slotTransfer(slot.getExprId())))
                .filter(pair -> filtersByExprId.containsKey(pair.second))
                .peek(pair -> exprIdToOlapScanNodeSlotRef.put(pair.first.getExprId(),
                        ctx.findSlotRef(pair.first.getExprId())))
                .forEach(pair -> filtersByExprId.get(pair.second)
                        .stream().peek(nereidsFilter -> filterTargetByExprId.computeIfAbsent(
                                nereidsFilter.getTargetExpr().getExprId(),
                                k -> new ArrayList<>()).add(
                                        new RuntimeFilter.RuntimeFilterTarget(node, pair.first)))
                        .filter(nereidsFilter -> !nereidsFilter.getTargetExpr().getExprId()
                                .equals(pair.first.getExprId()))
                        .forEach(nereidsFilter -> nereidsFilter.setTargetSlot(pair.first)));
    }

    public List<org.apache.doris.planner.RuntimeFilter> getRuntimeFilters() {
        return origFilters;
    }

    /**
     * s
     * @return s
     */
    @VisibleForTesting
    public List<RuntimeFilter> getNereridsRuntimeFilter() {
        List<RuntimeFilter> filters = filtersByExprId.values().stream()
                .reduce(Lists.newArrayList(), (l, r) -> {
                    l.addAll(r);
                    return l;
                });
        filters.sort((a, b) -> a.getId().compareTo(b.getId()));
        return filters;
    }

    private ExprId slotTransfer(ExprId exprId) {
        return aliasToChild.getOrDefault(exprId, exprId);
    }
}
