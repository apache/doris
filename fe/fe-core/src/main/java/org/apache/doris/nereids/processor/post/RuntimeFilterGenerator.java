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
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter.RuntimeFilterTarget;
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

    private final Map<ExprId, List<RuntimeFilter>> targetExprIdToFilter = Maps.newHashMap();

    private final Map<ExprId, List<RuntimeFilter.RuntimeFilterTarget>> targetExprIdToFilterTarget = Maps.newHashMap();

    private final List<org.apache.doris.planner.RuntimeFilter> legacyFilters = Lists.newArrayList();

    private final Map<ExprId, SlotRef> exprIdToOlapScanNodeSlotRef = Maps.newHashMap();

    private final Map<ExprId, ExprId> aliasChildToSelf = Maps.newHashMap();

    private final Map<SlotReference, OlapScanNode> scanNodeOfLegacyRuntimeFilterTarget = Maps.newHashMap();

    private final SessionVariable sessionVariable;

    private final FilterSizeLimits limits;

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

    public RuntimeFilterGenerator(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
        this.limits = new FilterSizeLimits(sessionVariable);
    }

    // TODO: current support inner join, cross join, right outer join, and will support more join type.
    @Override
    public PhysicalPlan visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> join, CascadesContext ctx) {
        if (deniedJoinType.contains(join.getJoinType())) {
            join.getOutput().forEach(slot -> targetExprIdToFilter.remove(slot.getExprId()));
        } else {
            List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values()).filter(type ->
                    (type.getValue() & sessionVariable.getRuntimeFilterType()) > 0).collect(Collectors.toList());
            List<RuntimeFilter> runtimeFilters = Lists.newArrayList();
            AtomicInteger cnt = new AtomicInteger();
            join.getHashJoinConjuncts().stream()
                    .map(EqualTo.class::cast).forEach(expr -> {
                        runtimeFilters.addAll(legalTypes.stream()
                                .map(type -> RuntimeFilter.createRuntimeFilter(generator.getNextId(), expr,
                                        type, cnt.get(), join))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()));
                        cnt.getAndIncrement();
                    });
            runtimeFilters.forEach(filter -> targetExprIdToFilter.computeIfAbsent(
                    filter.getTargetExpr().getExprId(), k -> new ArrayList<>()).add(filter));
        }
        join.left().accept(this, ctx);
        join.right().accept(this, ctx);
        return join;
    }

    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext ctx) {
        project.getProjects().stream().filter(Alias.class::isInstance)
                .map(Alias.class::cast)
                .forEach(expr -> aliasChildToSelf.put(((SlotReference) expr.child()).getExprId(), expr.getExprId()));
        project.child().accept(this, ctx);
        return project;
    }

    /**
     * Translate nereids runtime filter on hash join node
     * translate the runtime filter whose target expression id is one of the output slot reference of the left child of
     * the physical hash join.
     * @param join hash join physical plan
     * @param node hash join node
     * @param ctx plan translator context
     */
    public void translateRuntimeFilter(PhysicalHashJoin<Plan, Plan> join,
            HashJoinNode node, PlanTranslatorContext ctx) {
        join.getHashJoinConjuncts().forEach(expr -> {
            if (!(expr.child(0) instanceof  SlotReference)) {
                return;
            }
            ExprId exprId = ((SlotReference) expr.child(0)).getExprId();
            TupleId tid = ctx.findSlotRef(exprId).getDesc().getParent().getId();
            if (targetExprIdToFilterTarget.containsKey(exprId) && targetExprIdToFilter.containsKey(exprId)) {
                List<RuntimeFilter.RuntimeFilterTarget> targets = targetExprIdToFilterTarget.get(exprId);
                legacyFilters.addAll(targetExprIdToFilter.get(exprId).stream()
                        .filter(RuntimeFilter::isUninitialized)
                        .map(filter -> createLegacyRuntimeFilter(filter, targets, tid, node, ctx))
                        .collect(Collectors.toList()));
            }
        });
    }

    /**
     * translate runtime filter target.
     * @param scan physical plan of olap scan
     * @param node olap scan node
     * @param ctx plan translator context
     */
    public void translateRuntimeFilterTarget(PhysicalOlapScan scan, OlapScanNode node, PlanTranslatorContext ctx) {
        scan.getOutput().stream()
                .map(slot -> Pair.of(((SlotReference) slot), slotTransfer(slot.getExprId())))
                .filter(pair -> targetExprIdToFilter.containsKey(pair.second))
                .peek(pair -> exprIdToOlapScanNodeSlotRef.put(pair.first.getExprId(),
                        ctx.findSlotRef(pair.first.getExprId())))
                .forEach(pair -> targetExprIdToFilter.get(pair.second)
                        .stream().peek(nereidsFilter -> {
                            targetExprIdToFilterTarget.computeIfAbsent(
                                    nereidsFilter.getTargetExpr().getExprId(),
                                    k -> new ArrayList<>()).add(
                                        new RuntimeFilter.RuntimeFilterTarget(pair.first));
                            scanNodeOfLegacyRuntimeFilterTarget.put(pair.first, node);
                        })
                        .filter(nereidsFilter -> !nereidsFilter.getTargetExpr().getExprId()
                                .equals(pair.first.getExprId()))
                        .forEach(nereidsFilter -> nereidsFilter.setTargetSlot(pair.first)));
    }

    public List<org.apache.doris.planner.RuntimeFilter> getRuntimeFilters() {
        return legacyFilters;
    }

    /**
     * get nereids runtime filters
     * @return nereids runtime filters
     */
    @VisibleForTesting
    public List<RuntimeFilter> getNereidsRuntimeFilter() {
        List<RuntimeFilter> filters = targetExprIdToFilter.values().stream()
                .reduce(Lists.newArrayList(), (l, r) -> {
                    l.addAll(r);
                    return l;
                });
        filters.sort((a, b) -> a.getId().compareTo(b.getId()));
        return filters;
    }

    private ExprId slotTransfer(ExprId exprId) {
        return aliasChildToSelf.getOrDefault(exprId, exprId);
    }

    private org.apache.doris.planner.RuntimeFilter createLegacyRuntimeFilter(RuntimeFilter filter,
            List<RuntimeFilterTarget> targets, TupleId tid, HashJoinNode node, PlanTranslatorContext ctx) {
        SlotRef src = ctx.findSlotRef(filter.getSrcExpr().getExprId());
        SlotRef target = exprIdToOlapScanNodeSlotRef.get(filter.getTargetExpr().getExprId());
        org.apache.doris.planner.RuntimeFilter origFilter
                = org.apache.doris.planner.RuntimeFilter.fromNereidsRuntimeFilter(
                filter.getId(), node, src, filter.getExprOrder(), target,
                ImmutableMap.of(tid, ImmutableList.of(target.getSlotId())),
                filter.getType(), limits
        );
        targets.stream()
                .map(nereidsTarget -> {
                    OlapScanNode scanNode = scanNodeOfLegacyRuntimeFilterTarget.get(nereidsTarget.getExpr());
                    return nereidsTarget.toLegacyRuntimeFilterTarget(
                            scanNode,
                            exprIdToOlapScanNodeSlotRef.get(nereidsTarget.getExpr().getExprId()),
                            scanNode.getFragmentId().equals(node.getFragmentId())
                    );
                }).forEach(origFilter::addTarget);
        origFilter.setIsBroadcast(node.getDistributionMode() == DistributionMode.BROADCAST);
        origFilter.markFinalized();
        origFilter.assignToPlanNodes();
        origFilter.extractTargetsPosition();
        filter.setFinalized();
        return origFilter;
    }
}
