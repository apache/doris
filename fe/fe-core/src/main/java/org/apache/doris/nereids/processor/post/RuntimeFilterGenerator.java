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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.rules.analysis.OlapScanNodeId;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
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
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;
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
import java.util.Collections;
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

    private final Map<OlapScanNodeId, List<SlotReference>> targetOnOlapScanNodeMap = Maps.newHashMap();

    private final List<org.apache.doris.planner.RuntimeFilter> legacyFilters = Lists.newArrayList();

    private final Map<ExprId, SlotRef> exprIdToOlapScanNodeSlotRef = Maps.newHashMap();

    private final Map<SlotReference, NamedExpression> aliasChildToSelf = Maps.newHashMap();

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
            join.getOutput().forEach(slot -> targetExprIdToFilter.remove(slot.getExprId()));
        } else {
            List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values()).filter(type ->
                    (type.getValue() & sessionVariable.getRuntimeFilterType()) > 0).collect(Collectors.toList());
            AtomicInteger cnt = new AtomicInteger();
            join.getHashJoinConjuncts().stream()
                    .map(EqualTo.class::cast)
                    .peek(expr -> {
                        List<SlotReference> slots = expr.children().stream().filter(SlotReference.class::isInstance)
                                .map(SlotReference.class::cast).collect(Collectors.toList());
                        if (slots.size() != 2 || !(targetExprIdToFilter.containsKey(slots.get(0).getExprId())
                                || targetExprIdToFilter.containsKey(slots.get(1).getExprId()))) {
                            return;
                        }
                        int tag = targetExprIdToFilter.containsKey(slots.get(0).getExprId()) ? 0 : 1;
                        targetExprIdToFilter.computeIfAbsent(slots.get(tag ^ 1).getExprId(),
                                k -> targetExprIdToFilter.get(slots.get(tag).getExprId()).stream()
                                        .map(filter -> new RuntimeFilter(generator.getNextId(), filter.getSrcExpr(),
                                        slots.get(tag ^ 1), filter.getType(), filter.getExprOrder()))
                                        .collect(Collectors.toList()));
                    })
                    .forEach(expr -> legalTypes.stream()
                            .map(type -> RuntimeFilter.createRuntimeFilter(generator.getNextId(), expr,
                                    type, cnt.getAndIncrement(), join))
                            .filter(Objects::nonNull)
                            .forEach(filter -> targetExprIdToFilter.computeIfAbsent(
                                    filter.getTargetExpr().getExprId(), k -> new ArrayList<>()).add(filter)));
        }
        join.left().accept(this, ctx);
        join.right().accept(this, ctx);
        return join;
    }

    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext ctx) {
        project.getProjects().stream().filter(Alias.class::isInstance)
                .map(Alias.class::cast)
                .filter(expr -> expr.child() instanceof SlotReference)
                .forEach(expr -> aliasChildToSelf.put(((SlotReference) expr.child()), expr));
        project.child().accept(this, ctx);
        return project;
    }

    @Override
    public PhysicalOlapScan visitPhysicalOlapScan(PhysicalOlapScan scan, CascadesContext ctx) {
        scan.getOutput().stream()
                .filter(slot -> targetExprIdToFilter.containsKey(slotTransfer(((SlotReference) slot)).getExprId()))
                .forEach(slot -> targetOnOlapScanNodeMap.computeIfAbsent(scan.id, k -> new ArrayList<>())
                        .add((SlotReference) slot));
        return scan;
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
            if (!(expr.child(0) instanceof SlotReference)) {
                return;
            }
            expr.children().stream().filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .map(slot -> slotTransfer(slot).getExprId())
                    .filter(id -> targetExprIdToFilter.containsKey(id))
                    .forEach(id -> {
                        TupleId tid = ctx.findSlotRef(id).getDesc().getParent().getId();
                        legacyFilters.addAll(targetExprIdToFilter.get(id).stream()
                                .filter(RuntimeFilter::isUninitialized)
                                .map(filter -> createLegacyRuntimeFilter(filter, tid, node, ctx))
                                .collect(Collectors.toList()));
                    });
        });
    }

    public List<SlotReference> getTargetOnOlapScanNode(OlapScanNodeId id) {
        return targetOnOlapScanNodeMap.getOrDefault(id, Collections.emptyList());
    }

    /**
     * translate runtime filter target.
     * @param node olap scan node
     * @param ctx plan translator context
     */
    public void translateRuntimeFilterTarget(SlotReference slot, OlapScanNode node, PlanTranslatorContext ctx) {
        ExprId eid = slotTransfer(slot).getExprId();
        if (!targetExprIdToFilter.containsKey(eid)) {
            return;
        }
        exprIdToOlapScanNodeSlotRef.put(slot.getExprId(), ctx.findSlotRef(slot.getExprId()));
        scanNodeOfLegacyRuntimeFilterTarget.put(slot, node);
        targetExprIdToFilter.get(eid).stream()
                .filter(nereidsFilter -> !nereidsFilter.getTargetExpr().getExprId().equals(slot.getExprId()))
                .forEach(nereidsFilter -> nereidsFilter.setTargetSlot(slot));
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

    private NamedExpression slotTransfer(SlotReference slot) {
        return aliasChildToSelf.getOrDefault(slot, slot);
    }

    private org.apache.doris.planner.RuntimeFilter createLegacyRuntimeFilter(RuntimeFilter filter,
            TupleId tid, HashJoinNode node, PlanTranslatorContext ctx) {
        SlotReference slot = filter.getTargetExpr();
        SlotRef src = ctx.findSlotRef(filter.getSrcExpr().getExprId());
        SlotRef target = exprIdToOlapScanNodeSlotRef.get(slot.getExprId());
        org.apache.doris.planner.RuntimeFilter origFilter
                = org.apache.doris.planner.RuntimeFilter.fromNereidsRuntimeFilter(
                filter.getId(), node, src, filter.getExprOrder(), target,
                ImmutableMap.of(tid, ImmutableList.of(target.getSlotId())),
                filter.getType(), limits
        );
        origFilter.setIsBroadcast(node.getDistributionMode() == DistributionMode.BROADCAST);
        filter.setFinalized();
        OlapScanNode scanNode = scanNodeOfLegacyRuntimeFilterTarget.get(slot);
        origFilter.addTarget(new RuntimeFilterTarget(
                scanNode,
                exprIdToOlapScanNodeSlotRef.get(slot.getExprId()),
                true,
                scanNode.getFragmentId().equals(node.getFragmentId())));
        return finalize(origFilter);
    }

    private org.apache.doris.planner.RuntimeFilter finalize(org.apache.doris.planner.RuntimeFilter origFilter) {
        origFilter.markFinalized();
        origFilter.assignToPlanNodes();
        origFilter.extractTargetsPosition();
        return origFilter;
    }
}
