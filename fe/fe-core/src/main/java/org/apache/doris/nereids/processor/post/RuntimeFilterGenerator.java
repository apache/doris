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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;
import org.apache.doris.planner.RuntimeFilterGenerator.FilterSizeLimits;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

    /**
     * s
     */
    public static RuntimeFilterGenerator INSTANCE = new RuntimeFilterGenerator();

    private static final IdGenerator<RuntimeFilterId> GENERATOR = RuntimeFilterId.createGenerator();

    private final Map<ExprId, List<RuntimeFilter>> filtersByExprId = Maps.newHashMap();

    private final Map<ExprId, List<RuntimeFilterTarget>> filterTargetByTid = Maps.newHashMap();

    private SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();

    private FilterSizeLimits limits = new FilterSizeLimits(sessionVariable);

    /**
     * clear runtime filter and return the INSTANCE
     * @param ctx connect context
     * @return the INSTANCE
     */
    public static RuntimeFilterGenerator getInstance(ConnectContext ctx) {
        INSTANCE.filterTargetByTid.clear();
        INSTANCE.filtersByExprId.clear();
        INSTANCE.sessionVariable = ConnectContext.get().getSessionVariable();
        INSTANCE.limits = new FilterSizeLimits(INSTANCE.sessionVariable);
        return INSTANCE;
    }

    @Override
    public PhysicalPlan visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> join, CascadesContext ctx) {
        Plan left = join.left();
        Plan right = join.right();
        if (join.getJoinType() == JoinType.INNER_JOIN) {
            List<EqualTo> eqPreds = join.getHashJoinConjuncts().stream()
                    .map(EqualTo.class::cast).collect(Collectors.toList());
            List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values()).filter(type ->
                    (type.getValue() & sessionVariable.getRuntimeFilterType()) > 0).collect(Collectors.toList());
            List<RuntimeFilter> runtimeFilters = Lists.newArrayList();
            AtomicInteger cnt = new AtomicInteger();
            final PhysicalHashJoin<Plan, Plan> joinReplica = join;
            List<Expression> newHashConjuncts = Lists.newArrayList();
            eqPreds.forEach(expr -> {
                runtimeFilters.addAll(legalTypes.stream()
                        .map(type -> RuntimeFilter.createRuntimeFilter(GENERATOR.getNextId(), expr,
                                type, cnt.get(), joinReplica, newHashConjuncts))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
                cnt.getAndIncrement();
            });
            runtimeFilters.forEach(filter -> {
                filtersByExprId.computeIfAbsent(filter.getTargetExpr().getExprId(), k -> new ArrayList<>()).add(filter);
            });
            left = left.accept(this, ctx);
            runtimeFilters.forEach(RuntimeFilter::setFinalized);
            right = right.accept(this, ctx);
            join = join.withRuntimeFilterGenerator(this).withHashJoinConjuncts(newHashConjuncts);
        } else {
            left = join.left().accept(this, ctx);
            right = join.right().accept(this, ctx);
        }
        return join.withChildren(ImmutableList.of(left, right));
    }

    /**
     * s
     * @param join s
     * @param node s
     * @param ctx s
     */
    public void translateRuntimeFilter(PhysicalHashJoin<Plan, Plan> join,
            HashJoinNode node, PlanTranslatorContext ctx) {
        join.getHashJoinConjuncts().forEach(expr -> {
            ExprId exprId = ((SlotReference) expr.child(0)).getExprId();
            SlotRef ref = ctx.findSlotRef(exprId);
            TupleId tid = ref.getDesc().getParent().getId();
            if (filterTargetByTid.containsKey(exprId) && filtersByExprId.containsKey(exprId)) {
                List<RuntimeFilterTarget> targets = filterTargetByTid.get(exprId);
                filtersByExprId.get(exprId).forEach(filter -> {
                    SlotRef src = ctx.findSlotRef(filter.getSrcExpr().getExprId());
                    SlotRef target = ctx.findSlotRef(filter.getTargetExpr().getExprId());
                    org.apache.doris.planner.RuntimeFilter origFilter
                            = org.apache.doris.planner.RuntimeFilter.fromNereidsRuntimeFilter(
                            filter.getId(), node, src, 0, target,
                            ImmutableMap.of(tid, ImmutableList.of(target.getSlotId())),
                            filter.getType(), limits
                    );
                    targets.forEach(origFilter::addTarget);
                    origFilter.markFinalized();
                    origFilter.assignToPlanNodes();
                });
            }
        });
    }

    /**
     * s
     * @param node s
     */
    public void translateRuntimeFilterTarget(PhysicalOlapScan scan, OlapScanNode node, PlanTranslatorContext ctx) {
        scan.getOutput().stream()
                .filter(slot -> filtersByExprId.containsKey(slot.getExprId()))
                .forEach(slot -> {
                    filtersByExprId.get(slot.getExprId()).forEach(nereidsFilter -> {
                        SlotRef ref = ctx.findSlotRef(nereidsFilter.getTargetExpr().getExprId());
                        filterTargetByTid.computeIfAbsent(
                                nereidsFilter.getTargetExpr().getExprId(),
                                k -> new ArrayList<>()).add(
                                        new RuntimeFilterTarget(node, ref, false, false));
                    });
                });
    }

    public Map<ExprId, List<RuntimeFilter>> getFiltersByExprId() {
        return this.filtersByExprId;
    }
}
