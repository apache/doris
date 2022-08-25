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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.RuntimeFilterGenerator.FilterSizeLimits;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TRuntimeFilterMode;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * runtime filter
 */
public class RuntimeFilter {
    private SlotReference src;

    private RuntimeFilterId id;

    private PhysicalHashJoin<Plan, Plan> builderPlan;

    private TRuntimeFilterType type;

    private boolean finalized = false;

    private List<RuntimeFilterTarget> targets;

    private HashJoinNode builderNode;

    private FilterSizeLimits limits;

    private class RuntimeFilterTarget {
        public final SlotReference target;

        public final PhysicalOlapScan scanNode;

        public final boolean isBoundByKeyColumn;

        public final boolean isLocalTarget;

        private RuntimeFilterTarget(SlotReference target, PhysicalOlapScan scanNode,
                boolean isBoundByKeyColumn, boolean isLocalTarget) {
            this.target = target;
            this.scanNode = scanNode;
            this.isBoundByKeyColumn = isBoundByKeyColumn;
            this.isLocalTarget = isLocalTarget;
        }
    }

    private RuntimeFilter(RuntimeFilterId id, SlotReference src,
            PhysicalHashJoin<Plan, Plan> builderPlan, TRuntimeFilterType type, FilterSizeLimits limits) {
        this.id = id;
        this.builderPlan = builderPlan;
        this.src = src;
        this.type = type;
        this.limits = limits;
    }

    public void setFinalized() {
        this.finalized = true;
    }

    public boolean getFinalized() {
        return finalized;
    }

    /**
     * s
     *
     * @param conjunction s
     * @param type s
     * @param exprOrder s
     * @param node s
     * @return s
     */
    public static RuntimeFilter createRuntimeFilter(RuntimeFilterId id, EqualTo conjunction,
            TRuntimeFilterType type, int exprOrder, PhysicalHashJoin<Plan, Plan> node, FilterSizeLimits limits) {
        EqualTo expr = checkAndMaybeSwapChild(conjunction, node);
        if (expr == null) {
            return null;
        }
        return new RuntimeFilter(id, (SlotReference) expr.child(1), node, type, limits);
    }

    /**
     * s
     * @param ctx s
     * @return s
     */
    public org.apache.doris.planner.RuntimeFilter toOriginRuntimeFilter(PlanTranslatorContext ctx) {
        SlotRef srcRef = ctx.findSlotRef(src.getExprId());
        SlotRef targetRef = ctx.findSlotRef(targets.get(0).target.getExprId());
        Map<TupleId, List<SlotId>> map = ImmutableMap.<TupleId, List<SlotId>>builder()
                .put(targetRef.getDesc().getParent().getId(), ImmutableList.of(targetRef.getSlotId())).build();
        return org.apache.doris.planner.RuntimeFilter.fromNereidsRuntimeFilter(
                id, builderNode, srcRef, 0,
                targetRef, map, type, limits);
    }

    private static EqualTo checkAndMaybeSwapChild(EqualTo expr, PhysicalHashJoin<Plan, Plan> join) {
        if (expr.children().stream().anyMatch(Literal.class::isInstance)) {
            return null;
        }
        if (expr.child(0).equals(expr.child(1))) {
            return null;
        }
        //current we assume that there are certainly different slot reference in equal to.
        //they are not from the same relation.
        if (join.child(0).getOutput().contains(expr.child(1))) {
            expr = (EqualTo) expr.withChildren(expr.child(1), expr.child(0));
        }
        return expr;
    }

    public void setBuilderNode(HashJoinNode builderNode) {
        this.builderNode = builderNode;
    }

    /**
     * s
     * @param node s
     */
    public void addTargetFromNode(ScanNode node, PlanTranslatorContext ctx) {
        if (filters.get(plan.getTable().getId()) == null) {
            return plan;
        }
        String mode = sessionVariable.getRuntimeFilterMode();
        Preconditions.checkState(Arrays.stream(TRuntimeFilterMode.values()).map(Enum::name).anyMatch(
                p -> p.equals(mode.toUpperCase())), "runtimeFilterMode not expected");
        filters.get(plan.getTable().getId()).forEach(filter -> {
            filter.addTargetFromNode();
        });
        return plan;
    }
}
