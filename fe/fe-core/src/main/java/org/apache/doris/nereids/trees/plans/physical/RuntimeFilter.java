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

import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TRuntimeFilterType;

/**
 * runtime filter
 */
public class RuntimeFilter {

    private final SlotReference srcSlot;

    private final SlotReference targetSlot;

    private final RuntimeFilterId id;

    private final TRuntimeFilterType type;

    private final int exprOrder;

    private RuntimeFilter(RuntimeFilterId id, SlotReference src, SlotReference target, TRuntimeFilterType type,
            int exprOrder) {
        this.id = id;
        this.srcSlot = src;
        this.targetSlot = target;
        this.type = type;
        this.exprOrder = exprOrder;
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
            TRuntimeFilterType type, int exprOrder, PhysicalHashJoin<Plan, Plan> node) {
        Pair<Expression, Expression> srcs = checkAndMaybeSwapChild(conjunction, node);
        if (srcs == null) {
            return null;
        }
        return new RuntimeFilter(id, ((SlotReference) srcs.second), ((SlotReference) srcs.first), type, exprOrder);
    }

    private static Pair<Expression, Expression> checkAndMaybeSwapChild(EqualTo expr,
            PhysicalHashJoin<Plan, Plan> join) {
        if (expr.children().stream().anyMatch(Literal.class::isInstance)) {
            return null;
        }
        if (expr.child(0).equals(expr.child(1))) {
            return null;
        }
        if (!expr.children().stream().allMatch(SlotReference.class::isInstance)) {
            return null;
        }
        //current we assume that there are certainly different slot reference in equal to.
        //they are not from the same relation.
        int exchangeTag = join.child(0).getOutput().stream().anyMatch(slot -> slot.getExprId().equals(
                ((SlotReference) expr.child(1)).getExprId())) ? 1 : 0;
        return Pair.of(expr.child(exchangeTag), expr.child(1 ^ exchangeTag));
    }

    public SlotReference getSrcExpr() {
        return srcSlot;
    }

    public SlotReference getTargetExpr() {
        return targetSlot;
    }

    public RuntimeFilterId getId() {
        return id;
    }

    public TRuntimeFilterType getType() {
        return type;
    }

    public int getExprOrder() {
        return exprOrder;
    }

    /**
     * runtime filter target
     */
    public static class RuntimeFilterTarget {
        OlapScanNode node;
        SlotReference expr;

        public RuntimeFilterTarget(OlapScanNode node, SlotReference expr) {
            this.node = node;
            this.expr = expr;
        }

        /**
         * s
         * @param node s
         * @param targetSlotRef s
         * @return s
         */
        public org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget toOriginRuntimeFilterTarget(
                HashJoinNode node, SlotRef targetSlotRef) {
            return new org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget(
                    this.node, targetSlotRef, true, node.getFragmentId().equals(this.node.getFragmentId()));
        }

        public SlotReference getExpr() {
            return expr;
        }
    }
}
