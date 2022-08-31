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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
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

    private RuntimeFilter(RuntimeFilterId id, SlotReference src, SlotReference target, TRuntimeFilterType type) {
        this.id = id;
        this.srcSlot = src;
        this.targetSlot = target;
        this.type = type;
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
        return new RuntimeFilter(id, ((SlotReference) srcs.second), ((SlotReference) srcs.first), type);
    }

    private static Pair<Expression, Expression> checkAndMaybeSwapChild(EqualTo expr,
            PhysicalHashJoin<Plan, Plan> join) {
        if (expr.children().stream().anyMatch(Literal.class::isInstance)) {
            return null;
        }
        if (expr.child(0).equals(expr.child(1))) {
            return null;
        }
        //current we assume that there are certainly different slot reference in equal to.
        //they are not from the same relation.
        int exchangeTag = 0;
        if (join.child(0).getOutput().contains(expr.child(1))) {
            exchangeTag = 1;
        }
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

    /**
     * runtime filter target
     */
    public static class RuntimeFilterTarget {
        OlapScanNode node;
        Expression expr;

        public RuntimeFilterTarget(OlapScanNode node, Expression expr) {
            this.node = node;
            this.expr = expr;
        }

        /**
         * s
         * @param ctx s
         * @param node s
         * @return s
         */
        public org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget toOriginRuntimeFilterTarget(
                PlanTranslatorContext ctx, HashJoinNode node) {
            return new org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget(
                    this.node, slotRefTransfer(((SlotReference) expr).getExprId(), this.node, ctx
                    ), true,
                    node.getFragmentId().equals(this.node.getFragmentId())
            );
        }

        private SlotRef slotRefTransfer(ExprId eid, OlapScanNode node, PlanTranslatorContext ctx) {
            SlotDescriptor slotDesc = node.getTupleDesc().getColumnSlot(
                    ctx.findSlotRef(eid).getColumnName()
            );
            return new SlotRef(slotDesc);
        }
    }
}
