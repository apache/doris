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

import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TRuntimeFilterType;

import java.util.List;

/**
 * runtime filter
 */
public class RuntimeFilter {
    private final EqualTo expr;

    private final RuntimeFilterId id;

    private final TRuntimeFilterType type;

    private boolean finalized = false;

    private RuntimeFilter(RuntimeFilterId id, EqualTo expr, TRuntimeFilterType type) {
        this.id = id;
        this.expr = expr;
        this.type = type;
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
            TRuntimeFilterType type, int exprOrder, PhysicalHashJoin<Plan, Plan> node,
            List<Expression> newHashConjuncts) {
        EqualTo expr = checkAndMaybeSwapChild(conjunction, node);
        newHashConjuncts.add(expr);
        if (expr == null) {
            return null;
        }
        return new RuntimeFilter(id, expr, type);
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

    public SlotReference getSrcExpr() {
        return (SlotReference) expr.child(1);
    }

    public SlotReference getTargetExpr() {
        return (SlotReference) expr.child(0);
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
                    this.node, RuntimeFilterGenerator.slotRefTransfer(
                            ((SlotReference) expr).getExprId(), this.node, ctx
                    ), true,
                    node.getFragmentId().equals(this.node.getFragmentId())
            );
        }
    }
}
