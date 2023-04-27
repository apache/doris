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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * two phase read opt
 * refer to:
 * https://github.com/apache/doris/pull/15642
 * https://github.com/apache/doris/pull/16460
 * https://github.com/apache/doris/pull/16848
 */

public class TwoPhaseReadOpt extends PlanPostProcessor {

    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        if (plan instanceof PhysicalTopN) {
            PhysicalTopN<Plan> physicalTopN = (PhysicalTopN<Plan>) plan;
            if (physicalTopN.getSortPhase() == SortPhase.MERGE_SORT) {
                return plan.accept(this, ctx);
            }
        }
        return plan;
    }

    @Override
    public PhysicalTopN visitPhysicalTopN(PhysicalTopN<? extends Plan> mergeTopN, CascadesContext ctx) {
        mergeTopN.child().accept(this, ctx);
        if (mergeTopN.getSortPhase() != SortPhase.MERGE_SORT || !(mergeTopN.child() instanceof PhysicalDistribute)) {
            return mergeTopN;
        }
        PhysicalDistribute<Plan> distribute = (PhysicalDistribute<Plan>) mergeTopN.child();
        if (!(distribute.child() instanceof PhysicalTopN)) {
            return mergeTopN;
        }
        PhysicalTopN<Plan> localTopN = (PhysicalTopN<Plan>) distribute.child();

        if (localTopN.getOrderKeys().isEmpty()) {
            return mergeTopN;
        }

        // topn opt
        long topNOptLimitThreshold = getTopNOptLimitThreshold();
        if (topNOptLimitThreshold < 0 || mergeTopN.getLimit() > topNOptLimitThreshold) {
            return mergeTopN;
        }
        if (!localTopN.getOrderKeys().stream().map(OrderKey::getExpr).allMatch(Expression::isColumnFromTable)) {
            return mergeTopN;
        }

        PhysicalOlapScan olapScan;
        PhysicalProject<Plan> project = null;
        PhysicalFilter<Plan> filter = null;
        Plan child = localTopN.child();
        while (child instanceof Project || child instanceof Filter) {
            if (child instanceof Filter) {
                filter = (PhysicalFilter<Plan>) child;
            }
            if (child instanceof Project) {
                project = (PhysicalProject<Plan>) child;
                // TODO: remove this after fix two phase read on project core
                return mergeTopN;
            }
            child = child.child(0);
        }
        if (!(child instanceof PhysicalOlapScan)) {
            return mergeTopN;
        }
        olapScan = (PhysicalOlapScan) child;

        // all order key must column from table
        if (!olapScan.getTable().getEnableLightSchemaChange()) {
            return mergeTopN;
        }

        Map<ExprId, ExprId> projectRevertedMap = Maps.newHashMap();
        if (project != null) {
            for (Expression e : project.getProjects()) {
                if (e.isSlot()) {
                    Slot slot = (Slot) e;
                    projectRevertedMap.put(slot.getExprId(), slot.getExprId());
                } else if (e instanceof Alias) {
                    Alias alias = (Alias) e;
                    if (alias.child().isSlot()) {
                        Slot slot = (Slot) alias.child();
                        projectRevertedMap.put(alias.getExprId(), slot.getExprId());
                    }
                }
            }
        }
        Set<ExprId> deferredMaterializedExprIds = Sets.newHashSet(olapScan.getOutputExprIdSet());
        if (filter != null) {
            filter.getConjuncts().forEach(e -> deferredMaterializedExprIds.removeAll(e.getInputSlotExprIds()));
        }
        localTopN.getOrderKeys().stream()
                .map(OrderKey::getExpr)
                .map(Slot.class::cast)
                .map(NamedExpression::getExprId)
                .map(projectRevertedMap::get)
                .filter(Objects::nonNull)
                .forEach(deferredMaterializedExprIds::remove);
        localTopN.getOrderKeys().stream()
                .map(OrderKey::getExpr)
                .map(Slot.class::cast)
                .map(NamedExpression::getExprId)
                .forEach(deferredMaterializedExprIds::remove);
        localTopN.setMutableState(PhysicalTopN.TWO_PHASE_READ_OPT, true);
        mergeTopN.setMutableState(PhysicalTopN.TWO_PHASE_READ_OPT, true);
        olapScan.setMutableState(PhysicalOlapScan.DEFERRED_MATERIALIZED_SLOTS, deferredMaterializedExprIds);

        return mergeTopN;
    }

    private long getTopNOptLimitThreshold() {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            if (!ConnectContext.get().getSessionVariable().enableTwoPhaseReadOpt) {
                return -1;
            }
            return ConnectContext.get().getSessionVariable().topnOptLimitThreshold;
        }
        return -1;
    }
}
