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
    public PhysicalTopN visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
        topN.child().accept(this, ctx);
        Plan child = topN.child();
        if (topN.getSortPhase() != SortPhase.LOCAL_SORT) {
            return topN;
        }
        if (topN.getOrderKeys().isEmpty()) {
            return topN;
        }

        // topn opt
        long topNOptLimitThreshold = getTopNOptLimitThreshold();
        if (topNOptLimitThreshold < 0 || topN.getLimit() > topNOptLimitThreshold) {
            return topN;
        }
        if (!topN.getOrderKeys().stream().map(OrderKey::getExpr).allMatch(Expression::isColumnFromTable)) {
            return topN;
        }

        PhysicalOlapScan olapScan;
        PhysicalProject<Plan> project = null;
        PhysicalFilter<Plan> filter = null;
        while (child instanceof Project || child instanceof Filter) {
            if (child instanceof Filter) {
                filter = (PhysicalFilter<Plan>) child;
            }
            if (child instanceof Project) {
                project = (PhysicalProject<Plan>) child;
                // TODO: remove this after fix two phase read on project core
                return topN;
            }
            child = child.child(0);
        }
        if (!(child instanceof PhysicalOlapScan)) {
            return topN;
        }
        olapScan = (PhysicalOlapScan) child;

        // all order key must column from table
        if (!olapScan.getTable().getEnableLightSchemaChange()) {
            return topN;
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
        topN.getOrderKeys().stream()
                .map(OrderKey::getExpr)
                .map(Slot.class::cast)
                .map(NamedExpression::getExprId)
                .map(projectRevertedMap::get)
                .filter(Objects::nonNull)
                .forEach(deferredMaterializedExprIds::remove);
        topN.getOrderKeys().stream()
                .map(OrderKey::getExpr)
                .map(Slot.class::cast)
                .map(NamedExpression::getExprId)
                .forEach(deferredMaterializedExprIds::remove);
        topN.setMutableState(PhysicalTopN.TWO_PHASE_READ_OPT, true);
        olapScan.setMutableState(PhysicalOlapScan.DEFERRED_MATERIALIZED_SLOTS, deferredMaterializedExprIds);

        return topN;
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
