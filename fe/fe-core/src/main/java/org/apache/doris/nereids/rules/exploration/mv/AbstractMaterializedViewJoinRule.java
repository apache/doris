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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;

/**
 * AbstractMaterializedViewJoinRule
 * This is responsible for common join rewriting
 */
public abstract class AbstractMaterializedViewJoinRule extends AbstractMaterializedViewRule {

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping queryToViewSlotMappings,
            Plan tempRewritedPlan,
            MaterializationContext materializationContext) {

        List<? extends Expression> queryShuttleExpression = ExpressionUtils.shuttleExpressionWithLineage(
                queryStructInfo.getExpressions(),
                queryStructInfo.getOriginalPlan());
        // Rewrite top projects, represent the query projects by view
        List<NamedExpression> expressions = rewriteExpression(
                queryShuttleExpression,
                materializationContext.getViewExpressionIndexMapping(),
                queryToViewSlotMappings,
                tempRewritedPlan
        );
        // Can not rewrite, bail out
        if (expressions == null) {
            return null;
        }
        if (queryStructInfo.getOriginalPlan().getGroupExpression().isPresent()) {
            materializationContext.addMatchedGroup(
                    queryStructInfo.getOriginalPlan().getGroupExpression().get().getOwnerGroup().getGroupId());
        }
        return new LogicalProject<>(expressions, tempRewritedPlan);
    }

    // Check join is whether valid or not. Support join's input can not contain aggregate
    // Only support project, filter, join, logical relation node and
    // join condition should be slot reference equals currently
    @Override
    protected boolean checkPattern(StructInfo structInfo) {
        HyperGraph hyperGraph = structInfo.getHyperGraph();
        HashSet<JoinType> requiredJoinType = Sets.newHashSet(JoinType.INNER_JOIN, JoinType.LEFT_OUTER_JOIN);
        for (AbstractNode node : hyperGraph.getNodes()) {
            StructInfoNode structInfoNode = (StructInfoNode) node;
            if (!structInfoNode.getPlan().accept(StructInfo.JOIN_PATTERN_CHECKER,
                    requiredJoinType)) {
                return false;
            }
            for (Edge edge : hyperGraph.getEdges()) {
                if (!edge.getJoin().accept(StructInfo.JOIN_PATTERN_CHECKER, requiredJoinType)) {
                    return false;
                }
            }
        }
        return true;
    }
}
