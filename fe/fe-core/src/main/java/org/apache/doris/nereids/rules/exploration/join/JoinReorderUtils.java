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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common
 */
class JoinReorderUtils {
    /**
     * check project inside Join to prevent matching some pattern.
     * just allow projection is slot or Alias(slot) to prevent reorder when:
     * - output of project function is in condition, A join (project [abs(B.id), ..] B join C on ..) on abs(B.id)=A.id.
     * - hyper edge in projection. project A.id + B.id A join B on .. (this project will prevent join reorder).
     */
    static boolean checkProject(LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project) {
        List<NamedExpression> exprs = project.getProjects();
        // must be slot or Alias(slot)
        return exprs.stream().allMatch(expr -> {
            if (expr instanceof Slot) {
                return true;
            }
            if (expr instanceof Alias) {
                return ((Alias) expr).child() instanceof Slot;
            }
            return false;
        });
    }

    static Map<Boolean, List<NamedExpression>> splitProjection(
            List<NamedExpression> projects, Plan splitChild) {
        Set<ExprId> splitExprIds = splitChild.getOutputExprIdSet();

        Map<Boolean, List<NamedExpression>> projectExprsMap = projects.stream()
                .collect(Collectors.partitioningBy(projectExpr -> {
                    Set<ExprId> usedExprIds = projectExpr.getInputSlotExprIds();
                    return splitExprIds.containsAll(usedExprIds);
                }));

        return projectExprsMap;
    }

    public static Set<ExprId> combineProjectAndChildExprId(Plan b, List<NamedExpression> bProject) {
        return Stream.concat(
                b.getOutput().stream().map(NamedExpression::getExprId),
                bProject.stream().map(NamedExpression::getExprId)).collect(Collectors.toSet());
    }

    /**
     * If projectExprs is empty or project output equal plan output, return the original plan.
     */
    public static Plan projectOrSelf(List<NamedExpression> projectExprs, Plan plan) {
        if (projectExprs.isEmpty() || projectExprs.stream().map(NamedExpression::getExprId).collect(Collectors.toSet())
                .equals(plan.getOutputExprIdSet())) {
            return plan;
        }
        return new LogicalProject<>(projectExprs, plan);
    }
}
