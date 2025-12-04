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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.TypeUtils;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * project(A.*)
 *  - filter(B.slot is null)
 *    - LeftOuterJoin(A, B)
 * ==============================>
 * project(A.*)
 *    - LeftAntiJoin(A, B)
 */
public class ConvertOuterJoinToAntiJoin extends DefaultPlanRewriter<Map<ExprId, ExprId>> implements CustomRewriter {
    private ExprIdRewriter exprIdReplacer;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        if (!plan.containsType(LogicalJoin.class)) {
            return plan;
        }
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        ExprIdRewriter.ReplaceRule replaceRule = new ExprIdRewriter.ReplaceRule(replaceMap);
        exprIdReplacer = new ExprIdRewriter(replaceRule, jobContext);
        return plan.accept(this, replaceMap);
    }

    @Override
    public Plan visit(Plan plan, Map<ExprId, ExprId> replaceMap) {
        plan = visitChildren(this, plan, replaceMap);
        plan = exprIdReplacer.rewriteExpr(plan, replaceMap);
        return plan;
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Map<ExprId, ExprId> replaceMap) {
        filter = (LogicalFilter<? extends Plan>) visit(filter, replaceMap);
        if (!(filter.child() instanceof LogicalJoin)) {
            return filter;
        }
        return toAntiJoin((LogicalFilter<LogicalJoin<Plan, Plan>>) filter, replaceMap);
    }

    private Plan toAntiJoin(LogicalFilter<LogicalJoin<Plan, Plan>> filter, Map<ExprId, ExprId> replaceMap) {
        LogicalJoin<Plan, Plan> join = filter.child();
        if (!join.getJoinType().isLeftOuterJoin() && !join.getJoinType().isRightOuterJoin()) {
            return filter;
        }

        Set<Slot> alwaysNullSlots = filter.getConjuncts().stream()
                .filter(p -> TypeUtils.isNull(p).isPresent())
                .flatMap(p -> p.getInputSlots().stream())
                .collect(Collectors.toSet());
        Set<Slot> leftAlwaysNullSlots = join.left().getOutputSet().stream()
                .filter(s -> alwaysNullSlots.contains(s) && !s.nullable())
                .collect(Collectors.toSet());
        Set<Slot> rightAlwaysNullSlots = join.right().getOutputSet().stream()
                .filter(s -> alwaysNullSlots.contains(s) && !s.nullable())
                .collect(Collectors.toSet());

        Plan newChild = null;
        if (join.getJoinType().isLeftOuterJoin() && !rightAlwaysNullSlots.isEmpty()) {
            newChild = join.withJoinTypeAndContext(JoinType.LEFT_ANTI_JOIN, join.getJoinReorderContext());
        }
        if (join.getJoinType().isRightOuterJoin() && !leftAlwaysNullSlots.isEmpty()) {
            newChild = join.withJoinTypeAndContext(JoinType.RIGHT_ANTI_JOIN, join.getJoinReorderContext());
        }
        if (newChild == null) {
            return filter;
        }

        if (!newChild.getOutputSet().containsAll(filter.getInputSlots())) {
            // if there are slots that don't belong to join output, we use null alias to replace them
            // such as:
            //   project(A.id, null as B.id)
            //       -  (A left anti join B)
            Set<Slot> joinOutputs = newChild.getOutputSet();
            ImmutableList.Builder<NamedExpression> projectsBuilder = ImmutableList.builder();
            for (NamedExpression e : filter.getOutput()) {
                if (joinOutputs.contains(e)) {
                    projectsBuilder.add(e);
                } else {
                    Alias newAlias = new Alias(new NullLiteral(e.getDataType()), e.getName(), e.getQualifier());
                    replaceMap.put(e.getExprId(), newAlias.getExprId());
                    projectsBuilder.add(newAlias);
                }
            }
            newChild = new LogicalProject<>(projectsBuilder.build(), newChild);
            return exprIdReplacer.rewriteExpr(filter.withChildren(newChild), replaceMap);
        } else {
            return filter.withChildren(newChild);
        }
    }
}
