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

import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.PredicateInferUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * infer additional predicates for `LogicalFilter` and `LogicalJoin`.
 * <pre>
 * The logic is as follows:
 * 1. poll up bottom predicate then infer additional predicates
 *   for example:
 *   select * from (select * from t1 where t1.id = 1) t join t2 on t.id = t2.id
 *   1. poll up bottom predicate
 *      select * from (select * from t1 where t1.id = 1) t join t2 on t.id = t2.id and t.id = 1
 *   2. infer
 *      select * from (select * from t1 where t1.id = 1) t join t2 on t.id = t2.id and t.id = 1 and t2.id = 1
 *   finally transformed sql:
 *      select * from (select * from t1 where t1.id = 1) t join t2 on t.id = t2.id and t2.id = 1
 * 2. put these predicates into `otherJoinConjuncts` , these predicates are processed in the next
 *   round of predicate push-down
 * </pre>
 */
public class InferPredicates extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    private final PullUpPredicates pullUpPredicates = new PullUpPredicates(false);
    // The role of pullUpAllPredicates is to prevent inference of redundant predicates
    private final PullUpPredicates pullUpAllPredicates = new PullUpPredicates(true);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        // Preparing stmt requires that the predicate cannot be changed, so no predicate inference is performed.
        ConnectContext connectContext = jobContext.getCascadesContext().getConnectContext();
        if (connectContext != null && connectContext.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
            return plan;
        }
        return plan.accept(this, jobContext);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, JobContext context) {
        join = visitChildren(this, join, context);
        if (join.isMarkJoin()) {
            return join;
        }
        Plan left = join.left();
        Plan right = join.right();
        Set<Expression> expressions = getAllExpressions(left, right, join.getOnClauseCondition());
        switch (join.getJoinType()) {
            case INNER_JOIN:
            case CROSS_JOIN:
            case LEFT_SEMI_JOIN:
            case RIGHT_SEMI_JOIN:
                left = inferNewPredicate(left, expressions);
                right = inferNewPredicate(right, expressions);
                break;
            case LEFT_OUTER_JOIN:
            case LEFT_ANTI_JOIN:
                right = inferNewPredicate(right, expressions);
                break;
            case RIGHT_OUTER_JOIN:
            case RIGHT_ANTI_JOIN:
                left = inferNewPredicate(left, expressions);
                break;
            default:
                break;
        }
        if (left != join.left() || right != join.right()) {
            return join.withChildren(left, right);
        } else {
            return join;
        }
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, JobContext context) {
        filter = visitChildren(this, filter, context);
        Set<Expression> filterPredicates = pullUpPredicates(filter);
        filterPredicates.removeAll(pullUpAllPredicates(filter.child()));
        return new LogicalFilter<>(ImmutableSet.copyOf(filterPredicates), filter.child());
    }

    @Override
    public Plan visitLogicalExcept(LogicalExcept except, JobContext context) {
        except = visitChildren(this, except, context);
        Set<Expression> baseExpressions = pullUpPredicates(except);
        if (baseExpressions.isEmpty()) {
            return except;
        }
        ImmutableList.Builder<Plan> builder = ImmutableList.builder();
        builder.add(except.child(0));
        for (int i = 1; i < except.arity(); ++i) {
            Map<Expression, Expression> replaceMap = new HashMap<>();
            for (int j = 0; j < except.getOutput().size(); ++j) {
                NamedExpression output = except.getOutput().get(j);
                replaceMap.put(output, except.getRegularChildOutput(i).get(j));
            }
            builder.add(inferNewPredicate(except.child(i), ExpressionUtils.replace(baseExpressions, replaceMap)));
        }
        return except.withChildren(builder.build());
    }

    @Override
    public Plan visitLogicalIntersect(LogicalIntersect intersect, JobContext context) {
        intersect = visitChildren(this, intersect, context);
        Set<Expression> baseExpressions = pullUpPredicates(intersect);
        if (baseExpressions.isEmpty()) {
            return intersect;
        }
        ImmutableList.Builder<Plan> builder = ImmutableList.builder();
        for (int i = 0; i < intersect.arity(); ++i) {
            Map<Expression, Expression> replaceMap = new HashMap<>();
            for (int j = 0; j < intersect.getOutput().size(); ++j) {
                NamedExpression output = intersect.getOutput().get(j);
                replaceMap.put(output, intersect.getRegularChildOutput(i).get(j));
            }
            builder.add(inferNewPredicate(intersect.child(i), ExpressionUtils.replace(baseExpressions, replaceMap)));
        }
        return intersect.withChildren(builder.build());
    }

    private Set<Expression> getAllExpressions(Plan left, Plan right, Optional<Expression> condition) {
        Set<Expression> baseExpressions = pullUpPredicates(left);
        baseExpressions.addAll(pullUpPredicates(right));
        condition.ifPresent(on -> baseExpressions.addAll(ExpressionUtils.extractConjunction(on)));
        return PredicateInferUtils.inferPredicate(baseExpressions);
    }

    private Set<Expression> pullUpPredicates(Plan plan) {
        return Sets.newLinkedHashSet(plan.accept(pullUpPredicates, null));
    }

    private Set<Expression> pullUpAllPredicates(Plan plan) {
        return Sets.newLinkedHashSet(plan.accept(pullUpAllPredicates, null));
    }

    private Plan inferNewPredicate(Plan plan, Set<Expression> expressions) {
        Set<Expression> predicates = new LinkedHashSet<>();
        Set<Slot> planOutputs = plan.getOutputSet();
        for (Expression expr : expressions) {
            Set<Slot> slots = expr.getInputSlots();
            if (!slots.isEmpty() && planOutputs.containsAll(slots)) {
                predicates.add(expr);
            }
        }
        predicates.removeAll(plan.accept(pullUpAllPredicates, null));
        return PlanUtils.filterOrSelf(predicates, plan);
    }
}
