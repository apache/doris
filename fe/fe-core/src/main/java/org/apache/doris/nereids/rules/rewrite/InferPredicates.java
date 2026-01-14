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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

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
    private PullUpPredicates pullUpPredicates;
    // The role of pullUpAllPredicates is to prevent inference of redundant predicates
    private PullUpPredicates pullUpAllPredicates;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        // Preparing stmt requires that the predicate cannot be changed, so no predicate inference is performed.
        ConnectContext connectContext = jobContext.getCascadesContext().getConnectContext();
        if (connectContext != null && connectContext.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
            return plan;
        }
        pullUpPredicates = new PullUpPredicates(false, jobContext.getCascadesContext());
        pullUpAllPredicates = new PullUpPredicates(true, jobContext.getCascadesContext());
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
            case CROSS_JOIN:
                left = inferNewPredicate(left, expressions);
                right = inferNewPredicate(right, expressions);
                break;
            case INNER_JOIN:
            case LEFT_SEMI_JOIN:
            case RIGHT_SEMI_JOIN:
                left = inferNewPredicateRemoveUselessIsNull(left, expressions, join, context.getCascadesContext());
                right = inferNewPredicateRemoveUselessIsNull(right, expressions, join, context.getCascadesContext());
                break;
            case LEFT_OUTER_JOIN:
            case LEFT_ANTI_JOIN:
                right = inferNewPredicateRemoveUselessIsNull(right, expressions, join, context.getCascadesContext());
                break;
            case RIGHT_OUTER_JOIN:
            case RIGHT_ANTI_JOIN:
                left = inferNewPredicateRemoveUselessIsNull(left, expressions, join, context.getCascadesContext());
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
        if (filter.getConjuncts().contains(BooleanLiteral.FALSE)) {
            return new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(), filter.getOutput());
        }
        filter = visitChildren(this, filter, context);
        Set<Expression> inferredPredicates = pullUpPredicates(filter);
        inferredPredicates.removeAll(pullUpAllPredicates(filter.child()));
        if (inferredPredicates.isEmpty()) {
            return filter.child();
        }
        if (inferredPredicates.equals(filter.getConjuncts())) {
            return filter;
        } else {
            return new LogicalFilter<>(ImmutableSet.copyOf(inferredPredicates), filter.child());
        }
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
        boolean changed = false;
        for (int i = 1; i < except.arity(); ++i) {
            Map<Expression, Expression> replaceMap = new HashMap<>();
            for (int j = 0; j < except.getOutput().size(); ++j) {
                NamedExpression output = except.getOutput().get(j);
                replaceMap.put(output, except.getRegularChildOutput(i).get(j));
            }
            Plan newChild = inferNewPredicate(except.child(i), ExpressionUtils.replace(baseExpressions, replaceMap));
            changed = changed || newChild != except.child(i);
            builder.add(newChild);
        }
        return changed ? except.withChildren(builder.build()) : except;
    }

    @Override
    public Plan visitLogicalIntersect(LogicalIntersect intersect, JobContext context) {
        intersect = visitChildren(this, intersect, context);
        Set<Expression> baseExpressions = pullUpPredicates(intersect);
        if (baseExpressions.isEmpty()) {
            return intersect;
        }
        ImmutableList.Builder<Plan> builder = ImmutableList.builder();
        boolean changed = false;
        for (int i = 0; i < intersect.arity(); ++i) {
            Map<Expression, Expression> replaceMap = new HashMap<>();
            for (int j = 0; j < intersect.getOutput().size(); ++j) {
                NamedExpression output = intersect.getOutput().get(j);
                replaceMap.put(output, intersect.getRegularChildOutput(i).get(j));
            }
            Plan newChild = inferNewPredicate(intersect.child(i), ExpressionUtils.replace(baseExpressions, replaceMap));
            changed = changed || newChild != intersect.child(i);
            builder.add(newChild);
        }
        return changed ? intersect.withChildren(builder.build()) : intersect;
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

    // Remove redundant "or is null" from expressions.
    // For example, when we have a t2 left join t3 condition t2.a=t3.a, we can infer that t3.a is not null.
    // If we find a predicate like "t3.a = 1 or t3.a is null" in expressions, we change it to "t3.a=1".
    private Plan inferNewPredicateRemoveUselessIsNull(Plan plan, Set<Expression> expressions,
            LogicalJoin<? extends Plan, ? extends Plan> join, CascadesContext cascadesContext) {
        Supplier<Set<Slot>> supplier = Suppliers.memoize(() -> {
            Set<Expression> all = new HashSet<>();
            all.addAll(join.getHashJoinConjuncts());
            all.addAll(join.getOtherJoinConjuncts());
            return ExpressionUtils.inferNotNullSlots(all, cascadesContext);
        });

        Set<Expression> predicates = new LinkedHashSet<>();
        Set<Slot> planOutputs = plan.getOutputSet();
        for (Expression expr : expressions) {
            Set<Slot> slots = expr.getInputSlots();
            if (slots.isEmpty() || !planOutputs.containsAll(slots)) {
                continue;
            }
            if (expr instanceof Or && expr.isInferred()) {
                List<Expression> orChildren = ExpressionUtils.extractDisjunction(expr);
                List<Expression> newOrChildren = Lists.newArrayList();
                boolean changed = false;
                for (Expression orChild : orChildren) {
                    if (orChild instanceof IsNull && orChild.child(0) instanceof Slot
                            && supplier.get().contains(orChild.child(0))) {
                        changed = true;
                        continue;
                    }
                    newOrChildren.add(orChild);
                }
                if (changed) {
                    if (newOrChildren.size() == 1) {
                        predicates.add(withInferredIfSupported(newOrChildren.get(0), expr));
                    } else if (newOrChildren.size() > 1) {
                        predicates.add(ExpressionUtils.or(newOrChildren).withInferred(true));
                    }
                } else {
                    predicates.add(expr);
                }
            } else {
                predicates.add(expr);
            }
        }
        predicates.removeAll(plan.accept(pullUpAllPredicates, null));
        return PlanUtils.filterOrSelf(predicates, plan);
    }

    private Expression withInferredIfSupported(Expression expression, Expression originExpr) {
        try {
            return expression.withInferred(true);
        } catch (RuntimeException e) {
            return originExpr;
        }
    }
}
