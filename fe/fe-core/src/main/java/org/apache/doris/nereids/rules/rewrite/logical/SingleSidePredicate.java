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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Arithmetic;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * push down expression which is not slot reference
 */
public class SingleSidePredicate extends OneRewriteRuleFactory {

    /**
     * key : slotReference of a arithmetic expression
     * value : {
     *     key : arithmetic expression
     *     value : aliased arithmetic expression
     * }
     */
    private final Map<SlotReference, Map<Expression, Expression>> SlotReferenceToExpressionMap = new HashMap<>();

    /**
     * rewrite example:
     *join(t1.a + 1 = t2.b + 2 and t1.a > 2)        join(c = d and c > 2)
     *          │                                             │
     *          ├─olapScan(t1)                                ├────project(t1.a + 1 as c)
     *          │                               ====>         │       │
     *          └─olapScan(t2)                                │       └─olapScan(t1)
     *                                                        │
     *                                                        └────project(t2.b + 2 as d)
     *                                                                │
     *                                                                └─olapScan(t2)
     */
    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getJoinType() == JoinType.INNER_JOIN
                        && join.getCondition().isPresent())
                .then(join -> {
                    join.getCondition().get().accept(
                            new TreeCollectVisitor(),
                            SlotReferenceToExpressionMap);

                    List<Plan> children = join.children().stream()
                            .map(GroupPlan.class::cast)
                            .map(p -> {
                                List<NamedExpression> slots = p.getOutput()
                                                .stream()
                                                .filter(SlotReference.class::isInstance)
                                                .filter(ref -> SlotReferenceToExpressionMap.containsKey(ref))
                                                .map(ref -> SlotReferenceToExpressionMap.get(ref).values())
                                                .flatMap(Collection::stream)
                                                .map(NamedExpression.class::cast)
                                                .collect(Collectors.toList());
                                return new LogicalProject<>(slots, p);
                            })
                            .collect(Collectors.toList());

                    LogicalJoin<Plan, Plan> joinAddProjects = (LogicalJoin<Plan, Plan>) join.withChildren(children);

                    return joinAddProjects
                            .withCondition(Optional.of(join.getCondition().get().accept(
                                    new TreeReplaceVisitor(),
                                    SlotReferenceToExpressionMap.values().stream()
                                            .reduce(new HashMap<>(), (map1, map2) -> {
                                                map1.putAll(map2);
                                                return map1;
                                            })
                            ))
                    );
                }).toRule(RuleType.PUSH_DOWN_NOT_SLOT_REFERENCE_EXPRESSION);
    }

    private static class TreeCollectVisitor extends ExpressionVisitor<Expression,
            Map<SlotReference, Map<Expression, Expression>>> {

        @Override
        public Expression visit(Expression expr,
                Map<SlotReference, Map<Expression, Expression>> context) {
            Set<SlotReference> set = new HashSet<>(expr.collect(SlotReference.class::isInstance));
            if (set.size() == 1) {
                SlotReference ref = set.iterator().next();
                context.computeIfAbsent(ref, key -> new HashMap<>()).put(expr, new Alias(expr, expr.toSql()));
            }
            return expr;
        }

        public Expression visitChildren(Expression expr,
                Map<SlotReference, Map<Expression, Expression>> ctx) {
            expr.children().forEach(node -> node.accept(this, ctx));
            return expr;
        }

        @Override
        public Expression visitComparisonPredicate(ComparisonPredicate predicate,
                Map<SlotReference, Map<Expression, Expression>> ctx) {
            return visitChildren(predicate, ctx);
        }

        @Override
        public Expression visitCompoundPredicate(CompoundPredicate predicate,
                Map<SlotReference, Map<Expression, Expression>> ctx) {
            return visitChildren(predicate, ctx);
        }
    }

    private static class TreeReplaceVisitor extends ExpressionVisitor<Expression, Map<Expression, Expression>> {

        @Override
        public Expression visit(Expression expr, Map<Expression, Expression> context) {
            expr.children().forEach(node -> node.accept(this, context));
            return expr;
        }

        @Override
        public Expression visitLiteral(Literal literal, Map<Expression, Expression> context) {
            return literal;
        }

        @Override
        public Expression visitArithmetic(Arithmetic arithmetic, Map<Expression, Expression> ctx) {
            return ((Alias) ctx.get(arithmetic)).toSlot();
        }

        @Override
        public Expression visitComparisonPredicate(ComparisonPredicate predicate, Map<Expression, Expression> ctx) {
            return predicate.withChildren(predicate.children()
                    .stream()
                    .map(node -> node.accept(this, ctx))
                    .collect(Collectors.toList()));
        }

        @Override
        public Expression visitCompoundPredicate(CompoundPredicate predicate, Map<Expression, Expression> ctx) {
            return predicate.withChildren(predicate.children()
                    .stream()
                    .map(node -> node.accept(this, ctx))
                    .collect(Collectors.toList()));
        }
    }
}
