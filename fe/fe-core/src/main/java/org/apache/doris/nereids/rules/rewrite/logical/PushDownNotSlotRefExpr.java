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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.thoughtworks.qdox.model.expression.NotEquals;

import java.util.ArrayList;
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
public class PushDownNotSlotRefExpr extends OneRewriteRuleFactory {

    private static final IdGenerator<ExprId> ID_GENERATOR = new IdGenerator<ExprId>() {
        @Override
        public ExprId getNextId() {
            return new ExprId(nextId++);
        }

        @Override
        public ExprId getMaxId() {
            return getNextId();
        }

        @Override
        public String toString() {
            return "expr_" + getNextId();
        }
    };

    private static final List<Class> classes = ImmutableList.of(
            And.class,
            Or.class,
            Not.class,
            EqualTo.class,
            GreaterThan.class,
            LessThan.class,
            GreaterThanEqual.class,
            LessThanEqual.class,
            NotEquals.class
    );

    private class ListBuilder<T> {
        List<T> list;

        public ListBuilder() {
            list = new ArrayList<>();
        }

        public ListBuilder(List<T> list) {
            this.list = Lists.newArrayList(list);
        }

        public ListBuilder<T> addAll(List<T> element) {
            list.addAll(Lists.newArrayList(element));
            return this;
        }

        public List<T> get() {
            return list;
        }
    }

    private Map<Expression, Expression> exprMap = new HashMap<>();

    private Map<Integer, Set<Expression>> exprMap1 = new HashMap<>();

    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getCondition().isPresent() && checkExpr(join.getCondition().get()))
                .then(join -> {
                    travelTreeCollectExpr(join.getCondition().get());
                    join = LogicalJoin.class.cast(join.withChildren(join.children().stream()
                            .map(GroupPlan.class::cast).map(p -> {
                                List<NamedExpression> list = new ListBuilder<NamedExpression>(p.getOutput()
                                        .stream()
                                        .map(NamedExpression.class::cast)
                                        .collect(Collectors.toList()))
                                        .addAll(p.getOutput().stream()
                                                .filter(SlotReference.class::isInstance)
                                                .filter(expr -> exprMap1.containsKey(expr.getExprId().asInt()))
                                                .map(expr -> exprMap1.get(expr.getExprId().asInt()))
                                                .map(v -> v.stream().map(expr -> exprMap.get(expr)).collect(
                                                        Collectors.toList()))
                                                .flatMap(Collection::stream)
                                                .map(NamedExpression.class::cast)
                                                .collect(Collectors.toList())
                                        ).get();
                                return new LogicalProject<>(list, p.getGroup().getLogicalExpression().getPlan());
                            }).collect(Collectors.toList())));
                    join = join.withCondition(Optional.of(travelTreeReplaceExpr(join.getCondition().get())));
                    return join;
                }).toRule(RuleType.PUSH_DOWN_NOT_SLOT_REFERENCE_EXPRESSION);
    }

    private boolean check(Expression expr) {
        return classes.stream().anyMatch(c -> c.isInstance(expr));
    }

    private boolean checkExpr(Expression expr) {
        if (check(expr)) {
            return expr.children().stream().anyMatch(this::checkExpr);
        }
        return !(expr instanceof Literal || expr instanceof SlotReference);
    }

    private void travelTreeCollectExpr(Expression root) {
        if (check(root)) {
            root.children().forEach(this::travelTreeCollectExpr);
            return;
        }
        Set<SlotReference> set = findSlotRefOfExpr(root, new HashSet<>());
        if (set.size() == 0) {
            return;
        }
        exprMap.put(root, new Alias(root, ID_GENERATOR.toString()));
        if (set.size() == 1) {
            int refId = set.stream().findFirst().get().getExprId().asInt();
            if (exprMap1.get(refId) == null) {
                exprMap1.put(refId, Sets.newHashSet(root));
            } else {
                exprMap1.get(refId).add(root);
            }
        } else {
            throw new RuntimeException("unsupported");
        }
    }

    private Expression travelTreeReplaceExpr(Expression root) {
        if (check(root)) {
            return root.withChildren(
                    root.children()
                            .stream()
                            .map(this::travelTreeReplaceExpr)
                            .collect(Collectors.toList()));
        }
        if (root instanceof Literal) {
            return root;
        }
        return ((Alias) exprMap.get(root)).toSlot();
    }

    private Set<SlotReference> findSlotRefOfExpr(Expression root, Set<SlotReference> set) {
        if (root instanceof SlotReference) {
            set.add((SlotReference) root);
            return set;
        }
        root.children().forEach(expr -> findSlotRefOfExpr(expr, set));
        return set;
    }
}
