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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Util for plan
 */
public class PlanUtils {
    public static Optional<LogicalFilter<? extends Plan>> filter(Set<Expression> predicates, Plan plan) {
        if (predicates.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LogicalFilter<>(predicates, plan));
    }

    public static Plan filterOrSelf(Set<Expression> predicates, Plan plan) {
        return filter(predicates, plan).map(Plan.class::cast).orElse(plan);
    }

    /**
     * normalize comparison predicate on a binary plan to its two sides are corresponding to the child's output.
     */
    public static ComparisonPredicate maybeCommuteComparisonPredicate(ComparisonPredicate expression, Plan left) {
        Set<Slot> slots = expression.left().getInputSlots();
        Set<Slot> leftSlots = left.getOutputSet();
        Set<Slot> buffer = Sets.newHashSet(slots);
        buffer.removeAll(leftSlots);
        return buffer.isEmpty() ? expression : expression.commute();
    }

    public static Optional<LogicalProject<? extends Plan>> project(List<NamedExpression> projects, Plan plan) {
        if (projects.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LogicalProject<>(projects, plan));
    }

    public static Plan projectOrSelf(List<NamedExpression> projects, Plan plan) {
        return project(projects, plan).map(Plan.class::cast).orElse(plan);
    }

    /**
     * merge childProjects with parentProjects
     */
    public static List<NamedExpression> mergeProjections(List<NamedExpression> childProjects,
            List<NamedExpression> parentProjects) {
        Map<Expression, Alias> replaceMap =
                childProjects.stream().filter(e -> e instanceof Alias).collect(
                        Collectors.toMap(NamedExpression::toSlot, e -> (Alias) e, (v1, v2) -> v1));
        return parentProjects.stream().map(expr -> {
            if (expr instanceof Alias) {
                Alias alias = (Alias) expr;
                Expression insideExpr = alias.child();
                Expression newInsideExpr = insideExpr.rewriteUp(e -> {
                    Alias getAlias = replaceMap.get(e);
                    return getAlias == null ? e : getAlias.child();
                });
                return newInsideExpr == insideExpr ? expr
                        : alias.withChildren(ImmutableList.of(newInsideExpr));
            } else {
                Alias getAlias = replaceMap.get(expr);
                return getAlias == null ? expr : getAlias;
            }
        }).collect(ImmutableList.toImmutableList());
    }

    public static Plan skipProjectFilterLimit(Plan plan) {
        if (plan instanceof LogicalProject && ((LogicalProject<?>) plan).isAllSlots()
                || plan instanceof LogicalFilter || plan instanceof LogicalLimit) {
            return plan.child(0);
        }
        return plan;
    }
}
