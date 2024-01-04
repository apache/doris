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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ImmutableEquivalenceSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Eliminate join by foreign.
 */
public class EliminateJoinByFK extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        EliminateJoinByFKHelper helper = new EliminateJoinByFKHelper();
        return helper.rewriteRoot(plan, jobContext);
    }

    private static class EliminateJoinByFKHelper
            extends DefaultPlanRewriter<ForeignKeyContext> implements CustomRewriter {

        @Override
        public Plan rewriteRoot(Plan plan, JobContext jobContext) {
            return plan.accept(this, new ForeignKeyContext());
        }

        @Override
        public Plan visit(Plan plan, ForeignKeyContext context) {
            Plan newPlan = visitChildren(this, plan, context);
            // always expire primary key except filter, project and join.
            // always keep foreign key alive
            context.expirePrimaryKey(plan);
            return newPlan;
        }

        @Override
        public Plan visitLogicalRelation(LogicalRelation relation, ForeignKeyContext context) {
            if (!(relation instanceof LogicalCatalogRelation)) {
                return relation;
            }
            context.putAllForeignKeys(((LogicalCatalogRelation) relation).getTable());
            relation.getOutput().stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .forEach(context::putSlot);
            return relation;
        }

        private boolean canEliminate(LogicalJoin<?, ?> join, Map<Slot, Slot> primaryToForeign,
                ForeignKeyContext context) {
            if (!join.getOtherJoinConjuncts().isEmpty()) {
                return false;
            }
            if (!join.getJoinType().isInnerJoin() && !join.getJoinType().isSemiJoin()) {
                return false;
            }
            return context.satisfyConstraint(primaryToForeign, join);
        }

        private @Nullable Map<Expression, Expression> tryMapOutputToForeignPlan(Plan foreignPlan,
                Set<Slot> output, Map<Slot, Slot> primaryToForeign) {
            Set<Slot> residualPrimary = Sets.difference(output, foreignPlan.getOutputSet());
            ImmutableMap.Builder<Expression, Expression> builder = new ImmutableMap.Builder<>();
            for (Slot slot : residualPrimary) {
                if (primaryToForeign.containsKey(slot)) {
                    builder.put(slot, primaryToForeign.get(slot));
                } else {
                    return null;
                }
            }
            return builder.build();
        }

        private Plan applyNullCompensationFilter(Plan child, Set<Slot> childSlots) {
            Set<Expression> predicates = childSlots.stream()
                    .filter(ExpressionTrait::nullable)
                    .map(s -> new Not(new IsNull(s)))
                    .collect(ImmutableSet.toImmutableSet());
            if (predicates.isEmpty()) {
                return child;
            }
            return new LogicalFilter<>(predicates, child);
        }

        private Plan tryEliminatePrimaryPlan(LogicalProject<LogicalJoin<?, ?>> project,
                Plan foreignPlan, Set<Slot> foreignKeys,
                Map<Slot, Slot> primaryToForeign, ForeignKeyContext context) {
            Set<Slot> output = project.getInputSlots();
            Map<Expression, Expression> outputToForeign =
                    tryMapOutputToForeignPlan(foreignPlan, output, primaryToForeign);
            if (outputToForeign != null && canEliminate(project.child(), primaryToForeign, context)) {
                List<NamedExpression> newProjects = project.getProjects().stream()
                        .map(e -> outputToForeign.containsKey(e)
                                ? new Alias(e.getExprId(), outputToForeign.get(e), e.toSql())
                                : (NamedExpression) e.rewriteUp(s -> outputToForeign.getOrDefault(s, s)))
                        .collect(ImmutableList.toImmutableList());
                return project.withProjects(newProjects)
                        .withChildren(applyNullCompensationFilter(foreignPlan, foreignKeys));
            }
            return project;
        }

        private @Nullable Map<Slot, Slot> mapPrimaryToForeign(ImmutableEquivalenceSet<Slot> equivalenceSet,
                Set<Slot> foreignKeys) {
            ImmutableMap.Builder<Slot, Slot> builder = new ImmutableMap.Builder<>();
            for (Slot foreignSlot : foreignKeys) {
                Set<Slot> primarySlots = equivalenceSet.calEqualSet(foreignSlot);
                if (primarySlots.size() != 1) {
                    return null;
                }
                builder.put(primarySlots.iterator().next(), foreignSlot);
            }
            return builder.build();
        }

        // Right now we only support eliminate inner join, which should meet the following condition:
        // 1. only contain null-reject equal condition, and which all meet fk-pk constraint
        // 2. only output foreign table output or can be converted to foreign table output
        // 4. if foreign key is null, add a isNotNull predicate for null-reject join condition
        private Plan eliminateJoin(LogicalProject<LogicalJoin<?, ?>> project, ForeignKeyContext context) {
            LogicalJoin<?, ?> join = project.child();
            ImmutableEquivalenceSet<Slot> equalSet = join.getEqualSlots();
            Set<Slot> leftSlots = Sets.intersection(join.left().getOutputSet(), equalSet.getAllItemSet());
            Set<Slot> rightSlots = Sets.intersection(join.right().getOutputSet(), equalSet.getAllItemSet());
            if (context.isForeignKey(leftSlots) && context.isPrimaryKey(rightSlots)) {
                Map<Slot, Slot> primaryToForeignSlot = mapPrimaryToForeign(equalSet, leftSlots);
                if (primaryToForeignSlot != null) {
                    return tryEliminatePrimaryPlan(project, join.left(), leftSlots, primaryToForeignSlot, context);
                }
            } else if (context.isForeignKey(rightSlots) && context.isPrimaryKey(leftSlots)) {
                Map<Slot, Slot> primaryToForeignSlot = mapPrimaryToForeign(equalSet, rightSlots);
                if (primaryToForeignSlot != null) {
                    return tryEliminatePrimaryPlan(project, join.right(), rightSlots, primaryToForeignSlot, context);
                }
            }
            return project;
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<?> project, ForeignKeyContext context) {
            project = visitChildren(this, project, context);
            for (NamedExpression expression : project.getProjects()) {
                if (expression instanceof Alias && expression.child(0) instanceof Slot) {
                    context.putAlias(expression.toSlot(), (Slot) expression.child(0));
                }
            }
            if (project.child() instanceof LogicalJoin<?, ?>) {
                return eliminateJoin((LogicalProject<LogicalJoin<?, ?>>) project, context);
            }
            return project;
        }

        @Override
        public Plan visitLogicalJoin(LogicalJoin<?, ?> join, ForeignKeyContext context) {
            Plan plan = visitChildren(this, join, context);
            context.addJoin(join);
            return plan;
        }

        @Override
        public Plan visitLogicalFilter(LogicalFilter<?> filter, ForeignKeyContext context) {
            Plan plan = visitChildren(this, filter, context);
            context.addFilter(filter);
            return plan;
        }
    }

    private static class ForeignKeyContext {
        Set<Map<Column, Column>> constraints = new HashSet<>();
        Set<Column> foreignKeys = new HashSet<>();
        Set<Column> primaryKeys = new HashSet<>();
        Map<Slot, Column> slotToColumn = new HashMap<>();
        Map<Slot, Set<LogicalJoin<?, ?>>> slotWithJoin = new HashMap<>();
        Map<Slot, Set<Expression>> slotWithPredicates = new HashMap<>();

        public void putAllForeignKeys(TableIf table) {
            table.getForeignKeyConstraints().forEach(c -> {
                Map<Column, Column> constraint = c.getForeignToPrimary(table);
                constraints.add(c.getForeignToPrimary(table));
                foreignKeys.addAll(constraint.keySet());
                primaryKeys.addAll(constraint.values());
            });
        }

        public boolean isForeignKey(Set<Slot> key) {
            return foreignKeys.containsAll(
                    key.stream().map(s -> slotToColumn.get(s)).collect(Collectors.toSet()));
        }

        public boolean isPrimaryKey(Set<Slot> key) {
            return primaryKeys.containsAll(
                    key.stream().map(s -> slotToColumn.get(s)).collect(Collectors.toSet()));
        }

        public void putSlot(SlotReference slot) {
            if (!slot.getColumn().isPresent()) {
                return;
            }
            Column c = slot.getColumn().get();
            slotToColumn.put(slot, c);
        }

        public void putAlias(Slot newSlot, Slot originSlot) {
            if (slotToColumn.containsKey(originSlot)) {
                slotToColumn.put(newSlot, slotToColumn.get(originSlot));
            }
        }

        public void addFilter(LogicalFilter<?> filter) {
            filter.getOutput().stream()
                    .filter(slotToColumn::containsKey)
                    .forEach(slot -> {
                        slotWithPredicates.computeIfAbsent(slot, v -> new HashSet<>());
                        slotWithPredicates.get(slot).addAll(filter.getConjuncts());
                    });
        }

        public void addJoin(LogicalJoin<?, ?> join) {
            join.getOutput().stream()
                    .filter(slotToColumn::containsKey)
                    .forEach(slot ->
                            slotWithJoin.computeIfAbsent(slot, v -> Sets.newHashSet((join))));
        }

        public void expirePrimaryKey(Plan plan) {
            plan.getOutput().stream()
                    .filter(slotToColumn::containsKey)
                    .map(s -> slotToColumn.get(s))
                    .forEach(primaryKeys::remove);
        }

        public boolean satisfyConstraint(Map<Slot, Slot> primaryToForeign, LogicalJoin<?, ?> join) {
            Map<Column, Column> foreignToPrimary = primaryToForeign.entrySet().stream()
                    .collect(ImmutableMap.toImmutableMap(
                            e -> slotToColumn.get(e.getValue()),
                            e -> slotToColumn.get(e.getKey())));
            // The primary key can only contain join that may be eliminated
            if (!primaryToForeign.keySet().stream().allMatch(p ->
                    slotWithJoin.get(p).size() == 1 && slotWithJoin.get(p).iterator().next() == join)) {
                return false;
            }
            // The foreign key's filters must contain primary filters
            if (!isPredicateCompatible(primaryToForeign)) {
                return false;
            }
            return constraints.contains(foreignToPrimary);
        }

        // When predicates of foreign keys is a subset of that of primary keys
        private boolean isPredicateCompatible(Map<Slot, Slot> primaryToForeign) {
            return primaryToForeign.entrySet().stream().allMatch(pf -> {
                // There is no predicate in primary key
                if (!slotWithPredicates.containsKey(pf.getKey()) || slotWithPredicates.get(pf.getKey()).isEmpty()) {
                    return true;
                }
                // There are some predicates in primary key but there is no predicate in foreign key
                if (slotWithPredicates.containsKey(pf.getValue()) && slotWithPredicates.get(pf.getValue()).isEmpty()) {
                    return false;
                }
                Set<Expression> primaryPredicates = slotWithPredicates.get(pf.getKey()).stream()
                        .map(e -> e.rewriteUp(
                                s -> s instanceof Slot ? primaryToForeign.getOrDefault(s, (Slot) s) : s))
                        .collect(Collectors.toSet());
                return slotWithPredicates.get(pf.getValue()).containsAll(primaryPredicates);
            });
        }
    }
}
