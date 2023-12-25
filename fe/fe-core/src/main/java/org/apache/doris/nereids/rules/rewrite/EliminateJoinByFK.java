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
import org.apache.doris.catalog.Table;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ImmutableEquivalenceSet;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
            context.passThroughPlan(plan);
            return newPlan;
        }

        @Override
        public Plan visitLogicalRelation(LogicalRelation relation, ForeignKeyContext context) {
            if (!(relation instanceof LogicalCatalogRelation)) {
                return relation;
            }
            if (!(((LogicalCatalogRelation) relation).getTable() instanceof Table)) {
                return relation;
            }
            context.putAllForeignKeys(((LogicalCatalogRelation) relation).getTable());
            relation.getOutput().stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .forEach(context::putSlot);
            return relation;
        }

        private boolean canEliminate(LogicalJoin<?, ?> join, BiMap<Slot, Slot> equalSlots, ForeignKeyContext context) {
            if (!join.getOtherJoinConjuncts().isEmpty()) {
                return false;
            }
            if (!join.getJoinType().isInnerJoin() && !join.getJoinType().isSemiJoin()) {
                return false;
            }
            return context.satisfyConstraint(equalSlots, join);
        }

        private boolean isForeignKeyAndUnique(Plan plan,
                Set<Slot> keySet, ForeignKeyContext context) {
            boolean unique = keySet.stream()
                    .allMatch(s -> plan.getLogicalProperties().getFunctionalDependencies().isUnique(s));
            return unique && context.isForeignKey(keySet);
        }

        private @Nullable Map<Expression, Expression> tryGetOutputToChildMap(Plan child,
                Set<Slot> output, BiMap<Slot, Slot> equalSlots) {
            Set<Slot> residual = Sets.difference(output, child.getOutputSet());
            if (equalSlots.keySet().containsAll(residual)) {
                return residual.stream()
                        .collect(ImmutableMap.toImmutableMap(e -> e, equalSlots::get));
            }
            if (equalSlots.values().containsAll(residual)) {
                return residual.stream()
                        .collect(ImmutableMap.toImmutableMap(e -> e, e -> equalSlots.inverse().get(e)));
            }
            return null;
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

        private @Nullable Plan tryConstructPlanWithJoinChild(LogicalProject<LogicalJoin<?, ?>> project, Plan child,
                BiMap<Slot, Slot> equalSlots, ForeignKeyContext context) {
            Set<Slot> output = project.getInputSlots();
            Set<Slot> keySet = child.getOutputSet().containsAll(equalSlots.keySet())
                    ? equalSlots.keySet()
                    : equalSlots.values();
            Map<Expression, Expression> outputToRight = tryGetOutputToChildMap(child, output, equalSlots);
            if (outputToRight != null && isForeignKeyAndUnique(child, keySet, context)) {
                List<NamedExpression> newProjects = project.getProjects().stream()
                        .map(e -> outputToRight.containsKey(e)
                                ? new Alias(e.getExprId(), outputToRight.get(e), e.toSql())
                                : (NamedExpression) e.rewriteUp(s -> outputToRight.getOrDefault(s, s)))
                        .collect(ImmutableList.toImmutableList());
                return project.withProjects(newProjects)
                        .withChildren(applyNullCompensationFilter(child, keySet));
            }
            return null;
        }

        // Right now we only support eliminate inner join, which should meet the following condition:
        // 1. only contain null-reject equal condition, and which all meet fk-pk constraint
        // 2. only output foreign table output or can be converted to foreign table output
        // 3. foreign key is unique
        // 4. if foreign key is null, add a isNotNull predicate for null-reject join condition
        private Plan eliminateJoin(LogicalProject<LogicalJoin<?, ?>> project, ForeignKeyContext context) {
            LogicalJoin<?, ?> join = project.child();
            ImmutableEquivalenceSet<Slot> equalSet = join.getEqualSlots();
            BiMap<Slot, Slot> equalSlots = equalSet.tryToMap();
            if (equalSlots == null) {
                return project;
            }
            if (!canEliminate(join, equalSlots, context)) {
                return project;
            }
            Plan keepLeft = tryConstructPlanWithJoinChild(project, join.left(), equalSlots, context);
            if (keepLeft != null) {
                return keepLeft;
            }
            Plan keepRight = tryConstructPlanWithJoinChild(project, join.right(), equalSlots, context);
            if (keepRight != null) {
                return keepRight;
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
    }

    private static class ForeignKeyContext {
        static Set<Class<?>> propagatePrimaryKeyOperator = ImmutableSet
                .<Class<?>>builder()
                .add(LogicalProject.class)
                .add(LogicalSort.class)
                .add(LogicalJoin.class)
                .build();
        static Set<Class<?>> propagateForeignKeyOperator = ImmutableSet
                .<Class<?>>builder()
                .add(LogicalProject.class)
                .add(LogicalSort.class)
                .add(LogicalJoin.class)
                .add(LogicalFilter.class)
                .add(LogicalTopN.class)
                .add(LogicalLimit.class)
                .add(LogicalAggregate.class)
                .add(LogicalWindow.class)
                .build();
        Set<Map<Column, Column>> constraints = new HashSet<>();
        Set<Column> foreignKeys = new HashSet<>();
        Set<Column> primaryKeys = new HashSet<>();
        Map<Slot, Column> slotToColumn = new HashMap<>();
        Map<Column, Set<LogicalJoin<?, ?>>> columnWithJoin = new HashMap<>();
        Map<Column, Set<Expression>> columnWithPredicates = new HashMap<>();

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

        public void passThroughPlan(Plan plan) {
            Set<Column> output = plan.getOutput().stream()
                    .filter(slotToColumn::containsKey)
                    .map(s -> slotToColumn.get(s))
                    .collect(ImmutableSet.toImmutableSet());
            if (plan instanceof LogicalJoin) {
                output.forEach(c ->
                        columnWithJoin.computeIfAbsent(c, v -> Sets.newHashSet((LogicalJoin<?, ?>) plan)));
                return;
            }
            if (plan instanceof LogicalFilter) {
                output.forEach(c -> {
                    columnWithPredicates.computeIfAbsent(c, v -> new HashSet<>());
                    columnWithPredicates.get(c).addAll(((LogicalFilter<?>) plan).getConjuncts());
                });
                return;
            }
            if (!propagatePrimaryKeyOperator.contains(plan.getClass())) {
                output.forEach(primaryKeys::remove);
            }
            if (!propagateForeignKeyOperator.contains(plan.getClass())) {
                output.forEach(foreignKeys::remove);
            }
        }

        public boolean satisfyConstraint(BiMap<Slot, Slot> equalSlots, LogicalJoin<?, ?> join) {
            ImmutableMap.Builder<Column, Column> foreignToPrimaryBuilder = new Builder<>();
            for (Entry<Slot, Slot> entry : equalSlots.entrySet()) {
                Slot left = entry.getKey();
                Slot right = entry.getValue();
                if (!slotToColumn.containsKey(left) || !slotToColumn.containsKey(right)) {
                    return false;
                }
                Column leftColumn = slotToColumn.get(left);
                Column rightColumn = slotToColumn.get(right);
                if (foreignKeys.contains(leftColumn)) {
                    foreignToPrimaryBuilder.put(leftColumn, rightColumn);
                } else if (foreignKeys.contains(rightColumn)) {
                    foreignToPrimaryBuilder.put(rightColumn, leftColumn);
                } else {
                    return false;
                }
            }
            Map<Column, Column> foreignToPrimary = foreignToPrimaryBuilder.build();
            // The primary key can only contain join that may be eliminated
            if (!foreignToPrimary.values().stream().allMatch(p ->
                    columnWithJoin.get(p).size() == 1 && columnWithJoin.get(p).iterator().next() == join)) {
                return false;
            }
            // The foreign key's filters must contain primary filters
            if (!isPredicateCompatible(equalSlots, foreignToPrimary)) {
                return false;
            }
            return constraints.contains(foreignToPrimary);
        }

        private boolean isPredicateCompatible(BiMap<Slot, Slot> equalSlots, Map<Column, Column> foreignToPrimary) {
            return foreignToPrimary.entrySet().stream().allMatch(fp -> {
                BiMap<Slot, Slot> primarySlotToForeign = equalSlots.keySet().stream()
                        .map(slotToColumn::get)
                        .anyMatch(primaryKeys::contains)
                        ? equalSlots :
                        equalSlots.inverse();
                if (!columnWithPredicates.containsKey(fp.getValue())) {
                    return true;
                }
                Set<Expression> primaryPredicates = columnWithPredicates.get(fp.getValue()).stream()
                        .map(e -> e.rewriteUp(
                                s -> s instanceof Slot ? primarySlotToForeign.getOrDefault(s, (Slot) s) : s))
                        .collect(Collectors.toSet());
                if (columnWithPredicates.get(fp.getKey()) == null && !columnWithPredicates.isEmpty()) {
                    return false;
                } else {
                    return columnWithPredicates.get(fp.getKey()).containsAll(primaryPredicates);
                }
            });
        }
    }
}
