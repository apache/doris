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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Record Foreign Key Context
 */
public class ForeignKeyContext {
    Set<Map<Column, Column>> constraints = new HashSet<>();
    Set<Column> foreignKeys = new HashSet<>();
    Set<Column> primaryKeys = new HashSet<>();
    Map<Slot, Column> slotToColumn = new HashMap<>();
    Map<Slot, Set<Expression>> slotWithPredicates = new HashMap<>();

    /**
     * Collect Foreign Key Constraint From this Plan
     */
    public ForeignKeyContext collectForeignKeyConstraint(Plan plan) {
        plan.accept(new DefaultPlanVisitor<Void, ForeignKeyContext>() {
            @Override
            public Void visit(Plan plan, ForeignKeyContext context) {
                super.visit(plan, context);
                // always expire primary key except filter, project and join.
                // always keep foreign key alive
                context.expirePrimaryKey(plan);
                return null;
            }

            @Override
            public Void visitLogicalRelation(LogicalRelation relation, ForeignKeyContext context) {
                if (relation instanceof LogicalCatalogRelation) {
                    context.putAllForeignKeys(((LogicalCatalogRelation) relation).getTable());
                    context.putAllPrimaryKeys(((LogicalCatalogRelation) relation).getTable());
                    relation.getOutput().stream()
                            .filter(SlotReference.class::isInstance)
                            .map(SlotReference.class::cast)
                            .forEach(context::putSlot);
                }
                return null;
            }

            @Override
            public Void visitLogicalProject(LogicalProject<?> project, ForeignKeyContext context) {
                super.visit(project, context);
                for (NamedExpression expression : project.getProjects()) {
                    if (expression instanceof Alias && expression.child(0) instanceof Slot) {
                        context.putAlias(expression.toSlot(), (Slot) expression.child(0));
                    }
                }
                return null;
            }

            @Override
            public Void visitLogicalFilter(LogicalFilter<?> filter, ForeignKeyContext context) {
                super.visit(filter, context);
                context.addFilter(filter);
                return null;
            }
        }, this);
        return this;
    }

    void putAllForeignKeys(TableIf table) {
        table.getForeignKeyConstraints().forEach(c -> {
            Map<Column, Column> constraint = c.getForeignToPrimary(table);
            constraints.add(c.getForeignToPrimary(table));
            foreignKeys.addAll(constraint.keySet());
        });
    }

    void putAllPrimaryKeys(TableIf table) {
        table.getPrimaryKeyConstraints().forEach(c -> {
            Set<Column> primaryKey = c.getPrimaryKeys(table);
            primaryKeys.addAll(primaryKey);
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

    void putSlot(SlotReference slot) {
        if (!slot.getColumn().isPresent()) {
            return;
        }
        Column c = slot.getColumn().get();
        slotToColumn.put(slot, c);
    }

    void putAlias(Slot newSlot, Slot originSlot) {
        if (slotToColumn.containsKey(originSlot)) {
            slotToColumn.put(newSlot, slotToColumn.get(originSlot));
        }
    }

    private boolean isHiddenConjunct(Expression expression) {
        for (Slot slot : expression.getInputSlots()) {
            if (slot instanceof SlotReference
                    && ((SlotReference) slot).getColumn().isPresent()
                    && ((SlotReference) slot).getColumn().get().isDeleteSignColumn()) {
                return true;
            }
        }
        return false;
    }

    private void addFilter(LogicalFilter<?> filter) {
        for (Slot s : filter.getOutput()) {
            if (slotToColumn.containsKey(s)) {
                slotWithPredicates.computeIfAbsent(s, v -> new HashSet<>());
                for (Expression conjunct : filter.getConjuncts()) {
                    if (!isHiddenConjunct(conjunct)) {
                        slotWithPredicates.get(s).add(conjunct);
                    }
                }
            }
        }
    }

    private void expirePrimaryKey(Plan plan) {
        plan.getOutput().stream()
                .filter(slotToColumn::containsKey)
                .map(s -> slotToColumn.get(s))
                .forEach(primaryKeys::remove);
    }

    /**
     * Check whether the given mapping relation satisfies any constraints
     */
    public boolean satisfyConstraint(Map<Slot, Slot> primaryToForeign) {
        Map<Column, Column> foreignToPrimary = primaryToForeign.entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(
                        e -> slotToColumn.get(e.getValue()),
                        e -> slotToColumn.get(e.getKey())));
        if (primaryToForeign.isEmpty()) {
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
            if (!slotWithPredicates.containsKey(pf.getValue()) || slotWithPredicates.get(pf.getValue()).isEmpty()) {
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
