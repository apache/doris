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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.info.TableNameInfoUtils;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Record Foreign Key Context
 */
public class ForeignKeyContext {
    Set<Map<QualifiedColumn, QualifiedColumn>> constraints = new HashSet<>();
    Set<QualifiedColumn> foreignKeys = new HashSet<>();
    Set<QualifiedColumn> declaredPrimaryKeys = new HashSet<>();
    Set<Slot> activePrimaryKeySlots = new HashSet<>();
    Map<Slot, QualifiedColumn> slotToColumn = new HashMap<>();
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
                    TableIf table = ((LogicalCatalogRelation) relation).getTable();
                    context.putAllForeignKeys(table);
                    context.putAllPrimaryKeys(table);
                    relation.getOutput().stream()
                            .filter(SlotReference.class::isInstance)
                            .map(SlotReference.class::cast)
                            .forEach(slot -> context.putSlot(slot, table));
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
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo == null) {
            return;
        }
        for (ForeignKeyConstraint c : Env.getCurrentEnv().getConstraintManager()
                .getForeignKeyConstraints(tableNameInfo)) {
            TableIf referencedTable = c.getReferencedTable();
            Map<QualifiedColumn, QualifiedColumn> constraint = c.getForeignToReference().entrySet().stream()
                    .collect(ImmutableMap.toImmutableMap(
                            entry -> new QualifiedColumn(table, table.getColumn(entry.getKey())),
                            entry -> new QualifiedColumn(
                                    referencedTable, referencedTable.getColumn(entry.getValue()))));
            constraints.add(constraint);
            foreignKeys.addAll(constraint.keySet());
        }
    }

    void putAllPrimaryKeys(TableIf table) {
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo == null) {
            return;
        }
        for (PrimaryKeyConstraint c : Env.getCurrentEnv().getConstraintManager()
                .getPrimaryKeyConstraints(tableNameInfo)) {
            Set<QualifiedColumn> primaryKey = c.getPrimaryKeys(table).stream()
                    .map(column -> new QualifiedColumn(table, column)).collect(Collectors.toSet());
            declaredPrimaryKeys.addAll(primaryKey);
        }
    }

    public boolean isForeignKey(Set<Slot> key) {
        return foreignKeys.containsAll(
                key.stream().map(s -> slotToColumn.get(s)).collect(Collectors.toSet()));
    }

    public boolean isPrimaryKey(Set<Slot> key) {
        return !key.isEmpty() && activePrimaryKeySlots.containsAll(key);
    }

    void putSlot(SlotReference slot, TableIf table) {
        if (!slot.getOriginalColumn().isPresent()) {
            return;
        }
        Column c = slot.getOriginalColumn().get();
        QualifiedColumn qualifiedColumn = new QualifiedColumn(table, c);
        slotToColumn.put(slot, qualifiedColumn);
        if (declaredPrimaryKeys.contains(qualifiedColumn)) {
            activePrimaryKeySlots.add(slot);
        }
    }

    void putAlias(Slot newSlot, Slot originSlot) {
        if (slotToColumn.containsKey(originSlot)) {
            slotToColumn.put(newSlot, slotToColumn.get(originSlot));
            if (activePrimaryKeySlots.contains(originSlot)) {
                activePrimaryKeySlots.add(newSlot);
            }
        }
    }

    private boolean isHiddenConjunct(Expression expression) {
        for (Slot slot : expression.getInputSlots()) {
            if (slot instanceof SlotReference
                    && ((SlotReference) slot).getOriginalColumn().isPresent()
                    && ((SlotReference) slot).getOriginalColumn().get().isDeleteSignColumn()) {
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
        activePrimaryKeySlots.removeAll(plan.getOutput());
    }

    /**
     * Check whether the given mapping relation satisfies any constraints
     */
    public boolean satisfyConstraint(Map<Slot, Slot> primaryToForeign) {
        Map<QualifiedColumn, QualifiedColumn> foreignToPrimary = primaryToForeign.entrySet().stream()
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

    /** A column identity qualified by its owning table. */
    private static final class QualifiedColumn {
        private final TableIdentifier tableIdentifier;
        private final Column column;

        private QualifiedColumn(TableIf table, Column column) {
            this.tableIdentifier = new TableIdentifier(table);
            this.column = column;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof QualifiedColumn)) {
                return false;
            }
            QualifiedColumn other = (QualifiedColumn) obj;
            return tableIdentifier.equals(other.tableIdentifier) && column.equals(other.column);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableIdentifier, column);
        }
    }
}
