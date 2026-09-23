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
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tracks declared PK/FK constraints and the scan-slot lineage needed to eliminate PK/FK joins.
 *
 * <p>A declared key is useful only while all of its slots still represent one complete relation
 * instance. Aliases preserve that lineage, whereas operators such as joins and limits invalidate
 * the primary-key slots they output.
 */
public class ForeignKeyContext {
    /** Exact foreign-to-primary column mappings used after matching join slots. */
    Set<Map<QualifiedColumn, QualifiedColumn>> constraints = new HashSet<>();
    /** Foreign-side column sets indexed for the frequent isForeignKey membership check. */
    Set<Set<QualifiedColumn>> foreignKeyColumnSets = new HashSet<>();
    /** Declared primary keys; each column includes its owning table identity. */
    Set<Set<QualifiedColumn>> primaryKeys = new HashSet<>();
    /** Scan PK slots and their direct aliases that have not been expired by a row-changing plan. */
    Set<Slot> activePrimaryKeySlots = new HashSet<>();
    /** Original table column represented by each scan slot or direct alias. */
    Map<Slot, QualifiedColumn> slotToColumn = new HashMap<>();
    /** Scan instance that produced each slot; table identity alone cannot distinguish self-joins. */
    Map<Slot, RelationId> slotToRelationId = new HashMap<>();
    /** Filter conjuncts accumulated for each slot and rewritten through direct aliases. */
    Map<Slot, Set<Expression>> slotWithPredicates = new HashMap<>();

    /**
     * Collect declared constraints, slot lineage, and predicates by visiting the plan bottom-up.
     * A scan activates a declared primary key only when it reads the complete relation. Projects
     * and filters retain the relevant proof; other operators expire primary-key output slots.
     *
     * @param plan root of the plan whose PK/FK join may be eliminated
     * @return this context, populated with the plan's constraint information
     */
    public ForeignKeyContext collectForeignKeyConstraint(Plan plan) {
        plan.accept(new DefaultPlanVisitor<Void, ForeignKeyContext>() {
            /**
             * Visit children first, then expire PK status for this operator's output. Only the
             * dedicated project and filter visitors preserve PK slots, so joins and limits cannot
             * accidentally pass a scan proof to a parent join.
             */
            @Override
            public Void visit(Plan plan, ForeignKeyContext context) {
                super.visit(plan, context);
                // Operators without a dedicated proof-preserving visitor invalidate primary-key
                // proofs for their output slots. Filters and projects preserve the proof; joins,
                // limits, and other operators expire it. Foreign-key metadata remains available.
                context.expirePrimaryKey(plan);
                return null;
            }

            /**
             * Register declared keys and original slot lineage at a catalog scan. Non-catalog
             * relations have no table constraint metadata and contribute nothing to this context.
             */
            @Override
            public Void visitLogicalRelation(LogicalRelation relation, ForeignKeyContext context) {
                if (relation instanceof LogicalCatalogRelation) {
                    TableIf table = ((LogicalCatalogRelation) relation).getTable();
                    context.putAllForeignKeys(table);
                    Set<Set<QualifiedColumn>> tablePrimaryKeys = context.putAllPrimaryKeys(table);
                    context.putSlots((LogicalCatalogRelation) relation, table, tablePrimaryKeys);
                }
                return null;
            }

            /**
             * Visit the child, then copy lineage only for aliases whose child is already a slot.
             * Computed expressions are not interchangeable with the original constrained column.
             */
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

            /**
             * Visit the child, then record visible conjuncts for its tracked output slots. A
             * parent PK/FK join can be removed only if its foreign side implies these filters.
             */
            @Override
            public Void visitLogicalFilter(LogicalFilter<?> filter, ForeignKeyContext context) {
                super.visit(filter, context);
                context.addFilter(filter);
                return null;
            }
        }, this);
        return this;
    }

    /**
     * Load a table's declared foreign-key mappings and index their column sets for membership
     * checks. Different mappings with the same foreign columns remain in {@code constraints} for
     * the later exact foreign-to-primary mapping check.
     *
     * @param table catalog table whose FK declarations should be registered
     */
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
            foreignKeyColumnSets.add(constraint.keySet());
        }
    }

    /**
     * Load a table's declared primary-key column sets into the context-wide lookup, then return
     * only this table's declarations for scan activation. The declaration is trusted as metadata;
     * whether a particular scan can use it is decided separately by
     * {@link #canActivatePrimaryKey(LogicalCatalogRelation)}.
     *
     * @param table catalog table whose PK declarations should be registered
     * @return declared primary keys belonging to this table, excluding unrelated tables' keys
     */
    Set<Set<QualifiedColumn>> putAllPrimaryKeys(TableIf table) {
        Set<Set<QualifiedColumn>> tablePrimaryKeys = new HashSet<>();
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo == null) {
            return tablePrimaryKeys;
        }
        for (PrimaryKeyConstraint c : Env.getCurrentEnv().getConstraintManager()
                .getPrimaryKeyConstraints(tableNameInfo)) {
            Set<QualifiedColumn> primaryKey = c.getPrimaryKeys(table).stream()
                    .map(column -> new QualifiedColumn(table, column))
                    .collect(ImmutableSet.toImmutableSet());
            tablePrimaryKeys.add(primaryKey);
            primaryKeys.add(primaryKey);
        }
        return tablePrimaryKeys;
    }

    /**
     * Check that the slots are exactly one declared foreign key from one relation instance.
     * Matching only table-qualified columns would incorrectly combine components from two aliases
     * of the same table; {@code slotToRelationId} prevents that combination.
     *
     * @param key candidate foreign-side join slots
     * @return true only for a complete declared FK from one scan instance
     */
    public boolean isForeignKey(Set<Slot> key) {
        return matchesDeclaredKey(key, foreignKeyColumnSets);
    }

    /**
     * Check that all slots still have an active scan proof and form a complete declared primary
     * key of one relation instance. Alias combinations are checked without storing every variant.
     *
     * @param key candidate primary-side join slots
     * @return true only while a complete declared PK remains active
     */
    public boolean isPrimaryKey(Set<Slot> key) {
        return activePrimaryKeySlots.containsAll(key) && matchesDeclaredKey(key, primaryKeys);
    }

    /**
     * Match a slot set against declared keys without collapsing repeated columns or mixing
     * relation instances. The size comparison rejects two aliases of one component being treated
     * as two distinct components of a composite key.
     *
     * @param key candidate slots from a join condition
     * @param declaredKeys table-qualified PK or FK column sets
     * @return true if the slots exactly match one declared key from one scan instance
     */
    private boolean matchesDeclaredKey(Set<Slot> key, Set<Set<QualifiedColumn>> declaredKeys) {
        if (key.isEmpty()) {
            return false;
        }
        RelationId relationId = slotToRelationId.get(key.iterator().next());
        if (relationId == null || key.stream().anyMatch(slot -> !relationId.equals(slotToRelationId.get(slot)))) {
            return false;
        }
        Set<QualifiedColumn> columns = key.stream()
                .map(slotToColumn::get)
                .collect(Collectors.toSet());
        return key.size() == columns.size()
                && !columns.contains(null)
                && declaredKeys.contains(columns);
    }

    /**
     * Register a current-state scan's table columns and relation instance for both FK and PK
     * proofs. Historical snapshots, change reads, and raw-version scans cannot use the current
     * constraint metadata: even if their slots are not active PKs, recording their FK lineage
     * could eliminate a join against a different table version. Activate only this table's
     * complete PKs when the scan covers the full relation; local declarations avoid revisiting
     * earlier tables' keys.
     *
     * @param relation catalog scan contributing the slots and relation identity
     * @param table catalog table containing the declared columns
     * @param tablePrimaryKeys declared PK column sets belonging to this scan's table
     */
    void putSlots(LogicalCatalogRelation relation, TableIf table,
            Set<Set<QualifiedColumn>> tablePrimaryKeys) {
        if (!canUseCurrentConstraint(relation)) {
            return;
        }
        Map<QualifiedColumn, Slot> columnToSlot = new HashMap<>();
        for (Slot slot : relation.getOutput()) {
            if (!(slot instanceof SlotReference) || !((SlotReference) slot).getOriginalColumn().isPresent()) {
                continue;
            }
            Column column = ((SlotReference) slot).getOriginalColumn().get();
            QualifiedColumn qualifiedColumn = new QualifiedColumn(table, column);
            slotToColumn.put(slot, qualifiedColumn);
            slotToRelationId.put(slot, relation.getRelationId());
            columnToSlot.put(qualifiedColumn, slot);
        }

        if (tablePrimaryKeys.isEmpty() || !canActivatePrimaryKey(relation)) {
            return;
        }
        for (Set<QualifiedColumn> primaryKey : tablePrimaryKeys) {
            if (!columnToSlot.keySet().containsAll(primaryKey)) {
                continue;
            }
            Set<Slot> primaryKeySlots = primaryKey.stream()
                    .map(columnToSlot::get)
                    .collect(ImmutableSet.toImmutableSet());
            activePrimaryKeySlots.addAll(primaryKeySlots);
        }
    }

    /**
     * Check whether a scan reads the current table state assumed by its declared constraints.
     * A subset of current rows can still use an FK proof, but historical snapshots, explicit
     * branches/tags/options, native or external change reads, and raw-version scan modes may have
     * different relationships from the current PK table. Stream scans are conservatively excluded
     * for the same reason. Raw-version modes are rejected here, rather than only when activating a
     * PK, because a historical foreign row can also make join elimination unsound.
     *
     * @param relation catalog scan whose version and read mode are inspected
     * @return true if no known version selector, change-read mode, or raw-version mode is active
     */
    boolean canUseCurrentConstraint(LogicalCatalogRelation relation) {
        if (relation instanceof LogicalOlapTableStreamScan) {
            return false;
        }
        if (relation instanceof LogicalOlapScan) {
            LogicalOlapScan scan = (LogicalOlapScan) relation;
            return !scan.getScanParams().isPresent()
                    && !scan.isDuplicateProducingScanMode();
        }
        if (relation instanceof LogicalFileScan) {
            LogicalFileScan scan = (LogicalFileScan) relation;
            return !scan.getTableSnapshot().isPresent() && !scan.getScanParams().isPresent();
        }
        return true;
    }

    /**
     * Determine whether a scan reads the full relation described by its declared primary key.
     * This checks relation coverage after current-state eligibility has rejected versioned and
     * duplicate-producing reads. It deliberately does not check the data trait's inferred
     * uniqueness: PK constraints are declarative assumptions, and a trait check is not a
     * validation of stored data.
     *
     * @param relation scan whose output is compared with the declared table relation
     * @return true if no known scan selector or mode invalidates the PK proof
     */
    boolean canActivatePrimaryKey(LogicalCatalogRelation relation) {
        if (!canUseCurrentConstraint(relation)) {
            return false;
        }
        if (relation instanceof LogicalOlapScan) {
            LogicalOlapScan scan = (LogicalOlapScan) relation;
            return new HashSet<>(scan.getSelectedPartitionIds()).equals(
                            new HashSet<>(scan.getTable().getPartitionIds()))
                    && scan.getSelectedTabletIds().isEmpty()
                    && !scan.getTableSample().isPresent()
                    && !scan.isDirectMvScan();
        }
        if (relation instanceof LogicalFileScan) {
            LogicalFileScan scan = (LogicalFileScan) relation;
            LogicalFileScan.SelectedPartitions partitions = scan.getSelectedPartitions();
            boolean scansAllPartitions = !partitions.isPruned
                    || partitions.totalPartitionNum == partitions.selectedPartitions.size();
            return scansAllPartitions
                    && !scan.getTableSample().isPresent();
        }
        return true;
    }

    /**
     * Propagate column identity, relation identity, active PK status, and rewritten predicates
     * from a direct slot alias. An alias of a computed expression carries none of this lineage.
     *
     * @param newSlot output slot introduced by a direct alias
     * @param originSlot input slot referenced by that alias
     */
    void putAlias(Slot newSlot, Slot originSlot) {
        if (slotToColumn.containsKey(originSlot)) {
            slotToColumn.put(newSlot, slotToColumn.get(originSlot));
            slotToRelationId.put(newSlot, slotToRelationId.get(originSlot));
            if (activePrimaryKeySlots.contains(originSlot)) {
                activePrimaryKeySlots.add(newSlot);
            }
            if (slotWithPredicates.containsKey(originSlot)) {
                Set<Expression> aliasPredicates = slotWithPredicates.get(originSlot).stream()
                        .map(predicate -> predicate.rewriteUp(expression -> expression.equals(originSlot)
                                ? newSlot : expression))
                        .collect(Collectors.toSet());
                slotWithPredicates.put(newSlot, aliasPredicates);
            }
        }
    }

    /**
     * Recognize internal delete-sign predicates, which filter hidden storage rows rather than
     * impose a user-visible restriction that must be matched on the foreign-key side.
     *
     * @param expression filter conjunct to inspect
     * @return true if it references a storage delete-sign column
     */
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

    /**
     * Associate each tracked output slot with the filter's visible conjuncts. Hidden delete-sign
     * predicates are omitted because they are internal storage filtering, not a join restriction.
     *
     * @param filter logical filter whose conjuncts apply to its output slots
     */
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

    /**
     * Expire primary-key status for this operator's output slots. The declared constraint and FK
     * lineage remain available, but a parent join can no longer use these slots as a PK proof.
     *
     * @param plan operator whose output is no longer known to preserve a complete PK relation
     */
    private void expirePrimaryKey(Plan plan) {
        activePrimaryKeySlots.removeAll(plan.getOutputSet());
    }

    /**
     * Count active PK slots, including direct aliases. This test hook verifies that aliasing a
     * composite key adds one slot entry per alias rather than enumerating every key combination.
     *
     * @return number of active PK slot entries
     */
    int activePrimaryKeySlotCount() {
        return activePrimaryKeySlots.size();
    }

    /**
     * Check whether a complete primary-to-foreign slot mapping matches a declared FK constraint.
     * Before comparing the exact column mapping, require foreign-side filters to imply the
     * primary-side filters after substituting corresponding join slots.
     *
     * @param primaryToForeign primary-side join slot to corresponding foreign-side slot
     * @return true if the mapping and predicates satisfy one declared FK constraint
     */
    public boolean satisfyConstraint(Map<Slot, Slot> primaryToForeign) {
        if (primaryToForeign.isEmpty()) {
            return false;
        }
        Map<QualifiedColumn, QualifiedColumn> foreignToPrimary = primaryToForeign.entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(
                        e -> slotToColumn.get(e.getValue()),
                        e -> slotToColumn.get(e.getKey())));
        // The foreign key's filters must contain primary filters
        if (!isPredicateCompatible(primaryToForeign)) {
            return false;
        }
        return constraints.contains(foreignToPrimary);
    }

    /**
     * Require each primary-side predicate to appear on its corresponding foreign-side slot.
     * Rewriting the primary predicate through the join mapping lets expression equality compare
     * the two sides in the same slot namespace.
     *
     * @param primaryToForeign primary-side join slot to corresponding foreign-side slot
     * @return true if every primary-side predicate also holds on the foreign side
     */
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

    /**
     * A column identity qualified by its owning catalog table. Relation instance identity is
     * tracked separately because two aliases of this same column compare equal here.
     */
    private static final class QualifiedColumn {
        private final TableIdentifier tableIdentifier;
        private final Column column;

        /**
         * Bind a catalog column to its table so columns from different tables never collide.
         *
         * @param table catalog owner of the column
         * @param column original column object exposed by the scan slot
         */
        private QualifiedColumn(TableIf table, Column column) {
            this.tableIdentifier = new TableIdentifier(table);
            this.column = column;
        }

        /**
         * Compare catalog table and column identities; relation alias equality is handled by
         * {@code slotToRelationId} when matching a candidate key.
         */
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

        /**
         * Hash the table and column identities used by {@link #equals(Object)} so qualified
         * columns can be looked up in declared key sets and FK mappings.
         */
        @Override
        public int hashCode() {
            return Objects.hash(tableIdentifier, column);
        }
    }
}
