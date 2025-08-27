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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.FdFactory;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.TableFdItem;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * abstract class catalog relation for logical relation
 */
public abstract class LogicalCatalogRelation extends LogicalRelation implements CatalogRelation {

    protected final TableIf table;
    // [catalogName, databaseName]
    protected final ImmutableList<String> qualifier;

    protected final ImmutableList<Slot> operativeSlots;

    // use for virtual slot
    protected final List<NamedExpression> virtualColumns;

    public LogicalCatalogRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier) {
        this(relationId, type, table, qualifier, Optional.empty(), Optional.empty());
    }

    public LogicalCatalogRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        this(relationId, type, table, qualifier, ImmutableList.of(), ImmutableList.of(),
                groupExpression, logicalProperties);
    }

    /**
     * Constructs a LogicalCatalogRelation with specified parameters.
     *
     * @param relationId Unique identifier for this relation
     * @param type Plan type
     * @param table Table object associated with this relation
     * @param qualifier List of qualifiers, typically [catalogName, databaseName]
     * @param operativeSlots Collection of operative slots
     * @param virtualColumns List of virtual columns
     * @param groupExpression Optional group expression
     * @param logicalProperties Optional logical properties
     */
    public LogicalCatalogRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier,
            Collection<Slot> operativeSlots, List<NamedExpression> virtualColumns,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(relationId, type, groupExpression, logicalProperties);
        this.table = Objects.requireNonNull(table, "table can not be null");
        this.qualifier = Utils.fastToImmutableList(Objects.requireNonNull(qualifier, "qualifier can not be null"));
        this.operativeSlots = Utils.fastToImmutableList(operativeSlots);
        this.virtualColumns = Utils.fastToImmutableList(Objects.requireNonNull(virtualColumns,
                "virtualColumns can not be null"));
    }

    @Override
    public TableIf getTable() {
        return table;
    }

    @Override
    public DatabaseIf getDatabase() throws AnalysisException {
        Preconditions.checkArgument(!qualifier.isEmpty(), "qualifier can not be empty");
        try {
            int len = qualifier.size();
            if (2 == len) {
                CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCatalogMgr()
                        .getCatalogOrAnalysisException(qualifier.get(0));
                return catalog.getDbOrAnalysisException(qualifier.get(1));
            } else if (1 == len) {
                CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCurrentCatalog();
                return catalog.getDbOrAnalysisException(qualifier.get(0));
            } else if (0 == len) {
                CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCurrentCatalog();
                ConnectContext ctx = ConnectContext.get();
                return catalog.getDb(ctx.getDatabase()).get();
            }
            return null;
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    public List<Slot> computeOutput() {
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        Builder<Slot> slots = ImmutableList.builder();
        table.getBaseSchema()
                .stream()
                .map(col -> SlotReference.fromColumn(exprIdGenerator.getNextId(), table, col, qualified()))
                .forEach(slots::add);
        // add virtual slots
        for (NamedExpression virtualColumn : virtualColumns) {
            slots.add(virtualColumn.toSlot());
        }
        return slots.build();
    }

    @Override
    public List<String> getQualifier() {
        return qualifier;
    }

    /**
     * Full qualified name parts, i.e., concat qualifier and name into a list.
     */
    public List<String> qualified() {
        return Utils.qualifiedNameParts(qualifier, Util.getTempTableDisplayName(table.getName()));
    }

    /**
     * Full qualified table name, concat qualifier and name with `.` as separator.
     */
    public String qualifiedName() {
        return Utils.qualifiedName(qualifier, Util.getTempTableDisplayName(table.getName()));
    }

    @Override
    public List<Slot> getOperativeSlots() {
        return operativeSlots;
    }

    public List<NamedExpression> getVirtualColumns() {
        return virtualColumns;
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        Set<Slot> outputSet = Utils.fastToImmutableSet(getOutputSet());
        for (PrimaryKeyConstraint c : table.getPrimaryKeyConstraints()) {
            Set<Column> columns = c.getPrimaryKeys(table);
            builder.addUniqueSlot((ImmutableSet) findSlotsByColumn(outputSet, columns));
        }

        for (UniqueConstraint c : table.getUniqueConstraints()) {
            Set<Column> columns = c.getUniqueKeys(table);
            builder.addUniqueSlot((ImmutableSet) findSlotsByColumn(outputSet, columns));
        }
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        // No uniform slot for catalog relation
    }

    private ImmutableSet<FdItem> computeFdItems(Set<Slot> outputSet) {
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();

        for (PrimaryKeyConstraint c : table.getPrimaryKeyConstraints()) {
            Set<Column> columns = c.getPrimaryKeys(this.getTable());
            ImmutableSet<SlotReference> slotSet = findSlotsByColumn(outputSet, columns);
            TableFdItem tableFdItem = FdFactory.INSTANCE.createTableFdItem(
                    slotSet, true, false, ImmutableSet.of(table));
            builder.add(tableFdItem);
        }

        for (UniqueConstraint c : table.getUniqueConstraints()) {
            Set<Column> columns = c.getUniqueKeys(this.getTable());
            boolean allNotNull = true;

            for (Column column : columns) {
                if (column.isAllowNull()) {
                    allNotNull = false;
                    break;
                }
            }

            ImmutableSet<SlotReference> slotSet = findSlotsByColumn(outputSet, columns);
            TableFdItem tableFdItem = FdFactory.INSTANCE.createTableFdItem(
                    slotSet, true, !allNotNull, ImmutableSet.of(table));
            builder.add(tableFdItem);
        }
        return builder.build();
    }

    private ImmutableSet<SlotReference> findSlotsByColumn(Set<Slot> outputSet, Set<Column> columns) {
        ImmutableSet.Builder<SlotReference> slotSet = ImmutableSet.builderWithExpectedSize(columns.size());
        for (Slot slot : outputSet) {
            if (!(slot instanceof SlotReference)) {
                continue;
            }
            SlotReference slotRef = (SlotReference) slot;
            if (slotRef.getOriginalColumn().isPresent() && columns.contains(slotRef.getOriginalColumn().get())) {
                slotSet.add(slotRef);
            }
        }
        return slotSet.build();
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        // don't generate any equal pair
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        // don't generate any equal pair
    }

    public LogicalCatalogRelation withVirtualColumns(List<NamedExpression> virtualColumns) {
        return this;
    }

    public abstract LogicalCatalogRelation withRelationId(RelationId relationId);

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalCatalogRelation that = (LogicalCatalogRelation) o;
        return Objects.equals(operativeSlots, that.operativeSlots)
                && Objects.equals(virtualColumns, that.virtualColumns);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
