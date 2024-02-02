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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.FdFactory;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.FunctionalDependencies;
import org.apache.doris.nereids.properties.FunctionalDependencies.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.TableFdItem;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * abstract class catalog relation for logical relation
 */
public abstract class LogicalCatalogRelation extends LogicalRelation implements CatalogRelation {

    protected final TableIf table;
    // [catalogName, databaseName, tableName]
    protected final ImmutableList<String> qualifier;

    public LogicalCatalogRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier) {
        super(relationId, type);
        this.table = Objects.requireNonNull(table, "table can not be null");
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier can not be null"));
    }

    public LogicalCatalogRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(relationId, type, groupExpression, logicalProperties);
        this.table = Objects.requireNonNull(table, "table can not be null");
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier can not be null"));
    }

    @Override
    public TableIf getTable() {
        return table;
    }

    @Override
    public DatabaseIf getDatabase() throws AnalysisException {
        Preconditions.checkArgument(!qualifier.isEmpty(), "qualifier can not be empty");
        try {
            CatalogIf catalog = qualifier.size() == 3
                    ? Env.getCurrentEnv().getCatalogMgr().getCatalogOrException(qualifier.get(0),
                        s -> new Exception("Catalog [" + qualifier.get(0) + "] does not exist."))
                    : Env.getCurrentEnv().getCurrentCatalog();
            return catalog.getDbOrException(qualifier.size() == 3 ? qualifier.get(1) : qualifier.get(0),
                    s -> new Exception("Database [" + qualifier.get(1) + "] does not exist in catalog ["
                        + qualifier.get(0) + "]."));
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    public List<Slot> computeOutput() {
        return table.getBaseSchema()
                .stream()
                .map(col -> SlotReference.fromColumn(table, col, qualified(), this))
                .collect(ImmutableList.toImmutableList());
    }

    public List<String> getQualifier() {
        return qualifier;
    }

    /**
     * Full qualified name parts, i.e., concat qualifier and name into a list.
     */
    public List<String> qualified() {
        if (qualifier.size() == 3) {
            return qualifier;
        }
        return Utils.qualifiedNameParts(qualifier, table.getName());
    }

    /**
     * Full qualified table name, concat qualifier and name with `.` as separator.
     */
    public String qualifiedName() {
        if (qualifier.size() == 3) {
            return StringUtils.join(qualifier, ".");
        }
        return Utils.qualifiedName(qualifier, table.getName());
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems(Supplier<List<Slot>> outputSupplier) {
        Set<NamedExpression> output = ImmutableSet.copyOf(outputSupplier.get());
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();
        table.getPrimaryKeyConstraints().forEach(c -> {
            Set<Column> columns = c.getPrimaryKeys(this.getTable());
            ImmutableSet<SlotReference> slotSet = output.stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .filter(s -> s.getColumn().isPresent()
                            && columns.contains(s.getColumn().get()))
                    .collect(ImmutableSet.toImmutableSet());
            TableFdItem tableFdItem = FdFactory.INSTANCE.createTableFdItem(slotSet, true, false, ImmutableSet.of(table));
            builder.add(tableFdItem);
        });
        table.getUniqueConstraints().forEach(c -> {
            Set<Column> columns = c.getUniqueKeys(this.getTable());
            boolean allNotNull = columns.stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .allMatch(s -> !s.nullable());
            if (allNotNull) {
                ImmutableSet<SlotReference> slotSet = output.stream()
                        .filter(SlotReference.class::isInstance)
                        .map(SlotReference.class::cast)
                        .filter(s -> s.getColumn().isPresent()
                                && columns.contains(s.getColumn().get()))
                        .collect(ImmutableSet.toImmutableSet());
                TableFdItem tableFdItem = FdFactory.INSTANCE.createTableFdItem(slotSet,
                        true, false, ImmutableSet.of(table));
                builder.add(tableFdItem);
            } else {
                ImmutableSet<SlotReference> slotSet = output.stream()
                        .filter(SlotReference.class::isInstance)
                        .map(SlotReference.class::cast)
                        .filter(s -> s.getColumn().isPresent()
                                && columns.contains(s.getColumn().get()))
                        .collect(ImmutableSet.toImmutableSet());
                TableFdItem tableFdItem = FdFactory.INSTANCE.createTableFdItem(slotSet,
                        true, true, ImmutableSet.of(table));
                builder.add(tableFdItem);
            }
        });
        return builder.build();
    }
}
