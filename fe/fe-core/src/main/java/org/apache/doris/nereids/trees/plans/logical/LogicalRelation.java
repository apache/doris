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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Scan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical relation plan.
 */
public abstract class LogicalRelation extends LogicalLeaf implements Scan {

    protected final TableIf table;
    protected final ImmutableList<String> qualifier;

    protected final RelationId id;

    public LogicalRelation(RelationId id, PlanType type, TableIf table, List<String> qualifier) {
        this(id, type, table, qualifier, Optional.empty(), Optional.empty());
    }

    public LogicalRelation(RelationId id, PlanType type, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        this(id, type, new OlapTable(), Collections.emptyList(), groupExpression,
                logicalProperties);
    }

    /**
     * Constructor for LogicalRelationPlan.
     *
     * @param table Doris table
     * @param qualifier qualified relation name
     */
    public LogicalRelation(RelationId id, PlanType type, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(type, groupExpression, logicalProperties);
        this.table = Objects.requireNonNull(table, "table can not be null");
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier can not be null"));
        this.id = id;
    }

    @Override
    public TableIf getTable() {
        return table;
    }

    public List<String> getQualifier() {
        return qualifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalRelation that = (LogicalRelation) o;
        return this.id.equals(that.getId())
                && Objects.equals(this.table.getId(), that.table.getId())
                && Objects.equals(this.qualifier, that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public List<Slot> computeOutput() {
        return table.getBaseSchema()
                .stream()
                .map(col -> SlotReference.fromColumn(col, qualified()))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalRelation(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    /**
     * Full qualified name parts, i.e., concat qualifier and name into a list.
     */
    public List<String> qualified() {
        return Utils.qualifiedNameParts(qualifier, table.getName());
    }

    /**
     * Full qualified table name, concat qualifier and name with `.` as separator.
     */
    public String qualifiedName() {
        return Utils.qualifiedName(qualifier, table.getName());
    }

    public RelationId getId() {
        return id;
    }
}
