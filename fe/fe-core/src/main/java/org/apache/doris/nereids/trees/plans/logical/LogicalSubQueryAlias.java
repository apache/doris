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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The node of logical plan for sub query and alias
 *
 * @param <CHILD_TYPE> param
 */
public class LogicalSubQueryAlias<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {

    protected RelationId relationId;
    private final List<String> qualifier;
    private final Optional<List<String>> columnAliases;

    public LogicalSubQueryAlias(String tableAlias, CHILD_TYPE child) {
        this(ImmutableList.of(tableAlias), Optional.empty(), Optional.empty(), Optional.empty(), child);
    }

    public LogicalSubQueryAlias(List<String> qualifier, CHILD_TYPE child) {
        this(qualifier, Optional.empty(), Optional.empty(), Optional.empty(), child);
    }

    public LogicalSubQueryAlias(String tableAlias, Optional<List<String>> columnAliases, CHILD_TYPE child) {
        this(ImmutableList.of(tableAlias), columnAliases, Optional.empty(), Optional.empty(), child);
    }

    public LogicalSubQueryAlias(List<String> qualifier, Optional<List<String>> columnAliases, CHILD_TYPE child) {
        this(qualifier, columnAliases, Optional.empty(), Optional.empty(), child);
    }

    public LogicalSubQueryAlias(List<String> qualifier, Optional<List<String>> columnAliases,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_SUBQUERY_ALIAS, groupExpression, logicalProperties, child);
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier is null"));
        this.columnAliases = columnAliases;
    }

    @Override
    public List<Slot> computeOutput() {
        List<Slot> childOutput = child().getOutput();
        List<String> columnAliases = this.columnAliases.orElseGet(ImmutableList::of);
        ImmutableList.Builder<Slot> currentOutput = ImmutableList.builder();
        for (int i = 0; i < childOutput.size(); i++) {
            Slot originSlot = childOutput.get(i);
            String columnAlias;
            if (i < columnAliases.size()) {
                columnAlias = columnAliases.get(i);
            } else {
                columnAlias = originSlot.getName();
            }
            Slot qualified = originSlot
                    .withQualifier(qualifier)
                    .withName(columnAlias);
            currentOutput.add(qualified);
        }
        return currentOutput.build();
    }

    public String getAlias() {
        return qualifier.get(qualifier.size() - 1);
    }

    public Optional<List<String>> getColumnAliases() {
        return columnAliases;
    }

    @Override
    public String toString() {
        return columnAliases.map(strings -> Utils.toSqlString("LogicalSubQueryAlias",
                "qualifier", qualifier,
                "columnAliases", StringUtils.join(strings, ",")
        )).orElseGet(() -> Utils.toSqlString("LogicalSubQueryAlias",
                "qualifier", qualifier
        ));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalSubQueryAlias that = (LogicalSubQueryAlias) o;
        return qualifier.equals(that.qualifier) && this.child().equals(that.child());
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier, child().hashCode());
    }

    @Override
    public LogicalSubQueryAlias<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalSubQueryAlias<>(qualifier, columnAliases, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSubQueryAlias(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public LogicalSubQueryAlias<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalSubQueryAlias<>(qualifier, columnAliases, groupExpression,
                Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalSubQueryAlias<>(qualifier, columnAliases, groupExpression, logicalProperties,
                children.get(0));
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        builder.addUniqueSlot(child(0).getLogicalProperties().getTrait());
        Map<Slot, Slot> replaceMap = new HashMap<>();
        List<Slot> outputs = getOutput();
        for (int i = 0; i < outputs.size(); i++) {
            replaceMap.put(child(0).getOutput().get(i), outputs.get(i));
        }
        builder.replaceUniqueBy(replaceMap);
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
        Map<Slot, Slot> replaceMap = new HashMap<>();
        List<Slot> outputs = getOutput();
        for (int i = 0; i < outputs.size(); i++) {
            replaceMap.put(child(0).getOutput().get(i), outputs.get(i));
        }
        builder.replaceUniformBy(replaceMap);
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems() {
        // TODO: inherit from child with replaceMap
        return ImmutableSet.of();
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child(0).getLogicalProperties().getTrait());
        Map<Slot, Slot> replaceMap = new HashMap<>();
        List<Slot> outputs = getOutput();
        for (int i = 0; i < outputs.size(); i++) {
            replaceMap.put(child(0).getOutput().get(i), outputs.get(i));
        }
        builder.replaceEqualSetBy(replaceMap);
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
        Map<Slot, Slot> replaceMap = new HashMap<>();
        List<Slot> outputs = getOutput();
        for (int i = 0; i < outputs.size(); i++) {
            replaceMap.put(child(0).getOutput().get(i), outputs.get(i));
        }
        builder.replaceFuncDepsBy(replaceMap);
    }

    public void setRelationId(RelationId relationId) {
        this.relationId = relationId;
    }

    public RelationId getRelationId() {
        return relationId;
    }

    public List<String> getQualifier() {
        return qualifier;
    }

    @Override
    public Set<RelationId> getInputRelations() {
        Set<RelationId> relationIdSet = Sets.newHashSet();
        relationIdSet.add(relationId);
        return relationIdSet;
    }
}
