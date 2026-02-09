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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.DiffOutputInAsterisk;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.LazyCompute;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The node of logical plan for sub query and alias
 *
 * @param <CHILD_TYPE> param
 */
public class LogicalSubQueryAlias<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements DiffOutputInAsterisk {

    protected RelationId relationId;
    private final List<String> qualifier;
    private final Optional<List<String>> columnAliases;
    // AnalyzeCTE will check this flag to deal with recursive and normal CTE respectively
    private final Supplier<Boolean> isRecursiveCte;

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
        this.isRecursiveCte = computeIsRecursiveCte();
    }

    @Override
    public List<Slot> computeOutput() {
        return computeOutputInternal(false);
    }

    @Override
    public List<Slot> computeAsteriskOutput() {
        return computeOutputInternal(true);
    }

    private List<Slot> computeOutputInternal(boolean asteriskOutput) {
        List<Slot> childOutput = asteriskOutput ? child().getAsteriskOutput() : child().getOutput();
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
            List<String> originQualifier = originSlot.getQualifier();

            ArrayList<String> newQualifier = Lists.newArrayList(originQualifier);
            if (newQualifier.size() >= qualifier.size()) {
                for (int j = 0; j < qualifier.size(); j++) {
                    newQualifier.set(newQualifier.size() - qualifier.size() + j, qualifier.get(j));
                }
            } else if (newQualifier.isEmpty()) {
                newQualifier.addAll(qualifier);
            }

            Slot qualified = originSlot
                    .withQualifier(newQualifier)
                    .withName(columnAlias);
            currentOutput.add(qualified);
        }
        return currentOutput.build();
    }

    private Supplier<Boolean> computeIsRecursiveCte() {
        return LazyCompute.of(() -> {
            // we need check if any relation's name is same as alias query to know if it's recursive cte first
            // so in later AnalyzeCTE, we could deal with recursive cte and normal cte separately
            // it's a little ugly, maybe we can find a better way in future
            List<UnboundRelation> relationList = new ArrayList<>(8);
            collectRelationsInCurrentCte(this, relationList);
            for (UnboundRelation relation : relationList) {
                List<String> nameParts = relation.getNameParts();
                if (nameParts.size() == 1) {
                    String aliasName = getAlias();
                    String tablename = nameParts.get(0);
                    if (GlobalVariable.lowerCaseTableNames != 0) {
                        aliasName = aliasName.toLowerCase(Locale.ROOT);
                        tablename = tablename.toLowerCase(Locale.ROOT);
                    }
                    if (aliasName.equals(tablename)) {
                        return true;
                    }
                }
            }
            return false;
        });
    }

    void collectRelationsInCurrentCte(Plan plan, List<UnboundRelation> relationList) {
        for (Plan child : plan.children()) {
            if (child instanceof UnboundRelation) {
                relationList.add((UnboundRelation) child);
            }
            if (!(child instanceof LogicalCTE)) {
                collectRelationsInCurrentCte(child, relationList);
            }
        }
    }

    public boolean isRecursiveCte() {
        return isRecursiveCte.get();
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
    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(child().toDigest()).append(") AS ");
        sb.append(qualifier.get(0));
        if (columnAliases.isPresent()) {
            columnAliases.get().stream()
                    .collect(Collectors.joining(", ", "(", ")"));
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalSubQueryAlias<?> that = (LogicalSubQueryAlias) o;
        return qualifier.equals(that.qualifier);
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
