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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represent a relation plan node that has not been bound.
 */
public class UnboundRelation extends LogicalRelation implements Unbound, BlockFuncDepsPropagation {

    private final List<String> nameParts;
    private final List<String> partNames;
    private final List<Long> tabletIds;
    private final boolean isTempPart;
    private final List<String> hints;
    private final Optional<TableSample> tableSample;
    private final Optional<String> indexName;
    private TableScanParams scanParams;
    // the start and end position of the sql substring(e.g. "t1", "db1.t1", "ctl1.db1.t1")
    private final Optional<Pair<Integer, Integer>> indexInSqlString;

    private final Optional<TableSnapshot> tableSnapshot;

    public UnboundRelation(RelationId id, List<String> nameParts) {
        this(id, nameParts, Optional.empty(), Optional.empty(), ImmutableList.of(), false, ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), Optional.empty(), null, Optional.empty(), Optional.empty());
    }

    public UnboundRelation(RelationId id, List<String> nameParts, List<String> partNames, boolean isTempPart) {
        this(id, nameParts, Optional.empty(), Optional.empty(), partNames, isTempPart, ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), Optional.empty(), null, Optional.empty(), Optional.empty());
    }

    public UnboundRelation(RelationId id, List<String> nameParts, List<String> partNames, boolean isTempPart,
            List<Long> tabletIds, List<String> hints, Optional<TableSample> tableSample, Optional<String> indexName) {
        this(id, nameParts, Optional.empty(), Optional.empty(),
                partNames, isTempPart, tabletIds, hints, tableSample, indexName, null, Optional.empty(),
                Optional.empty());
    }

    public UnboundRelation(RelationId id, List<String> nameParts, List<String> partNames, boolean isTempPart,
            List<Long> tabletIds, List<String> hints, Optional<TableSample> tableSample, Optional<String> indexName,
            TableScanParams scanParams, Optional<TableSnapshot> tableSnapshot) {
        this(id, nameParts, Optional.empty(), Optional.empty(),
                partNames, isTempPart, tabletIds, hints, tableSample, indexName, scanParams, Optional.empty(),
                tableSnapshot);
    }

    public UnboundRelation(RelationId id, List<String> nameParts, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<String> partNames, boolean isTempPart,
            List<Long> tabletIds, List<String> hints, Optional<TableSample> tableSample, Optional<String> indexName) {
        this(id, nameParts, groupExpression, logicalProperties, partNames,
                isTempPart, tabletIds, hints, tableSample, indexName, null, Optional.empty(), Optional.empty());
    }

    public UnboundRelation(RelationId id, List<String> nameParts, List<String> partNames, boolean isTempPart,
            List<Long> tabletIds, List<String> hints, Optional<TableSample> tableSample, Optional<String> indexName,
            TableScanParams scanParams, Optional<Pair<Integer, Integer>> indexInSqlString,
            Optional<TableSnapshot> tableSnapshot) {
        this(id, nameParts, Optional.empty(), Optional.empty(),
                partNames, isTempPart, tabletIds, hints, tableSample, indexName, scanParams, indexInSqlString,
                tableSnapshot);
    }

    /**
     * constructor of UnboundRelation
     */
    public UnboundRelation(RelationId id, List<String> nameParts, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<String> partNames, boolean isTempPart,
            List<Long> tabletIds, List<String> hints, Optional<TableSample> tableSample, Optional<String> indexName,
            TableScanParams scanParams, Optional<Pair<Integer, Integer>> indexInSqlString,
            Optional<TableSnapshot> tableSnapshot) {
        super(id, PlanType.LOGICAL_UNBOUND_RELATION, groupExpression, logicalProperties);
        this.nameParts = ImmutableList.copyOf(Objects.requireNonNull(nameParts, "nameParts should not null"));
        this.partNames = ImmutableList.copyOf(Objects.requireNonNull(partNames, "partNames should not null"));
        this.tabletIds = ImmutableList.copyOf(Objects.requireNonNull(tabletIds, "tabletIds should not null"));
        this.isTempPart = isTempPart;
        this.hints = ImmutableList.copyOf(Objects.requireNonNull(hints, "hints should not be null."));
        this.tableSample = tableSample;
        this.indexName = indexName;
        this.scanParams = scanParams;
        this.indexInSqlString = indexInSqlString;
        this.tableSnapshot = tableSnapshot;
    }

    public List<String> getNameParts() {
        return nameParts;
    }

    public String getTableName() {
        return nameParts.stream().map(Utils::quoteIfNeeded)
                .reduce((left, right) -> left + "." + right).orElse("");
    }

    @Override
    public LogicalProperties computeLogicalProperties() {
        return UnboundLogicalProperties.INSTANCE;
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UnboundRelation(relationId, nameParts,
                groupExpression, Optional.of(getLogicalProperties()),
                partNames, isTempPart, tabletIds, hints, tableSample, indexName, null, indexInSqlString, tableSnapshot);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new UnboundRelation(relationId, nameParts, groupExpression, logicalProperties, partNames,
                isTempPart, tabletIds, hints, tableSample, indexName, null, indexInSqlString, tableSnapshot);
    }

    public UnboundRelation withIndexInSql(Pair<Integer, Integer> index) {
        return new UnboundRelation(relationId, nameParts, groupExpression, Optional.of(getLogicalProperties()),
                partNames, isTempPart, tabletIds, hints, tableSample, indexName, null,
                Optional.of(index), tableSnapshot);
    }

    @Override
    public UnboundRelation withRelationId(RelationId relationId) {
        throw new UnboundException("should not call UnboundRelation's withRelationId method");
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public String toString() {
        List<Object> args = Lists.newArrayList(
                "id", relationId,
                "nameParts", StringUtils.join(nameParts, ".")
        );
        if (CollectionUtils.isNotEmpty(hints)) {
            args.add("hints");
            args.add(StringUtils.join(hints, ", "));
        }
        return Utils.toSqlString("UnboundRelation", args.toArray());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundRelation(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " don't support getExpression()");
    }

    public List<String> getPartNames() {
        return partNames;
    }

    public boolean isTempPart() {
        return isTempPart;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public Optional<String> getIndexName() {
        return indexName;
    }

    public List<String> getHints() {
        return hints;
    }

    public Optional<TableSample> getTableSample() {
        return tableSample;
    }

    public TableScanParams getScanParams() {
        return scanParams;
    }

    public Optional<Pair<Integer, Integer>> getIndexInSqlString() {
        return indexInSqlString;
    }

    public Optional<TableSnapshot> getTableSnapshot() {
        return tableSnapshot;
    }
}
