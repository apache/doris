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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** PhysicalStorageLayerAggregate */
public class PhysicalStorageLayerAggregate extends PhysicalCatalogRelation {

    private final PhysicalCatalogRelation relation;
    private final PushDownAggOp aggOp;
    private final List<ExprId> countArgumentExprIds;

    public PhysicalStorageLayerAggregate(PhysicalCatalogRelation relation, PushDownAggOp aggOp) {
        this(relation, aggOp, ImmutableList.of());
    }

    public PhysicalStorageLayerAggregate(PhysicalCatalogRelation relation, PushDownAggOp aggOp,
            List<ExprId> countArgumentExprIds) {
        super(relation.getRelationId(), relation.getType(), relation.getTable(), relation.getQualifier(),
                Optional.empty(), relation.getLogicalProperties(), ImmutableList.of());
        this.relation = Objects.requireNonNull(relation, "relation cannot be null");
        this.aggOp = Objects.requireNonNull(aggOp, "aggOp cannot be null");
        this.countArgumentExprIds = ImmutableList.copyOf(countArgumentExprIds);
    }

    public PhysicalStorageLayerAggregate(PhysicalCatalogRelation relation, PushDownAggOp aggOp,
            List<ExprId> countArgumentExprIds,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics) {
        super(relation.getRelationId(), relation.getType(), relation.getTable(), relation.getQualifier(),
                groupExpression, logicalProperties, physicalProperties, statistics, ImmutableList.of());
        this.relation = Objects.requireNonNull(relation, "relation cannot be null");
        this.aggOp = Objects.requireNonNull(aggOp, "aggOp cannot be null");
        this.countArgumentExprIds = ImmutableList.copyOf(countArgumentExprIds);
    }

    public PhysicalRelation getRelation() {
        return relation;
    }

    public PushDownAggOp getAggOp() {
        return aggOp;
    }

    public List<ExprId> getCountArgumentExprIds() {
        return countArgumentExprIds;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalStorageLayerAggregate(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalStorageLayerAggregate[" + relationId.asInt() + "]" + getGroupIdWithPrefix(),
                "pushDownAggOp", aggOp,
                "relation", relation,
                "stats", statistics
        );
    }

    public PhysicalStorageLayerAggregate withPhysicalOlapScan(PhysicalOlapScan physicalOlapScan) {
        // branch-4.1 predates copyWithSameId; its plan-copy contract creates a fresh node ID.
        return new PhysicalStorageLayerAggregate(physicalOlapScan, aggOp, countArgumentExprIds);
    }

    @Override
    public PhysicalStorageLayerAggregate withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalStorageLayerAggregate(relation, aggOp, countArgumentExprIds,
                groupExpression, getLogicalProperties(), physicalProperties, statistics);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalStorageLayerAggregate(relation, aggOp, countArgumentExprIds,
                groupExpression, logicalProperties.get(), physicalProperties, statistics);
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalStorageLayerAggregate(
                (PhysicalCatalogRelation) relation.withPhysicalPropertiesAndStats(null, statistics),
                aggOp, countArgumentExprIds, groupExpression,
                getLogicalProperties(), physicalProperties, statistics);
    }

    /** PushAggOp */
    public enum PushDownAggOp {
        COUNT, MIN_MAX, MIX, COUNT_ON_MATCH;

        /** supportedFunctions */
        public static Map<Class<? extends AggregateFunction>, PushDownAggOp> supportedFunctions() {
            return ImmutableMap.<Class<? extends AggregateFunction>, PushDownAggOp>builder()
                    .put(Count.class, PushDownAggOp.COUNT)
                    .put(Min.class, PushDownAggOp.MIN_MAX)
                    .put(Max.class, PushDownAggOp.MIN_MAX)
                    .build();
        }
    }
}
