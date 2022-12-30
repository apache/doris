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

import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** PhysicalStorageLayerAggregate */
public class PhysicalStorageLayerAggregate extends PhysicalRelation {
    private final PhysicalRelation relation;
    private final PushDownAggOp aggOp;

    public PhysicalStorageLayerAggregate(PhysicalRelation relation, PushDownAggOp aggOp) {
        super(relation.getId(), relation.getType(), relation.getQualifier(),
                Optional.empty(), relation.getLogicalProperties());
        this.relation = Objects.requireNonNull(relation, "relation cannot be null");
        this.aggOp = Objects.requireNonNull(aggOp, "aggOp cannot be null");
    }

    public PhysicalStorageLayerAggregate(PhysicalRelation relation, PushDownAggOp aggOp,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult) {
        super(relation.getId(), relation.getType(), relation.getQualifier(), groupExpression,
                logicalProperties, physicalProperties, statsDeriveResult);
        this.relation = Objects.requireNonNull(relation, "relation cannot be null");
        this.aggOp = Objects.requireNonNull(aggOp, "aggOp cannot be null");
    }

    public PhysicalRelation getRelation() {
        return relation;
    }

    public PushDownAggOp getAggOp() {
        return aggOp;
    }

    @Override
    public Table getTable() {
        return (Table) relation.getTable();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalStorageLayerAggregate(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalStorageLayerAggregate that = (PhysicalStorageLayerAggregate) o;
        return Objects.equals(relation, that.relation) && aggOp == that.aggOp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), relation, aggOp);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalStorageLayerAggregate",
                "pushDownAggOp", aggOp,
                "relation", relation,
                "stats", statsDeriveResult
        );
    }

    public PhysicalStorageLayerAggregate withPhysicalOlapScan(PhysicalOlapScan physicalOlapScan) {
        return new PhysicalStorageLayerAggregate(relation, aggOp);
    }

    @Override
    public PhysicalStorageLayerAggregate withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalStorageLayerAggregate(relation, aggOp, groupExpression, getLogicalProperties(),
                physicalProperties, statsDeriveResult);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalStorageLayerAggregate(relation, aggOp, Optional.empty(),
                logicalProperties.get(), physicalProperties, statsDeriveResult);
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        return new PhysicalStorageLayerAggregate(relation, aggOp, Optional.empty(),
                getLogicalProperties(), physicalProperties, statsDeriveResult);
    }

    /** PushAggOp */
    public enum PushDownAggOp {
        COUNT, MIN_MAX, MIX;

        public static Map<Class, PushDownAggOp> supportedFunctions() {
            return ImmutableMap.<Class, PushDownAggOp>builder()
                    .put(Count.class, PushDownAggOp.COUNT)
                    .put(Min.class, PushDownAggOp.MIN_MAX)
                    .put(Max.class, PushDownAggOp.MIN_MAX)
                    .build();
        }
    }
}
