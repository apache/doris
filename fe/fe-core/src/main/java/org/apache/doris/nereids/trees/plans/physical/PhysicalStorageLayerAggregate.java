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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
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
public class PhysicalStorageLayerAggregate extends PhysicalRelation implements OlapScan {
    private final PhysicalOlapScan olapScan;
    private final PushDownAggOp aggOp;

    public PhysicalStorageLayerAggregate(PhysicalOlapScan olapScan, PushDownAggOp aggOp) {
        super(olapScan.getId(), olapScan.getType(), olapScan.getQualifier(),
                Optional.empty(), olapScan.getLogicalProperties());
        this.olapScan = Objects.requireNonNull(olapScan, "olapScan cannot be null");
        this.aggOp = Objects.requireNonNull(aggOp, "aggOp cannot be null");
    }

    public PhysicalStorageLayerAggregate(PhysicalOlapScan olapScan, PushDownAggOp aggOp,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult) {
        super(olapScan.getId(), olapScan.getType(), olapScan.getQualifier(), groupExpression,
                logicalProperties, physicalProperties, statsDeriveResult);
        this.olapScan = Objects.requireNonNull(olapScan, "olapScan cannot be null");
        this.aggOp = Objects.requireNonNull(aggOp, "aggOp cannot be null");
    }

    public PhysicalOlapScan getOlapScan() {
        return olapScan;
    }

    public PushDownAggOp getAggOp() {
        return aggOp;
    }

    @Override
    public OlapTable getTable() {
        return olapScan.getTable();
    }

    @Override
    public long getSelectedIndexId() {
        return olapScan.getSelectedIndexId();
    }

    @Override
    public List<Long> getSelectedPartitionIds() {
        return olapScan.getSelectedPartitionIds();
    }

    @Override
    public List<Long> getSelectedTabletIds() {
        return olapScan.getSelectedTabletIds();
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
        return Objects.equals(olapScan, that.olapScan) && aggOp == that.aggOp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), olapScan, aggOp);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalStorageLayerAggregate",
                "pushDownAggOp", aggOp,
                "table", olapScan,
                "stats", statsDeriveResult
        );
    }

    public PhysicalStorageLayerAggregate withPhysicalOlapScan(PhysicalOlapScan physicalOlapScan) {
        return new PhysicalStorageLayerAggregate(olapScan, aggOp);
    }

    @Override
    public PhysicalStorageLayerAggregate withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalStorageLayerAggregate(olapScan, aggOp, groupExpression, getLogicalProperties(),
                physicalProperties, statsDeriveResult);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalStorageLayerAggregate(olapScan, aggOp, Optional.empty(),
                logicalProperties.get(), physicalProperties, statsDeriveResult);
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        return new PhysicalStorageLayerAggregate(olapScan, aggOp, Optional.empty(),
                getLogicalProperties(), physicalProperties, statsDeriveResult);
    }

    /** PushAggOp */
    public enum PushDownAggOp {
        COUNT, MIN_MAX, MIX;

        public static Map<String, PushDownAggOp> supportedFunctions() {
            return ImmutableMap.<String, PushDownAggOp>builder()
                    .put("count", PushDownAggOp.COUNT)
                    .put("min", PushDownAggOp.MIN_MAX)
                    .put("max", PushDownAggOp.MIN_MAX)
                    .build();
        }
    }
}
