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
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * use for defer materialize top n
 */
public class PhysicalDeferMaterializeResultSink<CHILD_TYPE extends Plan>
        extends PhysicalSink<CHILD_TYPE> implements Sink {

    private final PhysicalResultSink<? extends Plan> physicalResultSink;
    private final OlapTable olapTable;
    private final long selectedIndexId;

    public PhysicalDeferMaterializeResultSink(PhysicalResultSink<? extends Plan> physicalResultSink,
            OlapTable olapTable, long selectedIndexId,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        this(physicalResultSink, olapTable, selectedIndexId,
                groupExpression, logicalProperties, PhysicalProperties.GATHER, null, child);
    }

    public PhysicalDeferMaterializeResultSink(PhysicalResultSink<? extends Plan> physicalResultSink,
            OlapTable olapTable, long selectedIndexId,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            @Nullable PhysicalProperties physicalProperties, Statistics statistics,
            CHILD_TYPE child) {
        super(physicalResultSink.getType(), physicalResultSink.outputExprs,
                groupExpression, logicalProperties, physicalProperties, statistics, child);
        this.physicalResultSink = physicalResultSink;
        this.olapTable = olapTable;
        this.selectedIndexId = selectedIndexId;
    }

    public PhysicalResultSink<? extends Plan> getPhysicalResultSink() {
        return physicalResultSink;
    }

    public OlapTable getOlapTable() {
        return olapTable;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalDeferMaterializeResultSink's children size must be 1, but real is %s", children.size());
        return new PhysicalDeferMaterializeResultSink<>(
                physicalResultSink.withChildren(ImmutableList.of(children.get(0))),
                olapTable, selectedIndexId, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalDeferMaterializeResultSink(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return physicalResultSink.getExpressions();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalDeferMaterializeResultSink<>(physicalResultSink, olapTable, selectedIndexId,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalDeferMaterializeResultSink's children size must be 1, but real is %s", children.size());
        return new PhysicalDeferMaterializeResultSink<>(
                physicalResultSink.withChildren(ImmutableList.of(children.get(0))),
                olapTable, selectedIndexId, groupExpression, logicalProperties.get(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalDeferMaterializeResultSink<>(physicalResultSink, olapTable, selectedIndexId,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public PhysicalDeferMaterializeResultSink<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalDeferMaterializeResultSink<>(physicalResultSink, olapTable, selectedIndexId,
                groupExpression, null, physicalProperties, statistics, child());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalDeferMaterializeResultSink<?> that = (PhysicalDeferMaterializeResultSink<?>) o;
        return selectedIndexId == that.selectedIndexId && physicalResultSink.equals(that.physicalResultSink)
                && olapTable.equals(that.olapTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalResultSink, olapTable, selectedIndexId);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalDeferMaterializeResultSink[" + id.asInt() + "]",
                "physicalResultSink", physicalResultSink,
                "olapTable", olapTable,
                "selectedIndexId", selectedIndexId
        );
    }
}
