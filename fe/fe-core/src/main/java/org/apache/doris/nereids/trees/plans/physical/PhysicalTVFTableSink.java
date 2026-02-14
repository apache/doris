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

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical TVF table sink for INSERT INTO tvf_name(properties) SELECT ...
 */
public class PhysicalTVFTableSink<CHILD_TYPE extends Plan> extends PhysicalSink<CHILD_TYPE>
        implements Sink {

    private final String tvfName;
    private final Map<String, String> properties;
    private final List<Column> cols;

    public PhysicalTVFTableSink(String tvfName, Map<String, String> properties,
            List<Column> cols, List<NamedExpression> outputExprs,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        this(tvfName, properties, cols, outputExprs, groupExpression,
                logicalProperties, PhysicalProperties.GATHER, null, child);
    }

    public PhysicalTVFTableSink(String tvfName, Map<String, String> properties,
            List<Column> cols, List<NamedExpression> outputExprs,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            @Nullable PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_TVF_TABLE_SINK, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.tvfName = Objects.requireNonNull(tvfName, "tvfName should not be null");
        this.properties = Objects.requireNonNull(properties, "properties should not be null");
        this.cols = Objects.requireNonNull(cols, "cols should not be null");
    }

    public String getTvfName() {
        return tvfName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public List<Column> getCols() {
        return cols;
    }

    @Override
    public PhysicalTVFTableSink<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalTVFTableSink's children size must be 1, but real is %s", children.size());
        return new PhysicalTVFTableSink<>(tvfName, properties, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalTVFTableSink(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return outputExprs;
    }

    @Override
    public PhysicalTVFTableSink<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalTVFTableSink<>(tvfName, properties, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public PhysicalTVFTableSink<Plan> withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalTVFTableSink's children size must be 1, but real is %s", children.size());
        return new PhysicalTVFTableSink<>(tvfName, properties, cols, outputExprs,
                groupExpression, logicalProperties.get(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalTVFTableSink<Plan> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalTVFTableSink<>(tvfName, properties, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalTVFTableSink<?> that = (PhysicalTVFTableSink<?>) o;
        return tvfName.equals(that.tvfName)
                && properties.equals(that.properties)
                && cols.equals(that.cols)
                && outputExprs.equals(that.outputExprs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tvfName, properties, cols, outputExprs);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalTVFTableSink[" + id.asInt() + "]",
                "tvfName", tvfName,
                "outputExprs", outputExprs);
    }

    @Override
    public PhysicalTVFTableSink<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalTVFTableSink<>(tvfName, properties, cols, outputExprs,
                groupExpression, null, physicalProperties, statistics, child());
    }
}
