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
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical TVF table sink for INSERT INTO tvf_name(properties) SELECT ...
 */
public class LogicalTVFTableSink<CHILD_TYPE extends Plan> extends LogicalSink<CHILD_TYPE>
        implements Sink, PropagateFuncDeps {

    private final String tvfName;
    private final Map<String, String> properties;
    private final List<Column> cols;

    public LogicalTVFTableSink(String tvfName, Map<String, String> properties,
            List<Column> cols, List<NamedExpression> outputExprs, CHILD_TYPE child) {
        super(PlanType.LOGICAL_TVF_TABLE_SINK, outputExprs, child);
        this.tvfName = Objects.requireNonNull(tvfName, "tvfName should not be null");
        this.properties = Objects.requireNonNull(properties, "properties should not be null");
        this.cols = Objects.requireNonNull(cols, "cols should not be null");
    }

    public LogicalTVFTableSink(String tvfName, Map<String, String> properties,
            List<Column> cols, List<NamedExpression> outputExprs,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_TVF_TABLE_SINK, outputExprs, groupExpression, logicalProperties, child);
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
    public LogicalTVFTableSink<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "LogicalTVFTableSink's children size must be 1, but real is %s", children.size());
        return new LogicalTVFTableSink<>(tvfName, properties, cols, outputExprs, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalTVFTableSink(this, context);
    }

    @Override
    public LogicalTVFTableSink<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalTVFTableSink<>(tvfName, properties, cols, outputExprs,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public LogicalTVFTableSink<Plan> withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalTVFTableSink only accepts one child");
        return new LogicalTVFTableSink<>(tvfName, properties, cols, outputExprs,
                groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public LogicalTVFTableSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalTVFTableSink<>(tvfName, properties, cols, outputExprs, child());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalTVFTableSink<?> that = (LogicalTVFTableSink<?>) o;
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
        return Utils.toSqlString("LogicalTVFTableSink[" + id.asInt() + "]",
                "tvfName", tvfName,
                "outputExprs", outputExprs);
    }
}
