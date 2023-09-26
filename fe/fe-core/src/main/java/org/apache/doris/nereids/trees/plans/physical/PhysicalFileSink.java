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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * physicalFileSink for select into outfile
 */
public class PhysicalFileSink<CHILD_TYPE extends Plan> extends PhysicalSink<CHILD_TYPE> implements Sink {

    private final String filePath;
    private final String format;
    private final Map<String, String> properties;

    public PhysicalFileSink(List<NamedExpression> outputExprs, String filePath, String format,
            Map<String, String> properties,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(outputExprs, filePath, format, properties, Optional.empty(), logicalProperties, child);
    }

    public PhysicalFileSink(List<NamedExpression> outputExprs, String filePath, String format,
            Map<String, String> properties,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        this(outputExprs, filePath, format, properties,
                groupExpression, logicalProperties, PhysicalProperties.GATHER, null, child);
    }

    public PhysicalFileSink(List<NamedExpression> outputExprs, String filePath, String format,
            Map<String, String> properties,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_FILE_SINK, outputExprs,
                groupExpression, logicalProperties, physicalProperties, statistics, child);
        this.filePath = filePath;
        this.format = format;
        this.properties = properties;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getFormat() {
        return format;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "PhysicalFileSink only accepts one child");
        return new PhysicalFileSink<>(outputExprs, filePath, format, properties,
                getLogicalProperties(), children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalFileSink(this, context);
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
        PhysicalFileSink<?> that = (PhysicalFileSink<?>) o;
        return Objects.equals(filePath, that.filePath)
                && Objects.equals(format, that.format)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, format, properties);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalFileSink[" + id.asInt() + "]",
                "outputExprs", outputExprs,
                "filePath", filePath,
                "format", format,
                "properties", properties);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalFileSink<>(outputExprs, filePath, format, properties,
                groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalFileSink<>(outputExprs, filePath, format, properties,
                groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalFileSink<>(outputExprs, filePath, format, properties, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public PhysicalFileSink<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalFileSink<>(outputExprs, filePath, format, properties, groupExpression, null,
                physicalProperties, statistics, child());
    }
}
