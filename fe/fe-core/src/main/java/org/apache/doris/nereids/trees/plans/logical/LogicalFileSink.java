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
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * logicalFileSink for select into outfile
 */
public class LogicalFileSink<CHILD_TYPE extends Plan> extends LogicalSink<CHILD_TYPE>
        implements Sink, PropagateFuncDeps {

    private final String filePath;
    private final String format;
    private final Map<String, String> properties;

    public LogicalFileSink(String filePath, String format,
            Map<String, String> properties, List<NamedExpression> outputExprs, CHILD_TYPE child) {
        this(filePath, format, properties, outputExprs, Optional.empty(), Optional.empty(), child);
    }

    public LogicalFileSink(String filePath, String format, Map<String, String> properties,
            List<NamedExpression> outputExprs,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_FILE_SINK, outputExprs, groupExpression, logicalProperties, child);
        this.filePath = Objects.requireNonNull(filePath);
        this.format = Objects.requireNonNull(format);
        this.properties = ImmutableMap.copyOf(Objects.requireNonNull(properties));
    }

    public LogicalFileSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalFileSink<>(filePath, format, properties, outputExprs, child());
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalFileSink<>(filePath, format, properties, outputExprs, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFileSink(this, context);
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
        LogicalFileSink<?> that = (LogicalFileSink<?>) o;
        return Objects.equals(filePath, that.filePath) && Objects.equals(format, that.format)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filePath, format, properties);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalFileSink<>(filePath, format, properties, outputExprs,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalFileSink<>(filePath, format, properties, outputExprs,
                groupExpression, logicalProperties, children.get(0));
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
}
