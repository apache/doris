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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unbound TVF table sink for INSERT INTO tvf_name(properties) SELECT ...
 */
public class UnboundTVFTableSink<CHILD_TYPE extends Plan> extends UnboundLogicalSink<CHILD_TYPE>
        implements Unbound, Sink, BlockFuncDepsPropagation {

    private final String tvfName;
    private final Map<String, String> properties;

    /**
     * For insert into tvf
     */
    public UnboundTVFTableSink(String tvfName, Map<String, String> properties,
            DMLCommandType dmlCommandType, CHILD_TYPE child) {
        super(ImmutableList.of(tvfName),
                PlanType.LOGICAL_UNBOUND_TVF_TABLE_SINK,
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                dmlCommandType,
                child);
        this.tvfName = tvfName;
        this.properties = properties;
    }

    private UnboundTVFTableSink(String tvfName, Map<String, String> properties,
            DMLCommandType dmlCommandType,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(ImmutableList.of(tvfName),
                PlanType.LOGICAL_UNBOUND_TVF_TABLE_SINK,
                ImmutableList.of(),
                groupExpression,
                logicalProperties,
                ImmutableList.of(),
                dmlCommandType,
                child);
        this.tvfName = tvfName;
        this.properties = properties;
    }

    public String getTvfName() {
        return tvfName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "UnboundTVFTableSink only accepts one child");
        return new UnboundTVFTableSink<>(tvfName, properties, getDMLCommandType(),
                groupExpression, Optional.empty(), children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundTVFTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UnboundTVFTableSink<>(tvfName, properties, getDMLCommandType(),
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "UnboundTVFTableSink only accepts one child");
        return new UnboundTVFTableSink<>(tvfName, properties, getDMLCommandType(),
                groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public UnboundTVFTableSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        throw new UnboundException("could not call withOutputExprs on UnboundTVFTableSink");
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public String toString() {
        return Utils.toSqlString("UnboundTVFTableSink[" + id.asInt() + "]",
                "tvfName", tvfName,
                "properties", properties);
    }
}
