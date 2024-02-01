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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * use for defer materialize top n
 */
public class LogicalDeferMaterializeResultSink<CHILD_TYPE extends Plan>
        extends LogicalSink<CHILD_TYPE> implements Sink, PropagateFuncDeps {

    private final LogicalResultSink<? extends Plan> logicalResultSink;
    private final OlapTable olapTable;
    private final long selectedIndexId;

    public LogicalDeferMaterializeResultSink(LogicalResultSink<CHILD_TYPE> logicalResultSink,
            OlapTable olapTable, long selectedIndexId) {
        this(logicalResultSink, olapTable, selectedIndexId,
                Optional.empty(), Optional.empty(), logicalResultSink.child());
    }

    public LogicalDeferMaterializeResultSink(LogicalResultSink<? extends Plan> logicalResultSink,
            OlapTable olapTable, long selectedIndexId,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(logicalResultSink.getType(), logicalResultSink.getOutputExprs(),
                groupExpression, logicalProperties, child);
        this.logicalResultSink = logicalResultSink;
        this.olapTable = olapTable;
        this.selectedIndexId = selectedIndexId;
    }

    public LogicalResultSink<? extends Plan> getLogicalResultSink() {
        return logicalResultSink;
    }

    public OlapTable getOlapTable() {
        return olapTable;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    @Override
    public LogicalDeferMaterializeResultSink<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "LogicalDeferMaterializeResultSink only accepts one child");
        return new LogicalDeferMaterializeResultSink<>(
                logicalResultSink.withChildren(ImmutableList.of(children.get(0))),
                olapTable, selectedIndexId, Optional.empty(), Optional.empty(), children.get(0));
    }

    @Override
    public LogicalDeferMaterializeResultSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalDeferMaterializeResultSink<>(logicalResultSink, olapTable, selectedIndexId,
                Optional.empty(), Optional.empty(), child());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalDeferMaterializeResultSink(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return logicalResultSink.getExpressions();
    }

    @Override
    public LogicalDeferMaterializeResultSink<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalDeferMaterializeResultSink<>(logicalResultSink, olapTable, selectedIndexId,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public LogicalDeferMaterializeResultSink<Plan> withGroupExprLogicalPropChildren(
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "LogicalDeferMaterializeResultSink only accepts one child");
        return new LogicalDeferMaterializeResultSink<>(
                logicalResultSink.withChildren(ImmutableList.of(children.get(0))),
                olapTable, selectedIndexId, groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalDeferMaterializeResultSink<?> that = (LogicalDeferMaterializeResultSink<?>) o;
        return selectedIndexId == that.selectedIndexId && Objects.equals(logicalResultSink,
                that.logicalResultSink) && Objects.equals(olapTable, that.olapTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalResultSink, olapTable, selectedIndexId);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalDeferMaterializeResultSink[" + id.asInt() + "]",
                "logicalResultSink", logicalResultSink,
                "olapTable", olapTable,
                "selectedIndexId", selectedIndexId
        );
    }
}
