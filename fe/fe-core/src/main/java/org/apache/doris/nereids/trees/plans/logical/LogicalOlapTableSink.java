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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import java.util.List;
import java.util.Optional;

/**
 * logical olap table sink for insert command
 */
public class LogicalOlapTableSink<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {
    private final OlapTable targetTable;
    private final List<Long> partitionIds;

    public LogicalOlapTableSink(CHILD_TYPE child, OlapTable targetTable, List<Long> partitionIds) {
        this(child, Optional.empty(), Optional.empty(), targetTable, partitionIds);
    }

    public LogicalOlapTableSink(CHILD_TYPE child, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, OlapTable targetTable, List<Long> partitionIds) {
        super(PlanType.LOGICAL_OLAP_TABLE_SINK, groupExpression, logicalProperties, child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return null;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return null;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return null;
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return null;
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return null;
    }

    @Override
    public List<Slot> computeOutput() {
        return null;
    }
}
