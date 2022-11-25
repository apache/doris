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
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.TVFRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.Optional;

/** LogicalTableValuedFunctionRelation */
public class LogicalTVFRelation extends LogicalRelation implements TVFRelation {
    private final TableValuedFunction function;

    public LogicalTVFRelation(RelationId id, TableValuedFunction function) {
        super(id, PlanType.LOGICAL_TVF_RELATION,
                Objects.requireNonNull(function, "table valued function can not be null").getTable(),
                ImmutableList.of());
        this.function = function;
    }

    public LogicalTVFRelation(RelationId id, TableValuedFunction function, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_TVF_RELATION,
                Objects.requireNonNull(function, "table valued function can not be null").getTable(),
                ImmutableList.of(), groupExpression, logicalProperties, ImmutableList.of());
        this.function = function;
    }

    @Override
    public LogicalTVFRelation withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalTVFRelation(id, function, groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public LogicalTVFRelation withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalTVFRelation(id, function, Optional.empty(), logicalProperties);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalTVFRelation",
                "qualified", qualifiedName(),
                "output", getOutput(),
                "function", function.toSql()
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalTVFRelation(this, context);
    }

    @Override
    public TableValuedFunction getFunction() {
        return function;
    }
}
