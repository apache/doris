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

import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.TVFRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.Optional;

/** PhysicalTableValuedFunctionRelation */
public class PhysicalTVFRelation extends PhysicalRelation implements TVFRelation {
    private final TableValuedFunction function;

    public PhysicalTVFRelation(RelationId id, TableValuedFunction function, LogicalProperties logicalProperties) {
        super(id, PlanType.PHYSICAL_TVF_RELATION,
                ImmutableList.of(), Optional.empty(), logicalProperties);
        this.function = Objects.requireNonNull(function, "function can not be null");
    }

    public PhysicalTVFRelation(RelationId id, TableValuedFunction function, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        super(id, PlanType.PHYSICAL_TVF_RELATION, ImmutableList.of(), groupExpression, logicalProperties,
                physicalProperties, statsDeriveResult);
        this.function = Objects.requireNonNull(function, "function can not be null");
    }

    @Override
    public PhysicalTVFRelation withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalTVFRelation(id, function, groupExpression, getLogicalProperties(),
                physicalProperties, statsDeriveResult);
    }

    @Override
    public PhysicalTVFRelation withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalTVFRelation(id, function, Optional.empty(),
                logicalProperties.get(), physicalProperties, statsDeriveResult);
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        return new PhysicalTVFRelation(id, function, Optional.empty(),
                getLogicalProperties(), physicalProperties, statsDeriveResult);
    }

    @Override
    public FunctionGenTable getTable() {
        return function.getTable();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalTVFRelation",
                "qualified", Utils.qualifiedName(qualifier, getTable().getName()),
                "output", getOutput(),
                "function", function.toSql()
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalTVFRelation(this, context);
    }

    @Override
    public TableValuedFunction getFunction() {
        return function;
    }
}
