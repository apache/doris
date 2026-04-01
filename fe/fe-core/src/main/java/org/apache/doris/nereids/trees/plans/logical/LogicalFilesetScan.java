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

import org.apache.doris.catalog.FilesetTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Optional;

/**
 * Logical scan node for FilesetTable.
 */
public class LogicalFilesetScan extends LogicalCatalogRelation {

    public LogicalFilesetScan(RelationId id, FilesetTable table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty());
    }

    private LogicalFilesetScan(RelationId id, FilesetTable table, List<String> qualifier,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        this(id, table, qualifier, groupExpression, logicalProperties, "");
    }

    private LogicalFilesetScan(RelationId id, FilesetTable table, List<String> qualifier,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, String tableAlias) {
        super(id, PlanType.LOGICAL_FILESET_SCAN, table, qualifier, groupExpression, logicalProperties,
                tableAlias);
    }

    @Override
    public FilesetTable getTable() {
        return (FilesetTable) table;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFilesetScan(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFilesetScan(relationId, getTable(), qualifier, groupExpression,
                        Optional.of(getLogicalProperties())));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFilesetScan(relationId, getTable(), qualifier, groupExpression, logicalProperties));
    }

    @Override
    public LogicalFilesetScan withRelationId(RelationId relationId) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFilesetScan(relationId, getTable(), qualifier, Optional.empty(), Optional.empty()));
    }

    @Override
    public LogicalFilesetScan withTableAlias(String tableAlias) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFilesetScan(relationId, getTable(), qualifier,
                        Optional.empty(), Optional.of(getLogicalProperties()), tableAlias));
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalFilesetScan",
                "qualified", qualifier,
                "output", getOutput());
    }
}
