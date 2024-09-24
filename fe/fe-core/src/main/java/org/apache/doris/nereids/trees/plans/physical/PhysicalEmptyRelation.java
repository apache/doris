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
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.ComputeResultSet;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.EmptyRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A physical relation that contains empty row.
 * e.g.
 * select * from tbl limit 0
 */
public class PhysicalEmptyRelation extends PhysicalRelation implements EmptyRelation, ComputeResultSet {

    private final List<? extends NamedExpression> projects;

    public PhysicalEmptyRelation(RelationId relationId, List<? extends NamedExpression> projects,
            LogicalProperties logicalProperties) {
        this(relationId, projects, Optional.empty(), logicalProperties, null, null);
    }

    public PhysicalEmptyRelation(RelationId relationId, List<? extends NamedExpression> projects,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics) {
        super(relationId, PlanType.PHYSICAL_EMPTY_RELATION, groupExpression,
                logicalProperties, physicalProperties, statistics);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalEmptyRelation(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalEmptyRelation(relationId, projects, groupExpression,
                logicalPropertiesSupplier.get(), physicalProperties, statistics);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalEmptyRelation(relationId, projects, groupExpression,
                logicalProperties.get(), physicalProperties, statistics);
    }

    @Override
    public List<Slot> computeOutput() {
        return projects.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalEmptyRelation",
                "projects", projects
        );
    }

    @Override
    public List<? extends NamedExpression> getProjects() {
        return projects;
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalEmptyRelation(relationId, projects, Optional.empty(),
                logicalPropertiesSupplier.get(), physicalProperties, statistics);
    }

    @Override
    public Optional<ResultSet> computeResultInFe(CascadesContext cascadesContext,
            Optional<SqlCacheContext> sqlCacheContext, List<Slot> outputSlots) {
        List<Column> columns = Lists.newArrayList();
        for (NamedExpression output : outputSlots) {
            columns.add(new Column(output.getName(), output.getDataType().toCatalogDataType()));
        }

        StatementContext statementContext = cascadesContext.getStatementContext();
        boolean enableSqlCache
                = CacheAnalyzer.canUseSqlCache(statementContext.getConnectContext().getSessionVariable());

        ResultSetMetaData metadata = new CommonResultSet.CommonResultSetMetaData(columns);
        ResultSet resultSet = new CommonResultSet(metadata, ImmutableList.of());
        if (sqlCacheContext.isPresent() && enableSqlCache) {
            sqlCacheContext.get().setResultSetInFe(resultSet);
            Env.getCurrentEnv().getSqlCacheManager().tryAddFeSqlCache(
                    statementContext.getConnectContext(),
                    statementContext.getOriginStatement().originStmt
            );
        }
        return Optional.of(resultSet);
    }
}
