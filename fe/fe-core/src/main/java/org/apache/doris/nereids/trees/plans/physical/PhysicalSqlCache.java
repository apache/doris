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

import org.apache.doris.analysis.Expr;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.FieldInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.ComputeResultSet;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.TreeStringPlan;
import org.apache.doris.nereids.trees.plans.algebra.SqlCache;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PCacheValue;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** PhysicalSqlCache */
public class PhysicalSqlCache extends PhysicalLeaf implements SqlCache, TreeStringPlan, ComputeResultSet {
    private final TUniqueId queryId;
    private final List<String> columnLabels;
    private final List<FieldInfo> fieldInfos;
    private final List<Expr> resultExprs;
    private final Optional<ResultSet> resultSet;
    private final List<InternalService.PCacheValue> cacheValues;
    private final String backendAddress;
    private final String planBody;

    /** PhysicalSqlCache */
    public PhysicalSqlCache(TUniqueId queryId,
            List<String> columnLabels, List<FieldInfo> fieldInfos, List<Expr> resultExprs,
            Optional<ResultSet> resultSet, List<InternalService.PCacheValue> cacheValues,
            String backendAddress, String planBody) {
        super(PlanType.PHYSICAL_SQL_CACHE, Optional.empty(),
                new LogicalProperties(() -> ImmutableList.of(), () -> DataTrait.EMPTY_TRAIT));
        this.queryId = Objects.requireNonNull(queryId, "queryId can not be null");
        this.columnLabels = Objects.requireNonNull(columnLabels, "colNames can not be null");
        this.fieldInfos = Objects.requireNonNull(fieldInfos, "fieldInfos can not be null");
        this.resultExprs = Objects.requireNonNull(resultExprs, "resultExprs can not be null");
        this.resultSet = Objects.requireNonNull(resultSet, "resultSet can not be null");
        this.cacheValues = Objects.requireNonNull(cacheValues, "cacheValues can not be null");
        this.backendAddress = Objects.requireNonNull(backendAddress, "backendAddress can not be null");
        this.planBody = Objects.requireNonNull(planBody, "planBody can not be null");
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public Optional<ResultSet> getResultSet() {
        return resultSet;
    }

    public List<InternalService.PCacheValue> getCacheValues() {
        return cacheValues;
    }

    public String getBackendAddress() {
        return backendAddress;
    }

    public List<String> getColumnLabels() {
        return columnLabels;
    }

    public List<FieldInfo> getFieldInfos() {
        return fieldInfos;
    }

    public List<Expr> getResultExprs() {
        return resultExprs;
    }

    public String getPlanBody() {
        return planBody;
    }

    @Override
    public String toString() {
        long rowCount = 0;
        if (resultSet.isPresent()) {
            rowCount = resultSet.get().getResultRows().size();
        } else {
            for (PCacheValue cacheValue : cacheValues) {
                rowCount += cacheValue.getRowsCount();
            }
        }
        return Utils.toSqlString("PhysicalSqlCache[" + id.asInt() + "]",
                "queryId", DebugUtil.printId(queryId),
                "backend", backendAddress,
                "rowCount", rowCount
        );
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalSqlCache(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getChildrenTreeString() {
        return planBody;
    }

    @Override
    public Optional<ResultSet> computeResultInFe(
            CascadesContext cascadesContext, Optional<SqlCacheContext> sqlCacheContext, List<Slot> outputSlots) {
        return resultSet;
    }
}
