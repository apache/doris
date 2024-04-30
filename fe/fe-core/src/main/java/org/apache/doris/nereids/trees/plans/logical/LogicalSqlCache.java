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

import org.apache.doris.analysis.Expr;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.TreeStringPlan;
import org.apache.doris.nereids.trees.plans.algebra.SqlCache;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** LogicalSqlCache */
public class LogicalSqlCache extends LogicalLeaf implements SqlCache, TreeStringPlan, BlockFuncDepsPropagation {
    private final TUniqueId queryId;
    private final List<String> columnLabels;
    private final List<Expr> resultExprs;
    private final Optional<ResultSet> resultSetInFe;
    private final List<InternalService.PCacheValue> cacheValues;
    private final String backendAddress;
    private final String planBody;

    /** LogicalSqlCache */
    public LogicalSqlCache(TUniqueId queryId,
            List<String> columnLabels, List<Expr> resultExprs,
            Optional<ResultSet> resultSetInFe, List<InternalService.PCacheValue> cacheValues,
            String backendAddress, String planBody) {
        super(PlanType.LOGICAL_SQL_CACHE, Optional.empty(), Optional.empty());
        this.queryId = Objects.requireNonNull(queryId, "queryId can not be null");
        this.columnLabels = Objects.requireNonNull(columnLabels, "columnLabels can not be null");
        this.resultExprs = Objects.requireNonNull(resultExprs, "resultExprs can not be null");
        this.resultSetInFe = Objects.requireNonNull(resultSetInFe, "resultSetInFe can not be null");
        this.cacheValues = Objects.requireNonNull(cacheValues, "cacheValues can not be null");
        this.backendAddress = Objects.requireNonNull(backendAddress, "backendAddress can not be null");
        this.planBody = Objects.requireNonNull(planBody, "planBody can not be null");
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public Optional<ResultSet> getResultSetInFe() {
        return resultSetInFe;
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

    public List<Expr> getResultExprs() {
        return resultExprs;
    }

    public String getPlanBody() {
        return planBody;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalSqlCache[" + id.asInt() + "]",
                "queryId", DebugUtil.printId(queryId)
        );
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSqlCache(this, context);
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
    public String getChildrenTreeString() {
        return planBody;
    }
}
