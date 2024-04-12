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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Logical Check Policy
 */
public class LogicalCheckPolicy<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements PropagateFuncDeps {

    public LogicalCheckPolicy(CHILD_TYPE child) {
        super(PlanType.LOGICAL_CHECK_POLICY, child);
    }

    public LogicalCheckPolicy(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_CHECK_POLICY, groupExpression, logicalProperties, child);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCheckPolicy(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalCheckPolicy");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalCheckPolicy that = (LogicalCheckPolicy) o;
        return child().equals(that.child());
    }

    @Override
    public int hashCode() {
        return child().hashCode();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCheckPolicy<>(groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalCheckPolicy<>(groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalCheckPolicy<>(children.get(0));
    }

    /**
     * find related policy for logicalRelation.
     *
     * @param logicalRelation include tableName and dbName
     * @param cascadesContext include information about user and policy
     */
    public RelatedPolicy findPolicy(LogicalRelation logicalRelation, CascadesContext cascadesContext) {
        if (!(logicalRelation instanceof CatalogRelation)) {
            return RelatedPolicy.NO_POLICY;
        }

        ConnectContext connectContext = cascadesContext.getConnectContext();
        AccessControllerManager accessManager = connectContext.getEnv().getAccessManager();
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        if (currentUserIdentity.isRootUser() || currentUserIdentity.isAdminUser()) {
            return RelatedPolicy.NO_POLICY;
        }

        CatalogRelation catalogRelation = (CatalogRelation) logicalRelation;
        String ctlName = catalogRelation.getDatabase().getCatalog().getName();
        String dbName = catalogRelation.getDatabase().getFullName();
        String tableName = catalogRelation.getTable().getName();

        NereidsParser nereidsParser = new NereidsParser();
        ImmutableList.Builder<NamedExpression> dataMasks
                = ImmutableList.builderWithExpectedSize(logicalRelation.getOutput().size());

        StatementContext statementContext = cascadesContext.getStatementContext();
        Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
        boolean hasDataMask = false;
        for (Slot slot : logicalRelation.getOutput()) {
            Optional<DataMaskPolicy> dataMaskPolicy = accessManager.evalDataMaskPolicy(
                    currentUserIdentity, ctlName, dbName, tableName, slot.getName());
            if (dataMaskPolicy.isPresent()) {
                Expression unboundExpr = nereidsParser.parseExpression(dataMaskPolicy.get().getMaskTypeDef());
                Expression childOfAlias
                        = unboundExpr instanceof UnboundAlias ? unboundExpr.child(0) : unboundExpr;
                Alias alias = new Alias(
                        StatementScopeIdGenerator.newExprId(),
                        ImmutableList.of(childOfAlias),
                        slot.getName(), slot.getQualifier(), false
                );
                dataMasks.add(alias);
                hasDataMask = true;
            } else {
                dataMasks.add(slot);
            }
            if (sqlCacheContext.isPresent()) {
                sqlCacheContext.get().addDataMaskPolicy(ctlName, dbName, tableName, slot.getName(), dataMaskPolicy);
            }
        }

        List<? extends RowFilterPolicy> rowPolicies = accessManager.evalRowFilterPolicies(
                currentUserIdentity, ctlName, dbName, tableName);
        if (sqlCacheContext.isPresent()) {
            sqlCacheContext.get().setRowFilterPolicy(ctlName, dbName, tableName, rowPolicies);
        }

        return new RelatedPolicy(
                Optional.ofNullable(CollectionUtils.isEmpty(rowPolicies) ? null : mergeRowPolicy(rowPolicies)),
                hasDataMask ? Optional.of(dataMasks.build()) : Optional.empty()
        );
    }

    private Expression mergeRowPolicy(List<? extends RowFilterPolicy> policies) {
        List<Expression> orList = new ArrayList<>();
        List<Expression> andList = new ArrayList<>();
        for (RowFilterPolicy policy : policies) {
            Expression wherePredicate = null;
            try {
                wherePredicate = policy.getFilterExpression();
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new AnalysisException(e.getMessage(), e);
            }
            switch (policy.getFilterType()) {
                case PERMISSIVE:
                    orList.add(wherePredicate);
                    break;
                case RESTRICTIVE:
                    andList.add(wherePredicate);
                    break;
                default:
                    throw new IllegalStateException("Invalid operator");
            }
        }
        if (!andList.isEmpty() && !orList.isEmpty()) {
            return new And(ExpressionUtils.and(andList), ExpressionUtils.or(orList));
        } else if (andList.isEmpty()) {
            return ExpressionUtils.or(orList);
        } else if (orList.isEmpty()) {
            return ExpressionUtils.and(andList);
        } else {
            return null;
        }
    }

    /** RelatedPolicy */
    public static class RelatedPolicy {
        public static final RelatedPolicy NO_POLICY = new RelatedPolicy(Optional.empty(), Optional.empty());

        public final Optional<Expression> rowPolicyFilter;
        public final Optional<List<NamedExpression>> dataMaskProjects;

        public RelatedPolicy(Optional<Expression> rowPolicyFilter, Optional<List<NamedExpression>> dataMaskProjects) {
            this.rowPolicyFilter = rowPolicyFilter;
            this.dataMaskProjects = dataMaskProjects;
        }
    }
}
