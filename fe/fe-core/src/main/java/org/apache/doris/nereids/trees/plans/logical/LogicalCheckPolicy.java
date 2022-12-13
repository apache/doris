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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.policy.PolicyMgr;
import org.apache.doris.policy.RowPolicy;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Logical Check Policy
 */
public class LogicalCheckPolicy<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {

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
        return Utils.toSqlString("LogicalCheckPolicy",
            "child", child()
        );
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
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCheckPolicy<>(Optional.empty(), logicalProperties, child());
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalCheckPolicy<>(children.get(0));
    }

    /**
     * get wherePredicate of policy for logicalRelation.
     *
     * @param logicalRelation include tableName and dbName
     * @param connectContext include information about user and policy
     */
    public Optional<Expression> getFilter(LogicalRelation logicalRelation, ConnectContext connectContext) {
        if (!(logicalRelation instanceof CatalogRelation)) {
            return Optional.empty();
        }

        PolicyMgr policyMgr = connectContext.getEnv().getPolicyMgr();
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        String user = connectContext.getQualifiedUser();
        if (currentUserIdentity.isRootUser() || currentUserIdentity.isAdminUser()) {
            return Optional.empty();
        }
        if (!policyMgr.existPolicy(user)) {
            return Optional.empty();
        }

        CatalogRelation catalogRelation = (CatalogRelation) logicalRelation;
        long dbId = catalogRelation.getDatabase().getId();
        long tableId = catalogRelation.getTable().getId();
        List<RowPolicy> policies = policyMgr.getMatchRowPolicy(dbId, tableId, currentUserIdentity);
        if (policies.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(mergeRowPolicy(policies));
    }

    private Expression mergeRowPolicy(List<RowPolicy> policies) {
        List<Expression> orList = new ArrayList<>();
        List<Expression> andList = new ArrayList<>();
        for (RowPolicy policy : policies) {
            String sql = policy.getOriginStmt();
            NereidsParser nereidsParser = new NereidsParser();
            CreatePolicyCommand command = (CreatePolicyCommand) nereidsParser.parseSingle(sql);
            Optional<Expression> wherePredicate = command.getWherePredicate();
            if (!wherePredicate.isPresent()) {
                throw new AnalysisException("Invaild row policy [" + policy.getPolicyName() + "], " + sql);
            }
            switch (policy.getFilterType()) {
                case PERMISSIVE:
                    orList.add(wherePredicate.get());
                    break;
                case RESTRICTIVE:
                    andList.add(wherePredicate.get());
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
}
