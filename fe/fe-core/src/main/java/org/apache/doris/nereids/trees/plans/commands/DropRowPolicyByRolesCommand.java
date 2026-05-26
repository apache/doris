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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * DropRowPolicyByRolesCommand
 * Drop all row policies bound to the specified roles.
 * Syntax: DROP ROW POLICY FOR ROLE role1, role2, ...
 **/
public class DropRowPolicyByRolesCommand extends DropCommand {
    private final List<String> roleNames;

    public DropRowPolicyByRolesCommand(List<String> roleNames) {
        super(PlanType.DROP_ROW_POLICY_BY_ROLES_COMMAND);
        this.roleNames = roleNames;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Set<String> uniqueRoleNames = new LinkedHashSet<>(roleNames);
        Env.getCurrentEnv().getPolicyMgr().dropRowPoliciesByRoles(new ArrayList<>(uniqueRoleNames));
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        for (String roleName : roleNames) {
            if (roleName == null || roleName.isEmpty()) {
                throw new AnalysisException("role name is empty");
            }
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.GRANT.getPrivs().toString());
        }
    }

    public List<String> getRoleNames() {
        return roleNames;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropRowPolicyByRolesCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
