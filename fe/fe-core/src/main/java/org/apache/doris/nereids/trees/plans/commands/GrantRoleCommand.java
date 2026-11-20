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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * GRANT role [, role] TO user_identity
 */
public class GrantRoleCommand extends Command implements ForwardWithSync {
    private final UserIdentity userIdentity;
    private final List<String> roles;

    public GrantRoleCommand(UserIdentity userIdentity, List<String> roles) {
        super(PlanType.GRANT_ROLE_COMMAND);
        Preconditions.checkArgument(roles.size() > 0, "roles doesn't exist");

        this.userIdentity = Objects.requireNonNull(userIdentity, "userIdentity is null");
        this.roles = Objects.requireNonNull(roles, "roles is null");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        Env.getCurrentEnv().getAuth().grantRoleCommand(this);
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")) {
            throw new AnalysisException("Grant is prohibited when Ranger is enabled.");
        }

        userIdentity.analyze();

        for (int i = 0; i < roles.size(); i++) {
            String originalRoleName = roles.get(i);
            FeNameFormat.checkRoleName(originalRoleName, true /* can be admin */, "Can not grant role");
        }

        checkRolePrivileges(roles);
    }

    /** The gate of every role grant or revoke: grant_priv. A revoke takes nothing, so it needs no more. */
    public static void checkRolePrivileges() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/REVOKE");
        }
    }

    /**
     * Granting a role is granting a membership, not a copy of the privileges the role carries today:
     * the role's privileges can change later and every holder follows. So the grantor needs grant_priv
     * and must hold the role itself; admin_priv grants any role. Holding every privilege the role
     * carries is not holding the role, and granting privileges stays a separate act with its own rule.
     * To hold a role is to have it granted in Doris: a role that reaches a session from outside (an LDAP
     * group, the role mapping of an authentication integration) is governed there and does not count,
     * since a grant made on its strength would outlive it as a stored membership.
     */
    public static void checkRolePrivileges(List<String> roleNames) throws AnalysisException {
        checkRolePrivileges();
        ConnectContext ctx = ConnectContext.get();
        Env env = Env.getCurrentEnv();
        if (env.getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            return;
        }
        Set<String> heldRoles = env.getAuth().getRolesByUser(ctx.getCurrentUserIdentity(), true);
        for (String roleName : roleNames) {
            if (!heldRoles.contains(roleName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "ADMIN or membership of role '" + roleName + "'");
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRoleCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.GRANT;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public List<String> getRoles() {
        return roles;
    }
}
