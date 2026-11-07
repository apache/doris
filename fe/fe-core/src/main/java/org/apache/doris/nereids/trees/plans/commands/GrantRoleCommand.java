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

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.WorkloadGroupPattern;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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
     * Granting a role hands its holder every privilege the role carries, so it is held to the rule a
     * direct GRANT already enforces: the grantor needs grant_priv and, unless it has admin_priv, each
     * privilege the role carries on the same object (otherwise any account with grant_priv alone could
     * take any existing role for itself); node_priv is never obtained through a role by an account that
     * lacks it, admin_priv or not, as a direct grant of node_priv is refused to such an account. The rule
     * reads the role as it is at the grant, exactly as a direct grant hands out fixed privileges: a
     * privilege granted TO the role later reaches every holder, so whoever grants to a role answers for
     * its membership (SHOW GRANTS lists it). A role that does not exist is reported by the grant itself.
     */
    public static void checkRolePrivileges(List<String> roleNames) throws AnalysisException {
        checkRolePrivileges();
        ConnectContext ctx = ConnectContext.get();
        Env env = Env.getCurrentEnv();
        for (String roleName : roleNames) {
            Role role = env.getAuth().getRoleByName(roleName);
            if (role != null && role.getTblPatternToPrivs().values().stream()
                    .anyMatch(privs -> privs.containsPrivs(Privilege.NODE_PRIV))
                    && !env.getAccessManager().checkGlobalPriv(ctx, PrivPredicate.OPERATOR)) {
                throw new AnalysisException("Only user with NODE_PRIV can grant/revoke NODE_PRIV to other user");
            }
        }
        if (env.getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            return;
        }
        for (String roleName : roleNames) {
            Role role = env.getAuth().getRoleByName(roleName);
            if (role == null) {
                continue;
            }
            for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                GrantTablePrivilegeCommand.checkGrantorHoldsTablePrivileges(ctx,
                        entry.getValue().toPrivilegeList(), entry.getKey());
            }
            GrantTablePrivilegeCommand.checkGrantorHoldsColumnPrivileges(ctx,
                    env.getAuth().getRoleColPrivsSnapshot(roleName));
            // Cloud clusters and stages exist on a cloud frontend only; their privilege tables are
            // checked against the cloud system info, which a shared-nothing frontend does not have.
            List<Map<ResourcePattern, PrivBitSet>> resourcePrivTables = Config.isCloudMode()
                    ? List.of(role.getResourcePatternToPrivs(), role.getStorageVaultPatternToPrivs(),
                            role.getClusterPatternToPrivs(), role.getStagePatternToPrivs())
                    : List.of(role.getResourcePatternToPrivs(), role.getStorageVaultPatternToPrivs());
            for (Map<ResourcePattern, PrivBitSet> resourcePrivs : resourcePrivTables) {
                for (Map.Entry<ResourcePattern, PrivBitSet> entry : resourcePrivs.entrySet()) {
                    GrantResourcePrivilegeCommand.checkGrantorHoldsResourcePrivileges(ctx,
                            entry.getValue().toPrivilegeList(), entry.getKey());
                }
            }
            for (Map.Entry<WorkloadGroupPattern, PrivBitSet> entry
                    : role.getWorkloadGroupPatternToPrivs().entrySet()) {
                GrantResourcePrivilegeCommand.checkGrantorHoldsWorkloadGroupPrivileges(ctx,
                        entry.getValue().toPrivilegeList(), entry.getKey());
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
