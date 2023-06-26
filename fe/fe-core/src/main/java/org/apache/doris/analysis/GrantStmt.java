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

package org.apache.doris.analysis;

import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

// GRANT STMT
// GRANT privilege [, privilege] ON db.tbl TO user_identity [ROLE 'role'];
// GRANT privilege [, privilege] ON RESOURCE 'resource' TO user_identity [ROLE 'role'];
// GRANT role [, role] TO user_identity
public class GrantStmt extends DdlStmt {
    private UserIdentity userIdent;
    // Indicates which permissions are granted to this role
    private String role;
    private TablePattern tblPattern;
    private ResourcePattern resourcePattern;
    private WorkloadGroupPattern workloadGroupPattern;
    private List<Privilege> privileges;
    // Indicates that these roles are granted to a user
    private List<String> roles;

    public GrantStmt(UserIdentity userIdent, String role, TablePattern tblPattern, List<AccessPrivilege> privileges) {
        this(userIdent, role, tblPattern, null, null, privileges);
    }

    public GrantStmt(UserIdentity userIdent, String role,
            ResourcePattern resourcePattern, List<AccessPrivilege> privileges) {
        this(userIdent, role, null, resourcePattern, null, privileges);
    }

    public GrantStmt(UserIdentity userIdent, String role,
            WorkloadGroupPattern workloadGroupPattern, List<AccessPrivilege> privileges) {
        this(userIdent, role, null, null, workloadGroupPattern, privileges);
    }

    public GrantStmt(List<String> roles, UserIdentity userIdent) {
        this.userIdent = userIdent;
        this.roles = roles;
    }

    private GrantStmt(UserIdentity userIdent, String role, TablePattern tblPattern, ResourcePattern resourcePattern,
            WorkloadGroupPattern workloadGroupPattern, List<AccessPrivilege> privileges) {
        this.userIdent = userIdent;
        this.role = role;
        this.tblPattern = tblPattern;
        this.resourcePattern = resourcePattern;
        this.workloadGroupPattern = workloadGroupPattern;
        PrivBitSet privs = PrivBitSet.of();
        for (AccessPrivilege accessPrivilege : privileges) {
            privs.or(accessPrivilege.toPaloPrivilege());
        }
        this.privileges = privs.toPrivilegeList();
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public TablePattern getTblPattern() {
        return tblPattern;
    }

    public ResourcePattern getResourcePattern() {
        return resourcePattern;
    }

    public WorkloadGroupPattern getWorkloadGroupPattern() {
        return workloadGroupPattern;
    }

    public boolean hasRole() {
        return !Strings.isNullOrEmpty(role);
    }

    public String getQualifiedRole() {
        return role;
    }

    public List<Privilege> getPrivileges() {
        return privileges;
    }

    public List<String> getRoles() {
        return roles;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (userIdent != null) {
            userIdent.analyze(analyzer.getClusterName());
        } else {
            FeNameFormat.checkRoleName(role, false /* can not be admin */, "Can not grant to role");
            role = ClusterNamespace.getFullName(analyzer.getClusterName(), role);
        }

        if (tblPattern != null) {
            tblPattern.analyze(analyzer);
        } else if (resourcePattern != null) {
            resourcePattern.analyze();
        } else if (workloadGroupPattern != null) {
            workloadGroupPattern.analyze();
        } else if (roles != null) {
            for (int i = 0; i < roles.size(); i++) {
                String originalRoleName = roles.get(i);
                FeNameFormat.checkRoleName(originalRoleName, false /* can not be admin */, "Can not grant role");
                roles.set(i, ClusterNamespace.getFullName(analyzer.getClusterName(), originalRoleName));
            }
        }

        if (CollectionUtils.isEmpty(privileges) && CollectionUtils.isEmpty(roles)) {
            throw new AnalysisException("No privileges or roles in grant statement.");
        }

        if (tblPattern != null) {
            checkTablePrivileges(privileges, role, tblPattern);
        } else if (resourcePattern != null) {
            checkResourcePrivileges(privileges, role, resourcePattern);
        } else if (workloadGroupPattern != null) {
            checkWorkloadGroupPrivileges(privileges, role, workloadGroupPattern);
        } else if (roles != null) {
            checkRolePrivileges();
        }
    }

    /**
     * Rules:
     * 1. ADMIN_PRIV and NODE_PRIV can only be granted/revoked on GLOBAL level
     * 2. Only the user with NODE_PRIV can grant NODE_PRIV to other user
     * 3. Privileges can not be granted/revoked to/from ADMIN and OPERATOR role
     * 4. Only user with GLOBAL level's GRANT_PRIV can grant/revoke privileges to/from roles.
     * 5.1 User should has GLOBAL level GRANT_PRIV
     * 5.2 or user has DATABASE/TABLE level GRANT_PRIV if grant/revoke to/from certain database or table.
     * 5.3 or user should has 'resource' GRANT_PRIV if grant/revoke to/from certain 'resource'
     * 5.4 or user should has 'workload group' GRANT_PRIV if grant/revoke to/from certain 'workload group'
     * 6. Can not grant USAGE_PRIV to database or table
     *
     * @param privileges
     * @param role
     * @param tblPattern
     * @throws AnalysisException
     */
    public static void checkTablePrivileges(List<Privilege> privileges, String role, TablePattern tblPattern)
            throws AnalysisException {
        // Rule 1
        if (tblPattern.getPrivLevel() != PrivLevel.GLOBAL && (privileges.contains(Privilege.ADMIN_PRIV)
                || privileges.contains(Privilege.NODE_PRIV))) {
            throw new AnalysisException("ADMIN_PRIV and NODE_PRIV can only be granted/revoke on/from *.*.*");
        }

        // Rule 2
        if (privileges.contains(Privilege.NODE_PRIV) && !Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            throw new AnalysisException("Only user with NODE_PRIV can grant/revoke NODE_PRIV to other user");
        }

        if (role != null) {
            // Rule 3 and 4
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
            }
        } else {
            // Rule 5.1 and 5.2
            if (tblPattern.getPrivLevel() == PrivLevel.GLOBAL) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
                }
            } else if (tblPattern.getPrivLevel() == PrivLevel.CATALOG) {
                if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(ConnectContext.get(),
                        tblPattern.getQualifiedCtl(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
                }
            } else if (tblPattern.getPrivLevel() == PrivLevel.DATABASE) {
                if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                        tblPattern.getQualifiedCtl(), tblPattern.getQualifiedDb(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
                }
            } else {
                // table level
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), tblPattern.getQualifiedCtl(), tblPattern.getQualifiedDb(),
                                tblPattern.getTbl(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
                }
            }
        }

        // Rule 6
        if (privileges.contains(Privilege.USAGE_PRIV)) {
            throw new AnalysisException("Can not grant/revoke USAGE_PRIV to/from database or table");
        }
    }

    public static void checkResourcePrivileges(List<Privilege> privileges, String role,
            ResourcePattern resourcePattern) throws AnalysisException {
        for (int i = 0; i < Privilege.notBelongToResourcePrivileges.length; i++) {
            if (privileges.contains(Privilege.notBelongToResourcePrivileges[i])) {
                throw new AnalysisException(
                        String.format("Can not grant/revoke %s on resource to/from any other users or roles",
                                Privilege.notBelongToResourcePrivileges[i]));
            }
        }

        if (role != null) {
            // Rule 3 and 4
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
            }
        } else {
            // Rule 5.1 and 5.3
            if (resourcePattern.getPrivLevel() == PrivLevel.GLOBAL) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
                }
            } else {
                if (!Env.getCurrentEnv().getAccessManager().checkResourcePriv(ConnectContext.get(),
                        resourcePattern.getResourceName(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
                }
            }
        }
    }

    public static void checkWorkloadGroupPrivileges(List<Privilege> privileges, String role,
            WorkloadGroupPattern workloadGroupPattern) throws AnalysisException {
        for (int i = 0; i < Privilege.notBelongToWorkloadGroupPrivileges.length; i++) {
            if (privileges.contains(Privilege.notBelongToWorkloadGroupPrivileges[i])) {
                throw new AnalysisException(
                        String.format("Can not grant/revoke %s on workload group to/from any other users or roles",
                                Privilege.notBelongToWorkloadGroupPrivileges[i]));
            }
        }

        if (role != null) {
            // Rule 4
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
            }
        } else if (!Env.getCurrentEnv().getAccessManager().checkWorkloadGroupPriv(ConnectContext.get(),
                workloadGroupPattern.getworkloadGroupName(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
        }
    }

    public static void checkRolePrivileges() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/ROVOKE");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT ");
        if (privileges != null) {
            sb.append(Joiner.on(", ").join(privileges));
        } else {
            sb.append(Joiner.on(", ").join(roles));
        }

        if (tblPattern != null) {
            sb.append(" ON ").append(tblPattern).append(" TO ");
        } else if (resourcePattern != null) {
            sb.append(" ON RESOURCE '").append(resourcePattern).append("' TO ");
        } else if (workloadGroupPattern != null) {
            sb.append(" ON WORKLOAD GROUP '").append(workloadGroupPattern).append("' TO ");
        } else {
            sb.append(" TO ");
        }
        if (!Strings.isNullOrEmpty(role)) {
            sb.append(" ROLE '").append(role).append("'");
        } else {
            sb.append(userIdent);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
