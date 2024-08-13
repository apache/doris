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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.mysql.privilege.ColPrivilegeKey;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

// GRANT STMT
// GRANT privilege[(col1,col2...)] [, privilege] ON db.tbl TO user_identity [ROLE 'role'];
// GRANT privilege [, privilege] ON RESOURCE 'resource' TO user_identity [ROLE 'role'];
// GRANT role [, role] TO user_identity
public class GrantStmt extends DdlStmt {
    private UserIdentity userIdent;
    // Indicates which permissions are granted to this role
    private String role;
    private TablePattern tblPattern;
    private ResourcePattern resourcePattern;
    private WorkloadGroupPattern workloadGroupPattern;
    private Set<Privilege> privileges = Sets.newHashSet();
    // Privilege,ctl,db,table -> cols
    private Map<ColPrivilegeKey, Set<String>> colPrivileges = Maps.newHashMap();
    // Indicates that these roles are granted to a user
    private List<String> roles;
    // AccessPrivileges will be parsed into two parts,
    // with the column permissions section placed in "colPrivileges" and the others in "privileges"
    private List<AccessPrivilegeWithCols> accessPrivileges;

    public GrantStmt(UserIdentity userIdent, String role, TablePattern tblPattern,
            List<AccessPrivilegeWithCols> privileges) {
        this(userIdent, role, tblPattern, null, null, privileges, ResourceTypeEnum.GENERAL);
    }

    public GrantStmt(UserIdentity userIdent, String role,
            ResourcePattern resourcePattern, List<AccessPrivilegeWithCols> privileges, ResourceTypeEnum type) {
        this(userIdent, role, null, resourcePattern, null, privileges, type);
    }

    public GrantStmt(UserIdentity userIdent, String role,
            WorkloadGroupPattern workloadGroupPattern, List<AccessPrivilegeWithCols> privileges) {
        this(userIdent, role, null, null, workloadGroupPattern, privileges, ResourceTypeEnum.GENERAL);
    }

    public GrantStmt(List<String> roles, UserIdentity userIdent) {
        this.userIdent = userIdent;
        this.roles = roles;
    }

    private GrantStmt(UserIdentity userIdent, String role, TablePattern tblPattern, ResourcePattern resourcePattern,
            WorkloadGroupPattern workloadGroupPattern, List<AccessPrivilegeWithCols> accessPrivileges,
            ResourceTypeEnum type) {
        this.userIdent = userIdent;
        this.role = role;
        this.tblPattern = tblPattern;
        this.resourcePattern = resourcePattern;
        this.workloadGroupPattern = workloadGroupPattern;
        if (this.resourcePattern != null) {
            this.resourcePattern.setResourceType(type);
        }
        this.accessPrivileges = accessPrivileges;
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

    public Set<Privilege> getPrivileges() {
        return privileges;
    }

    public List<String> getRoles() {
        return roles;
    }

    public Map<ColPrivilegeKey, Set<String>> getColPrivileges() {
        return colPrivileges;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")) {
            throw new AnalysisException("Grant is prohibited when Ranger is enabled.");
        }

        if (userIdent != null) {
            userIdent.analyze();
        } else {
            FeNameFormat.checkRoleName(role, false /* can not be admin */, "Can not grant to role");
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
                FeNameFormat.checkRoleName(originalRoleName, true /* can be admin */, "Can not grant role");
            }
        }

        if (!CollectionUtils.isEmpty(accessPrivileges)) {
            checkAccessPrivileges(accessPrivileges);

            for (AccessPrivilegeWithCols accessPrivilegeWithCols : accessPrivileges) {
                accessPrivilegeWithCols.transferAccessPrivilegeToDoris(privileges, colPrivileges, tblPattern);
            }
        }

        if (CollectionUtils.isEmpty(privileges) && CollectionUtils.isEmpty(roles) && MapUtils.isEmpty(colPrivileges)) {
            throw new AnalysisException("No privileges or roles in grant statement.");
        }

        if (tblPattern != null) {
            checkTablePrivileges(privileges, tblPattern, colPrivileges);
        } else if (resourcePattern != null) {
            PrivBitSet.convertResourcePrivToCloudPriv(resourcePattern, privileges);
            checkResourcePrivileges(privileges, resourcePattern);
        } else if (workloadGroupPattern != null) {
            checkWorkloadGroupPrivileges(privileges, workloadGroupPattern);
        } else if (roles != null) {
            checkRolePrivileges();
        }
    }

    public static void checkAccessPrivileges(
            List<AccessPrivilegeWithCols> accessPrivileges) throws AnalysisException {
        for (AccessPrivilegeWithCols access : accessPrivileges) {
            if ((!access.getAccessPrivilege().canHasColPriv()) && !CollectionUtils
                    .isEmpty(access.getCols())) {
                throw new AnalysisException(
                        String.format("%s do not support col auth.", access.getAccessPrivilege().name()));
            }
        }
    }

    /**
     * Rules:
     * 1. some privs in Privilege.notBelongToTablePrivileges can not granted/revoked on table
     * 2. ADMIN_PRIV and NODE_PRIV can only be granted/revoked on GLOBAL level
     * 3. Only the user with NODE_PRIV can grant NODE_PRIV to other user
     * 4. Check that the current user has both grant_priv and the permissions to be assigned to others
     * 5. col priv must assign to specific table
     *
     * @param privileges
     * @param tblPattern
     * @throws AnalysisException
     */
    public static void checkTablePrivileges(Collection<Privilege> privileges, TablePattern tblPattern,
            Map<ColPrivilegeKey, Set<String>> colPrivileges)
            throws AnalysisException {
        // Rule 1
        Privilege.checkIncorrectPrivilege(Privilege.notBelongToTablePrivileges, privileges);
        // Rule 2
        if (tblPattern.getPrivLevel() != PrivLevel.GLOBAL && (privileges.contains(Privilege.ADMIN_PRIV)
                || privileges.contains(Privilege.NODE_PRIV))) {
            throw new AnalysisException("ADMIN_PRIV and NODE_PRIV can only be granted/revoke on/from *.*.*");
        }

        // Rule 3
        if (privileges.contains(Privilege.NODE_PRIV) && !Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            throw new AnalysisException("Only user with NODE_PRIV can grant/revoke NODE_PRIV to other user");
        }

        // Rule 4
        PrivPredicate predicate = getPrivPredicate(privileges);
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        if (!accessManager.checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !checkTablePriv(ConnectContext.get(), predicate, tblPattern)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ALL_ACCESS_DENIED_ERROR,
                    predicate.getPrivs().toPrivilegeList());
        }

        // Rule 5
        if (!MapUtils.isEmpty(colPrivileges) && "*".equals(tblPattern.getTbl())) {
            throw new AnalysisException("Col auth must specify specific table");
        }
    }

    private static PrivPredicate getPrivPredicate(Collection<Privilege> privileges) {
        ArrayList<Privilege> privs = Lists.newArrayList(privileges);
        privs.add(Privilege.GRANT_PRIV);
        return PrivPredicate.of(PrivBitSet.of(privs), Operator.AND);
    }

    private static boolean checkTablePriv(ConnectContext ctx, PrivPredicate wanted,
            TablePattern tblPattern) {
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        switch (tblPattern.getPrivLevel()) {
            case GLOBAL:
                return accessManager.checkGlobalPriv(ctx, wanted);
            case CATALOG:
                return accessManager.checkCtlPriv(ConnectContext.get(),
                        tblPattern.getQualifiedCtl(), wanted);
            case DATABASE:
                return accessManager.checkDbPriv(ConnectContext.get(),
                        tblPattern.getQualifiedCtl(), tblPattern.getQualifiedDb(), wanted);
            default:
                return accessManager.checkTblPriv(ConnectContext.get(), tblPattern.getQualifiedCtl(),
                        tblPattern.getQualifiedDb(), tblPattern.getTbl(), wanted);

        }
    }

    public static void checkResourcePrivileges(Collection<Privilege> privileges,
            ResourcePattern resourcePattern) throws AnalysisException {
        Privilege.checkIncorrectPrivilege(Privilege.notBelongToResourcePrivileges, privileges);

        PrivPredicate predicate = getPrivPredicate(privileges);
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        if (!accessManager.checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !checkResourcePriv(ConnectContext.get(), resourcePattern, predicate)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ALL_ACCESS_DENIED_ERROR,
                    predicate.getPrivs().toPrivilegeList());
        }

    }

    private static boolean checkResourcePriv(ConnectContext ctx, ResourcePattern resourcePattern,
            PrivPredicate privPredicate) {
        if (resourcePattern.getPrivLevel() == PrivLevel.GLOBAL) {
            return Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, privPredicate);
        }

        switch (resourcePattern.getResourceType()) {
            case GENERAL:
            case STORAGE_VAULT:
                return Env.getCurrentEnv().getAccessManager()
                        .checkResourcePriv(ctx, resourcePattern.getResourceName(), privPredicate);
            case CLUSTER:
            case STAGE:
                return Env.getCurrentEnv().getAccessManager()
                        .checkCloudPriv(ctx, resourcePattern.getResourceName(), privPredicate,
                                resourcePattern.getResourceType());
            default:
                return true;
        }
    }

    public static void checkWorkloadGroupPrivileges(Collection<Privilege> privileges,
            WorkloadGroupPattern workloadGroupPattern) throws AnalysisException {
        Privilege.checkIncorrectPrivilege(Privilege.notBelongToWorkloadGroupPrivileges, privileges);

        PrivPredicate predicate = getPrivPredicate(privileges);
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        if (!accessManager.checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !accessManager.checkWorkloadGroupPriv(ConnectContext.get(),
                workloadGroupPattern.getworkloadGroupName(), predicate)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ALL_ACCESS_DENIED_ERROR,
                    predicate.getPrivs().toPrivilegeList());
        }
    }

    public static void checkRolePrivileges() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/REVOKE");
        }
    }

    public static String colPrivMapToString(Map<ColPrivilegeKey, Set<String>> colPrivileges) {
        if (MapUtils.isEmpty(colPrivileges)) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (Entry<ColPrivilegeKey, Set<String>> entry : colPrivileges.entrySet()) {
            builder.append(entry.getKey().getPrivilege());
            builder.append("(");
            builder.append(Joiner.on(", ").join(entry.getValue()));
            builder.append(")");
            builder.append(",");
        }
        return builder.deleteCharAt(builder.length() - 1).toString();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT ");
        if (!CollectionUtils.isEmpty(privileges)) {
            sb.append(Joiner.on(", ").join(privileges));
        }
        if (!MapUtils.isEmpty(colPrivileges)) {
            sb.append(colPrivMapToString(colPrivileges));
        }
        if (!CollectionUtils.isEmpty(roles)) {
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

    @Override
    public StmtType stmtType() {
        return StmtType.GRANT;
    }
}
