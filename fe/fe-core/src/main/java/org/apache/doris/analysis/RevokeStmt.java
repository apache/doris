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

import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.ColPrivilegeKey;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.Privilege;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// REVOKE STMT
// revoke privilege from some user, this is an administrator operation.
// REVOKE privilege[(col1,col2...)] [, privilege] ON db.tbl FROM user_identity [ROLE 'role'];
// REVOKE privilege [, privilege] ON resource 'resource' FROM user_identity [ROLE 'role'];
// REVOKE role [, role] FROM user_identity
public class RevokeStmt extends DdlStmt implements NotFallbackInParser {
    private UserIdentity userIdent;
    // Indicates which permissions are revoked from this role
    private String role;
    private TablePattern tblPattern;
    private ResourcePattern resourcePattern;
    private WorkloadGroupPattern workloadGroupPattern;
    private Set<Privilege> privileges = Sets.newHashSet();
    private Map<ColPrivilegeKey, Set<String>> colPrivileges = Maps.newHashMap();
    // Indicates that these roles are revoked from a user
    private List<String> roles;
    List<AccessPrivilegeWithCols> accessPrivileges;

    public RevokeStmt(UserIdentity userIdent, String role, TablePattern tblPattern,
            List<AccessPrivilegeWithCols> privileges) {
        this(userIdent, role, tblPattern, null, null, privileges, ResourceTypeEnum.GENERAL);
    }

    public RevokeStmt(UserIdentity userIdent, String role,
            ResourcePattern resourcePattern, List<AccessPrivilegeWithCols> privileges, ResourceTypeEnum type) {
        this(userIdent, role, null, resourcePattern, null, privileges, type);
    }

    public RevokeStmt(UserIdentity userIdent, String role,
            WorkloadGroupPattern workloadGroupPattern, List<AccessPrivilegeWithCols> privileges) {
        this(userIdent, role, null, null, workloadGroupPattern, privileges, ResourceTypeEnum.GENERAL);
    }

    public RevokeStmt(List<String> roles, UserIdentity userIdent) {
        this.roles = roles;
        this.userIdent = userIdent;
    }

    private RevokeStmt(UserIdentity userIdent, String role, TablePattern tblPattern, ResourcePattern resourcePattern,
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
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")) {
            throw new AnalysisException("Revoke is prohibited when Ranger is enabled.");
        }

        if (userIdent != null) {
            userIdent.analyze();
        } else {
            FeNameFormat.checkRoleName(role, false /* can not be superuser */, "Can not revoke from role");
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
                FeNameFormat.checkRoleName(originalRoleName, true /* can be admin */, "Can not revoke role");
            }
        }
        if (!CollectionUtils.isEmpty(accessPrivileges)) {
            GrantStmt.checkAccessPrivileges(accessPrivileges);

            for (AccessPrivilegeWithCols accessPrivilegeWithCols : accessPrivileges) {
                accessPrivilegeWithCols.transferAccessPrivilegeToDoris(privileges, colPrivileges, tblPattern);
            }
        }
        if (CollectionUtils.isEmpty(privileges) && CollectionUtils.isEmpty(roles) && MapUtils.isEmpty(colPrivileges)) {
            throw new AnalysisException("No privileges or roles in revoke statement.");
        }

        // Revoke operation obey the same rule as Grant operation. reuse the same method
        if (tblPattern != null) {
            GrantStmt.checkTablePrivileges(privileges, tblPattern, colPrivileges);
        } else if (resourcePattern != null) {
            PrivBitSet.convertResourcePrivToCloudPriv(resourcePattern, privileges);
            GrantStmt.checkResourcePrivileges(privileges, resourcePattern);
        } else if (workloadGroupPattern != null) {
            GrantStmt.checkWorkloadGroupPrivileges(privileges, workloadGroupPattern);
        } else if (roles != null) {
            GrantStmt.checkRolePrivileges();
            if (roles.stream().map(String::toLowerCase).collect(Collectors.toList()).contains("admin")
                    && userIdent.isAdminUser()) {
                ErrorReport.reportAnalysisException("Unsupported operation");
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REVOKE ");
        if (!CollectionUtils.isEmpty(privileges)) {
            sb.append(Joiner.on(", ").join(privileges));
        }
        if (!MapUtils.isEmpty(colPrivileges)) {
            sb.append(GrantStmt.colPrivMapToString(colPrivileges));
        }
        if (!CollectionUtils.isEmpty(roles)) {
            sb.append(Joiner.on(", ").join(roles));
        }

        if (tblPattern != null) {
            sb.append(" ON ").append(tblPattern).append(" FROM ");
        } else if (resourcePattern != null) {
            sb.append(" ON RESOURCE '").append(resourcePattern).append("' FROM ");
        } else if (workloadGroupPattern != null) {
            sb.append(" ON WORKLOAD GROUP '").append(workloadGroupPattern).append("' FROM ");
        } else {
            sb.append(" FROM ");
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
        return StmtType.REVOKE;
    }
}
