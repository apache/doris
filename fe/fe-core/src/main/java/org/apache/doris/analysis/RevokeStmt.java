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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.Privilege;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

// REVOKE STMT
// revoke privilege from some user, this is an administrator operation.
//
// REVOKE privilege [, privilege] ON db.tbl FROM user_identity [ROLE 'role'];
// REVOKE privilege [, privilege] ON resource 'resource' FROM user_identity [ROLE 'role'];
// REVOKE role [, role] FROM user_identity
public class RevokeStmt extends DdlStmt {
    private UserIdentity userIdent;
    // Indicates which permissions are revoked from this role
    private String role;
    private TablePattern tblPattern;
    private ResourcePattern resourcePattern;
    private List<Privilege> privileges;
    // Indicates that these roles are revoked from a user
    private List<String> roles;

    public RevokeStmt(UserIdentity userIdent, String role, TablePattern tblPattern, List<AccessPrivilege> privileges) {
        this.userIdent = userIdent;
        this.role = role;
        this.tblPattern = tblPattern;
        this.resourcePattern = null;
        PrivBitSet privs = PrivBitSet.of();
        for (AccessPrivilege accessPrivilege : privileges) {
            privs.or(accessPrivilege.toPaloPrivilege());
        }
        this.privileges = privs.toPrivilegeList();
    }

    public RevokeStmt(UserIdentity userIdent, String role,
            ResourcePattern resourcePattern, List<AccessPrivilege> privileges) {
        this.userIdent = userIdent;
        this.role = role;
        this.tblPattern = null;
        this.resourcePattern = resourcePattern;
        PrivBitSet privs = PrivBitSet.of();
        for (AccessPrivilege accessPrivilege : privileges) {
            privs.or(accessPrivilege.toPaloPrivilege());
        }
        this.privileges = privs.toPrivilegeList();
    }

    public RevokeStmt(List<String> roles, UserIdentity userIdent) {
        this.roles = roles;
        this.userIdent = userIdent;
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
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (userIdent != null) {
            userIdent.analyze(analyzer.getClusterName());
        } else {
            FeNameFormat.checkRoleName(role, false /* can not be superuser */, "Can not revoke from role");
            role = ClusterNamespace.getFullName(analyzer.getClusterName(), role);
        }

        if (tblPattern != null) {
            tblPattern.analyze(analyzer);
        } else if (resourcePattern != null) {
            resourcePattern.analyze();
        } else if (roles != null) {
            for (int i = 0; i < roles.size(); i++) {
                String originalRoleName = roles.get(i);
                FeNameFormat.checkRoleName(originalRoleName, false /* can not be admin */, "Can not revoke role");
                roles.set(i, ClusterNamespace.getFullName(analyzer.getClusterName(), originalRoleName));
            }
        }

        if (CollectionUtils.isEmpty(privileges) && CollectionUtils.isEmpty(roles)) {
            throw new AnalysisException("No privileges or roles in revoke statement.");
        }

        // Revoke operation obey the same rule as Grant operation. reuse the same method
        if (tblPattern != null) {
            GrantStmt.checkTablePrivileges(privileges, role, tblPattern);
        } else if (resourcePattern != null) {
            GrantStmt.checkResourcePrivileges(privileges, role, resourcePattern);
        } else if (roles != null) {
            GrantStmt.checkRolePrivileges();
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REVOKE ");
        if (privileges != null) {
            sb.append(Joiner.on(", ").join(privileges));
        } else {
            sb.append(Joiner.on(", ").join(roles));
        }

        if (tblPattern != null) {
            sb.append(" ON ").append(tblPattern).append(" FROM ");
        } else if (resourcePattern != null) {
            sb.append(" ON RESOURCE '").append(resourcePattern).append("' FROM ");
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
}
