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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.PaloPrivilege;
import org.apache.doris.mysql.privilege.PrivBitSet;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.List;

// REVOKE STMT
// revoke privilege from some user, this is an administrator operation.
//
// REVOKE privilege [, privilege] ON db.tbl FROM user [ROLE 'role'];
// REVOKE privilege [, privilege] ON resource 'resource' FROM user [ROLE 'role'];
public class RevokeStmt extends DdlStmt {
    private UserIdentity userIdent;
    private String role;
    private TablePattern tblPattern;
    private ResourcePattern resourcePattern;
    private List<PaloPrivilege> privileges;

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

    public RevokeStmt(UserIdentity userIdent, String role, ResourcePattern resourcePattern, List<AccessPrivilege> privileges) {
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

    public List<PaloPrivilege> getPrivileges() {
        return privileges;
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
            tblPattern.analyze(analyzer.getClusterName());
        } else {
            // TODO(wyb): spark-load
            if (!Config.enable_spark_load) {
                throw new AnalysisException("REVOKE ON RESOURCE is comming soon");
            }
            resourcePattern.analyze();
        }

        if (privileges == null || privileges.isEmpty()) {
            throw new AnalysisException("No privileges in revoke statement.");
        }

        // Revoke operation obey the same rule as Grant operation. reuse the same method
        if (tblPattern != null) {
            GrantStmt.checkPrivileges(analyzer, privileges, role, tblPattern);
        } else {
            GrantStmt.checkPrivileges(analyzer, privileges, role, resourcePattern);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REVOKE ").append(Joiner.on(", ").join(privileges));
        if (tblPattern != null) {
            sb.append(" ON ").append(tblPattern).append(" FROM ");
        } else {
            sb.append(" ON RESOURCE '").append(resourcePattern).append("' FROM ");
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
