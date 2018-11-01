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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth.PrivLevel;
import org.apache.doris.mysql.privilege.PaloPrivilege;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.List;

// GRANT STMT
// grant privilege to some user, this is an administrator operation.
//
// GRANT privilege [, privilege] ON db.tbl TO user [ROLE 'role'];
public class GrantStmt extends DdlStmt {
    private UserIdentity userIdent;
    private String role;
    private TablePattern tblPattern;
    private List<PaloPrivilege> privileges;

    public GrantStmt(UserIdentity userIdent, String role, TablePattern tblPattern, List<AccessPrivilege> privileges) {
        this.userIdent = userIdent;
        this.role = role;
        this.tblPattern = tblPattern;
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

    public boolean hasRole() {
        return !Strings.isNullOrEmpty(role);
    }

    public String getQualifiedRole() {
        return role;
    }

    public List<PaloPrivilege> getPrivileges() {
        return privileges;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (userIdent != null) {
            userIdent.analyze(analyzer.getClusterName());
        } else {
            FeNameFormat.checkUserName(role);
        }

        tblPattern.analyze(analyzer.getClusterName());

        if (privileges == null || privileges.isEmpty()) {
            throw new AnalysisException("No privileges in grant statement.");
        }

        // can not grant NODE_PRIV to any other user(root has NODE_PRIV, no need to grant)
        for (PaloPrivilege paloPrivilege : privileges) {
            if (paloPrivilege == PaloPrivilege.NODE_PRIV) {
                throw new AnalysisException("Can not grant NODE_PRIV to any other users or roles");
            }
        }

        // ADMIN_PRIV and GRANT_PRIV can only be granted as global
        if (tblPattern.getPrivLevel() != PrivLevel.GLOBAL) {
            for (PaloPrivilege paloPrivilege : privileges) {
                if (paloPrivilege == PaloPrivilege.ADMIN_PRIV || paloPrivilege == PaloPrivilege.GRANT_PRIV) {
                    throw new AnalysisException(
                            "Can not grant ADMIN_PRIV or GRANT_PRIV to specified database or table. Only support to *.*");
                }
            }
        }

        if (role != null) {
            // can not grant to admin or operator role
            FeNameFormat.checkRoleName(role, false /* can not be admin */, "Can not grant to role");
            role = ClusterNamespace.getFullName(analyzer.getClusterName(), role);
        }

        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                                "GRANT");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT ").append(Joiner.on(", ").join(privileges));
        sb.append(" ON ").append(tblPattern).append(" TO ");
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
