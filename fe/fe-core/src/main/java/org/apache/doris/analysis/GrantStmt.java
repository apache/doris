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
import org.apache.doris.common.Config;
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
// GRANT privilege [, privilege] ON db.tbl TO user [ROLE 'role'];
// GRANT privilege [, privilege] ON RESOURCE 'resource' TO user [ROLE 'role'];
public class GrantStmt extends DdlStmt {
    private UserIdentity userIdent;
    private String role;
    private TablePattern tblPattern;
    private ResourcePattern resourcePattern;
    private List<PaloPrivilege> privileges;

    public GrantStmt(UserIdentity userIdent, String role, TablePattern tblPattern, List<AccessPrivilege> privileges) {
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

    public GrantStmt(UserIdentity userIdent, String role, ResourcePattern resourcePattern, List<AccessPrivilege> privileges) {
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
            FeNameFormat.checkRoleName(role, false /* can not be admin */, "Can not grant to role");
            role = ClusterNamespace.getFullName(analyzer.getClusterName(), role);
        }

        if (tblPattern != null) {
            tblPattern.analyze(analyzer.getClusterName());
        } else {
            // TODO(wyb): spark-load
            if (!Config.enable_spark_load) {
                throw new AnalysisException("GRANT ON RESOURCE is coming soon");
            }
            resourcePattern.analyze();
        }

        if (privileges == null || privileges.isEmpty()) {
            throw new AnalysisException("No privileges in grant statement.");
        }

        if (tblPattern != null) {
            checkPrivileges(analyzer, privileges, role, tblPattern);
        } else {
            checkPrivileges(analyzer, privileges, role, resourcePattern);
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
     *
     * @param analyzer
     * @param privileges
     * @param role
     * @param tblPattern
     * @throws AnalysisException
     */
    public static void checkPrivileges(Analyzer analyzer, List<PaloPrivilege> privileges,
                                       String role, TablePattern tblPattern) throws AnalysisException {
        // Rule 1
        if (tblPattern.getPrivLevel() != PrivLevel.GLOBAL && (privileges.contains(PaloPrivilege.ADMIN_PRIV)
                || privileges.contains(PaloPrivilege.NODE_PRIV))) {
            throw new AnalysisException("ADMIN_PRIV and NODE_PRIV can only be granted on *.*");
        }

        // Rule 2
        if (privileges.contains(PaloPrivilege.NODE_PRIV) && !Catalog.getCurrentCatalog().getAuth()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            throw new AnalysisException("Only the user with NODE_PRIV can grant NODE_PRIV to other user");
        }

        if (role != null) {
            // Rule 3 and 4
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        } else {
            // Rule 5.1 and 5.2
            if (tblPattern.getPrivLevel() == PrivLevel.GLOBAL) {
                if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            } else if (tblPattern.getPrivLevel() == PrivLevel.DATABASE){
                if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), tblPattern.getQualifiedDb(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            } else {
                // table level
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tblPattern.getQualifiedDb(), tblPattern.getTbl(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            }
        }
    }

    public static void checkPrivileges(Analyzer analyzer, List<PaloPrivilege> privileges,
                                       String role, ResourcePattern resourcePattern) throws AnalysisException {
        // Rule 1
        if (privileges.contains(PaloPrivilege.NODE_PRIV)) {
            throw new AnalysisException("Can not grant NODE_PRIV to any other users or roles");
        }

        // Rule 2
        if (resourcePattern.getPrivLevel() != PrivLevel.GLOBAL && privileges.contains(PaloPrivilege.ADMIN_PRIV)) {
            throw new AnalysisException("ADMIN_PRIV privilege can only be granted on resource *");
        }

        if (role != null) {
            // Rule 3 and 4
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        } else {
            // Rule 5.1 and 5.3
            if (resourcePattern.getPrivLevel() == PrivLevel.GLOBAL) {
                if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            } else {
                if (!Catalog.getCurrentCatalog().getAuth().checkResourcePriv(ConnectContext.get(), resourcePattern.getResourceName(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT ").append(Joiner.on(", ").join(privileges));
        if (tblPattern != null) {
            sb.append(" ON ").append(tblPattern).append(" TO ");
        } else {
            sb.append(" ON RESOURCE '").append(resourcePattern).append("' TO ");
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
