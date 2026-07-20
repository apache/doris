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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.proc.AuthProcDir;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * show grants command
 */
public class ShowGrantsCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA;
    private final boolean isAll;
    private UserIdentity userIdent; // if not given will update with self.
    private String roleName;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String col : AuthProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(100)));
        }
        META_DATA = builder.build();
    }

    public ShowGrantsCommand(UserIdentity userIdent, boolean isAll) {
        super(PlanType.SHOW_GRANTS_COMMAND);
        this.userIdent = userIdent;
        this.isAll = isAll;
        this.roleName = null;
    }

    public ShowGrantsCommand(String roleName) {
        super(PlanType.SHOW_GRANTS_COMMAND);
        this.roleName = roleName;
        this.isAll = false;
        this.userIdent = null;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (userIdent != null) {
            if (isAll) {
                throw new AnalysisException("Can not specified keyword ALL when specified user");
            }
            userIdent.analyze();
        } else {
            if (!isAll) {
                // self
                userIdent = ConnectContext.get().getCurrentUserIdentity();
            }
        }
        boolean isSelf = userIdent != null && ConnectContext.get().getCurrentUserIdentity().equals(userIdent);
        boolean hasGlobalGrantPriv = Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT);
        Preconditions.checkState(isAll || userIdent != null || roleName != null);
        // if show all grants, or show other user's grants, or show role's grants, need global GRANT priv.
        if (isAll || !isSelf) {
            if (!hasGlobalGrantPriv) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }
        if (roleName != null) {
            // check role if exists
            if (!Env.getCurrentEnv().getAccessManager().getAuth().doesRoleExist(roleName)) {
                throw new AnalysisException(String.format("Role: %s does not exist", roleName));
            }

            // check current user if belongs to this role
            Set<UserIdentity> roleUsers = Env.getCurrentEnv().getAccessManager().getAuth().getRoleUsers(roleName);
            boolean hasRole = roleUsers.contains(ConnectContext.get().getCurrentUserIdentity());

            // only users has admin priv or users belong to this role, that have show priv
            if (!hasGlobalGrantPriv && !hasRole) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return getRoleGrants(ctx, roleName, hasGlobalGrantPriv);
        }
        // ldap user not exist in userManager, so should not check
        if (userIdent != null && !isSelf && !Env.getCurrentEnv().getAccessManager().getAuth()
                .doesUserExist(userIdent)) {
            throw new AnalysisException(String.format("User: %s does not exist", userIdent));
        }
        List<List<String>> infos = Env.getCurrentEnv().getAuth().getAuthInfo(userIdent);

        // order by UserIdentity
        infos.sort(Comparator.comparing(list -> list.isEmpty() ? "" : list.get(0)));
        return new ShowResultSet(getMetaData(), infos);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowGrantsCommand(this, context);
    }

    private ShowResultSet getRoleGrants(ConnectContext ctx, String roleName, boolean hasGlobalGrantPriv)
            throws Exception {
        List<List<String>> roleAuthInfo;
        if (hasGlobalGrantPriv) {
            // only users have admin priv can show all grants for this role
            roleAuthInfo = Env.getCurrentEnv().getAccessManager().getAuth().getRoleAuthInfo(roleName);
        } else {
            // normal user can only show grant of themselves
            UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
            roleAuthInfo = Env.getCurrentEnv().getAccessManager().getAuth().getRoleAuthInfo(roleName, currentUser);
        }
        roleAuthInfo.sort(Comparator.comparing(list -> list.isEmpty() ? "" : list.get(0)));
        return new ShowResultSet(getMetaData(), roleAuthInfo);
    }

}
