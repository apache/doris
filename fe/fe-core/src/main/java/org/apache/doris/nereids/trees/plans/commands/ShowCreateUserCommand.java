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
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.User;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * show create user command
 */
public class ShowCreateUserCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowCreateUserCommand.class);
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("User Identity", ScalarType.createVarchar(512)))
                    .addColumn(new Column("Create Stmt", ScalarType.createVarchar(1024)))
                    .build();
    private UserIdentity userIdent;
    private boolean showAllUser;

    /**
     * constructor for show create user
     */
    public ShowCreateUserCommand(UserIdentity userIdent, boolean showAllUser) {
        super(PlanType.SHOW_CREATE_USER_COMMAND);
        this.userIdent = userIdent;
        this.showAllUser = showAllUser;
    }

    @VisibleForTesting
    protected ShowResultSet handleShowCreateUser(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (userIdent != null) {
            if (showAllUser) {
                throw new AnalysisException("Can not specified keyword ALL when specified user");
            }
            userIdent.analyze();
        } else {
            if (!showAllUser) {
                // self
                userIdent = ConnectContext.get().getCurrentUserIdentity();
            }
        }
        Preconditions.checkState(showAllUser || userIdent != null);
        UserIdentity self = ConnectContext.get().getCurrentUserIdentity();

        // if show all grants, or show other user's grants, need global GRANT priv.
        if (showAllUser || !self.equals(userIdent)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }
        if (userIdent != null && !Env.getCurrentEnv().getAccessManager().getAuth().doesUserExist(userIdent)) {
            throw new AnalysisException(String.format("User: %s does not exist", userIdent));
        }

        // get all user identity
        List<List<String>> infos = new ArrayList<>();
        List<UserIdentity> userList = new ArrayList<>();
        if (!showAllUser) {
            if (userIdent != null) {
                userList.add(userIdent);
            }
        } else {
            if (Env.getCurrentEnv().getAuth().getUserManager() != null) {
                Map<String, List<User>> usersMap = Env.getCurrentEnv().getAuth().getUserManager().getNameToUsers();
                for (List<User> users : usersMap.values()) {
                    for (User user : users) {
                        if (user == null) {
                            continue;
                        }
                        userList.add(user.getUserIdentity());
                    }
                }
            }
        }

        // get all user's create stmt
        for (UserIdentity identity : userList) {
            if (identity == null) {
                continue;
            }
            List<String> userInfo = new ArrayList<>();
            userInfo.add(identity.getQualifiedUser());
            userInfo.add(toSql(identity));
            infos.add(userInfo);
        }

        // order by UserIdentity
        infos.sort(Comparator.comparing(list -> list.isEmpty() ? "" : list.get(0)));
        return new ShowResultSet(getMetaData(), infos);
    }

    @VisibleForTesting
    protected String toSql(UserIdentity user) {
        if (user == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder("CREATE USER ");

        // user identity
        sb.append(user);

        // user password
        sb.append(" IDENTIFIED BY *** \n");

        // default role
        Set<String> roles = Env.getCurrentEnv().getAuth().getRolesByUser(user, true);
        if (roles != null && !roles.isEmpty()) {
            sb.append(" DEFAULT ROLE '").append(String.join(",", roles)).append("' \n");
        }

        // password policy
        if (Env.getCurrentEnv().getAuth().getPasswdPolicyManager() != null) {
            List<List<String>> policies = Env.getCurrentEnv().getAuth().getPasswdPolicyManager().getPolicyInfo(user);
            if (policies != null) {
                if (policies.size() > 1) {
                    List<String> expire = policies.get(0);
                    sb.append(" PASSWORD_EXPIRE ").append(expire.get(1));
                }
                if (policies.size() > 3) {
                    List<String> history = policies.get(2);
                    sb.append(" PASSWORD_HISTORY ").append(history.get(1));
                }
                if (policies.size() > 7) {
                    List<String> failedAttempts = policies.get(4);
                    List<String> lockTime = policies.get(5);
                    sb.append(" FAILED_LOGIN_ATTEMPTS ").append(failedAttempts.get(1))
                            .append(" PASSWORD_LOCK_TIME ").append(lockTime.get(1))
                            .append("\n");
                }
            }
        }

        // comment
        if (Env.getCurrentEnv().getAuth().getUserManager().getUserByUserIdentity(user) != null) {
            String comment = Env.getCurrentEnv().getAuth().getUserManager().getUserByUserIdentity(user).getComment();
            if (comment != null) {
                sb.append(comment);
            }
        }
        return sb.toString();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowCreateUser(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateUserCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

}
