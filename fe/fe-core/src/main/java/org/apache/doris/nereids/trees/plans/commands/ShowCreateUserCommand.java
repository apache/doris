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
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * show create user command
 */
public class ShowCreateUserCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("User Identity", ScalarType.createVarchar(512)))
                .addColumn(new Column("Create Stmt", ScalarType.createVarchar(1024)))
                .build();
    private UserIdentity userIdent;

    /**
     * constructor for show create user
     */
    public ShowCreateUserCommand(UserIdentity userIdent) {
        super(PlanType.SHOW_CREATE_USER_COMMAND);
        this.userIdent = userIdent;
    }

    @VisibleForTesting
    protected ShowResultSet handleShowCreateUser(ConnectContext ctx, StmtExecutor executor) throws Exception {
        userIdent.analyze();
        Preconditions.checkState(userIdent != null);
        UserIdentity self = ConnectContext.get().getCurrentUserIdentity();

        // need global GRANT priv.
        if (!self.equals(userIdent)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }
        if (userIdent != null && !Env.getCurrentEnv().getAccessManager().getAuth().doesUserExist(userIdent)) {
            throw new AnalysisException(String.format("User: %s does not exist", userIdent));
        }

        // get all user identity
        List<List<String>> infos = new ArrayList<>();

        // get user's create stmt
        List<String> userInfo = new ArrayList<>();
        userInfo.add(userIdent.getQualifiedUser());
        userInfo.add(toSql(userIdent));
        infos.add(userInfo);

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
        sb.append(" IDENTIFIED BY *** ");

        // password policy
        if (Env.getCurrentEnv().getAuth().getPasswdPolicyManager() != null) {
            // policies are : <expirePolicy, historyPolicy, failedLoginPolicy>
            // expirePolicy has two lists: <EXPIRATION_SECONDS>, <PASSWORD_CREATION_TIME>
            // historyPolicy has two lists: <HISTORY_NUM>, <HISTORY_PASSWORDS>
            // failedLoginPolicy has four lists :
            // <NUM_FAILED_LOGIN>, <PASSWORD_LOCK_SECONDS>, <FAILED_LOGIN_COUNTER>, <LOCK_TIME>
            List<List<String>> policies = Env.getCurrentEnv().getAuth().getPasswdPolicyManager().getPolicyInfo(user);
            if (policies != null) {
                // historyPolicy: <HISTORY_NUM>, <HISTORY_PASSWORDS>; use HISTORY_NUM only
                if (policies.size() > 3) {
                    List<String> history = policies.get(2);
                    String historyValue = history.get(1).equalsIgnoreCase("DEFAULT")
                            || history.get(1).equalsIgnoreCase("NO_RESTRICTION") ? "DEFAULT" : history.get(1);
                    sb.append(" PASSWORD_HISTORY ").append(historyValue);
                }

                // expirePolicy: <EXPIRATION_SECONDS>, <PASSWORD_CREATION_TIME>; use EXPIRATION_SECONDS only
                if (policies.size() > 1) {
                    List<String> expire = policies.get(0);
                    String expireValue = expire.get(1);
                    if (expireValue.equalsIgnoreCase("DEFAULT") || expireValue.equalsIgnoreCase("NEVER")) {
                        sb.append(" PASSWORD_EXPIRE ").append(expireValue);
                    } else {
                        sb.append(" PASSWORD_EXPIRE INTERVAL ").append(expireValue).append(" SECOND");
                    }
                }

                // failedLoginPolicy: <NUM_FAILED_LOGIN>, <PASSWORD_LOCK_SECONDS>, <FAILED_LOGIN_COUNTER>, <LOCK_TIME>
                // use NUM_FAILED_LOGIN and PASSWORD_LOCK_SECONDS only
                if (policies.size() > 7) {
                    List<String> failedAttempts = policies.get(4);
                    String attemptValue = failedAttempts.get(1).equalsIgnoreCase("DISABLED") ? "0"
                            : failedAttempts.get(1);
                    sb.append(" FAILED_LOGIN_ATTEMPTS ").append(attemptValue);

                    List<String> lockTime = policies.get(5);
                    String lockValue = lockTime.get(1).equalsIgnoreCase("DISABLED") ? "UNBOUNDED" : lockTime.get(1);
                    if (lockValue.equalsIgnoreCase("DISABLED") || lockValue.equalsIgnoreCase("UNBOUNDED")) {
                        sb.append(" PASSWORD_LOCK_TIME UNBOUNDED");
                    } else {
                        sb.append(" PASSWORD_LOCK_TIME ").append(lockValue).append(" SECOND");
                    }
                }
            }
        }

        // comment
        if (Env.getCurrentEnv().getAuth().getUserManager().getUserByUserIdentity(user) != null) {
            String comment = Env.getCurrentEnv().getAuth().getUserManager().getUserByUserIdentity(user).getComment();
            if (comment != null) {
                sb.append(" COMMENT ").append("\"").append(comment).append("\"");
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
