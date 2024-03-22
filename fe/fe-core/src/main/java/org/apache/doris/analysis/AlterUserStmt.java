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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PasswordPolicy.FailedLoginPolicy;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import java.util.Set;

// ALTER USER user@host [IDENTIFIED BY "password"] [DEFAULT ROLE role] [password_options]
// password_options:
//      PASSWORD_HISTORY
//      ACCOUNT_LOCK[ACCOUNT_UNLOCK]
//      FAILED_LOGIN_ATTEMPTS
//      PASSWORD_LOCK_TIME
public class AlterUserStmt extends DdlStmt {
    private boolean ifExist;
    private UserDesc userDesc;
    private String role;
    private PasswordOptions passwordOptions;

    private String comment;

    // Only support doing one of these operation at one time.
    public enum OpType {
        SET_PASSWORD,
        SET_ROLE,
        SET_PASSWORD_POLICY,
        LOCK_ACCOUNT,
        UNLOCK_ACCOUNT,
        MODIFY_COMMENT
    }

    private Set<OpType> ops = Sets.newHashSet();

    public AlterUserStmt(boolean ifExist, UserDesc userDesc, String role, PasswordOptions passwordOptions,
            String comment) {
        this.ifExist = ifExist;
        this.userDesc = userDesc;
        this.role = role;
        this.passwordOptions = passwordOptions;
        this.comment = comment;
    }

    public boolean isIfExist() {
        return ifExist;
    }

    public UserIdentity getUserIdent() {
        return userDesc.getUserIdent();
    }

    public byte[] getPassword() {
        if (userDesc.hasPassword()) {
            return userDesc.getPassVar().getScrambled();
        }
        return null;
    }

    public String getRole() {
        return role;
    }

    public PasswordOptions getPasswordOptions() {
        return passwordOptions;
    }

    public OpType getOpType() {
        Preconditions.checkState(ops.size() == 1);
        return ops.iterator().next();
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        userDesc.getUserIdent().analyze();
        userDesc.getPassVar().analyze();

        if (userDesc.hasPassword()) {
            ops.add(OpType.SET_PASSWORD);
        }

        if (!Strings.isNullOrEmpty(role)) {
            ops.add(OpType.SET_ROLE);
        }

        // may be set comment to "", so not use `Strings.isNullOrEmpty`
        if (comment != null) {
            ops.add(OpType.MODIFY_COMMENT);
        }
        passwordOptions.analyze();
        if (passwordOptions.getAccountUnlocked() == FailedLoginPolicy.LOCK_ACCOUNT) {
            throw new AnalysisException("Not support lock account now");
        } else if (passwordOptions.getAccountUnlocked() == FailedLoginPolicy.UNLOCK_ACCOUNT) {
            ops.add(OpType.UNLOCK_ACCOUNT);
        } else if (passwordOptions.getExpirePolicySecond() != PasswordOptions.UNSET
                || passwordOptions.getHistoryPolicy() != PasswordOptions.UNSET
                || passwordOptions.getPasswordLockSecond() != PasswordOptions.UNSET
                || passwordOptions.getLoginAttempts() != PasswordOptions.UNSET) {
            ops.add(OpType.SET_PASSWORD_POLICY);
        }

        if (ops.size() != 1) {
            throw new AnalysisException("Only support doing one type of operation at one time");
        }

        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER USER ").append(userDesc.getUserIdent());
        if (!Strings.isNullOrEmpty(userDesc.getPassVar().getText())) {
            if (userDesc.getPassVar().isPlain()) {
                sb.append(" IDENTIFIED BY '").append("*XXX").append("'");
            } else {
                sb.append(" IDENTIFIED BY PASSWORD '").append(userDesc.getPassVar().getText()).append("'");
            }
        }

        if (!Strings.isNullOrEmpty(role)) {
            sb.append(" DEFAULT ROLE '").append(role).append("'");
        }
        if (passwordOptions != null) {
            sb.append(passwordOptions.toSql());
        }

        return sb.toString();
    }
}
