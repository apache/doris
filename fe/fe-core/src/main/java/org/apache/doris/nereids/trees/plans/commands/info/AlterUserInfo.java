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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PasswordPolicy;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * AlterUserInfo
 */
public class AlterUserInfo {
    private boolean ifExist;
    private UserDesc userDesc;
    private PasswordOptions passwordOptions;
    private String comment;
    private Set<AlterOpType> ops = Sets.newHashSet();

    public AlterUserInfo(boolean ifExist, UserDesc userDesc, PasswordOptions passwordOptions, String comment) {
        this.ifExist = ifExist;
        this.userDesc = userDesc;
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

    public PasswordOptions getPasswordOptions() {
        return passwordOptions;
    }

    public AlterOpType getOpType() {
        Preconditions.checkState(ops.size() == 1);
        return ops.iterator().next();
    }

    public String getComment() {
        return comment;
    }

    /**
     * validate
     */
    public void validate() throws UserException {
        userDesc.getUserIdent().analyze();
        userDesc.getPassVar().analyze();

        if (userDesc.hasPassword()) {
            ops.add(AlterOpType.SET_PASSWORD);
        }

        // may be set comment to "", so not use `Strings.isNullOrEmpty`
        if (comment != null) {
            ops.add(AlterOpType.MODIFY_COMMENT);
        }
        passwordOptions.analyze();
        if (passwordOptions.getAccountUnlocked() == PasswordPolicy.FailedLoginPolicy.LOCK_ACCOUNT) {
            throw new org.apache.doris.common.AnalysisException("Not support lock account now");
        } else if (passwordOptions.getAccountUnlocked() == PasswordPolicy.FailedLoginPolicy.UNLOCK_ACCOUNT) {
            ops.add(AlterOpType.UNLOCK_ACCOUNT);
        } else if (passwordOptions.getExpirePolicySecond() != PasswordOptions.UNSET
                || passwordOptions.getHistoryPolicy() != PasswordOptions.UNSET
                || passwordOptions.getPasswordLockSecond() != PasswordOptions.UNSET
                || passwordOptions.getLoginAttempts() != PasswordOptions.UNSET) {
            ops.add(AlterOpType.SET_PASSWORD_POLICY);
        }

        if (ops.size() != 1) {
            throw new org.apache.doris.common.AnalysisException("Only support doing one type of operation at one time");
        }

        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }
    }
}
