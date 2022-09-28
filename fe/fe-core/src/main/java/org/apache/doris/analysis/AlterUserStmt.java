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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth.PrivLevel;
import org.apache.doris.mysql.privilege.PasswordPolicy.FailedLoginPolicy;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AlterUserStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(AlterUserStmt.class);

    private boolean ifNotExist;
    private UserDesc userDesc;
    private String role;
    private PasswordOptions passwordOptions;

    public AlterUserStmt(boolean ifNotExist, UserDesc userDesc, String role, PasswordOptions passwordOptions) {
        this.ifNotExist = ifNotExist;
        this.userDesc = userDesc;
        this.role = role;
        this.passwordOptions = passwordOptions;
    }

    public boolean isIfNotExist() {
        return ifNotExist;
    }

    public UserIdentity getUserIdent() {
        return userDesc.getUserIdent();
    }

    public PasswordOptions getPasswordOptions() {
        return passwordOptions;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        userDesc.getUserIdent().analyze(analyzer.getClusterName());
        userDesc.getPassVar().analyze();

        if (!Strings.isNullOrEmpty(userDesc.getPassVar().getText())) {
            throw new UserException("Not support setting password in alter stmt");
        }

        if (!Strings.isNullOrEmpty(role)) {
            throw new UserException("Not support setting role in alter user stmt");
        }

        passwordOptions.analyze();
        if (passwordOptions.getAccountUnlocked() == FailedLoginPolicy.LOCK_ACCOUNT) {
            throw new UserException("Not supprt locking account");
        }

        // check if current user has GRANT priv on GLOBAL or DATABASE level.
        if (!Env.getCurrentEnv().getAuth().checkHasPriv(ConnectContext.get(),
                PrivPredicate.GRANT, PrivLevel.GLOBAL, PrivLevel.DATABASE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        return sb.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        // TODO: currently we only support unlock acccount, which is executed on specific FE.
        return RedirectStatus.NO_FORWARD;
    }
}
