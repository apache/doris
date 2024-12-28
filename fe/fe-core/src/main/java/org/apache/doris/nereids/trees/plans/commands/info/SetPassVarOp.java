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

import org.apache.doris.analysis.PassVar;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

/**
 * SetPassVarOp
 */
public class SetPassVarOp extends SetVarOp {
    private UserIdentity userIdent;
    private PassVar passVar;

    // The password in parameter is a hashed password.
    public SetPassVarOp(UserIdentity userIdent, PassVar passVar) {
        super(SetType.DEFAULT);
        this.userIdent = userIdent;
        this.passVar = passVar;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        boolean isSelf = false;
        if (userIdent == null) {
            // set userIdent as what current_user() returns
            userIdent = ctx.getCurrentUserIdentity();
            isSelf = true;
        } else {
            userIdent.analyze();
            if (userIdent.equals(ctx.getCurrentUserIdentity())) {
                isSelf = true;
            }
        }

        // Check password
        if (passVar != null) {
            passVar.analyze();
        }

        // check privs.
        // 1. this is user itself
        if (isSelf) {
            return;
        }

        // 2. No user can set password for root expect for root user itself
        if (userIdent.getQualifiedUser().equals(Auth.ROOT_USER)) {
            throw new AnalysisException("Can not set password for root user, except root itself");
        }

        // 3. user has grant privs
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }
    }

    @Override
    public void run(ConnectContext ctx) throws Exception {
        ctx.getEnv().getAuth().setPassword(userIdent, passVar.getScrambled());
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SET PASSWORD");
        if (userIdent != null) {
            sb.append(" FOR ").append(userIdent);
        }
        sb.append(" = '*XXX'");
        return sb.toString();
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }
}
