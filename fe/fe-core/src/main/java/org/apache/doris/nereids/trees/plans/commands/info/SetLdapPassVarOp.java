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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

/**
 * SetLdapPassVarOp
 */
public class SetLdapPassVarOp extends SetVarOp {
    private final PassVar passVar;

    public SetLdapPassVarOp(PassVar passVar) {
        super(SetType.DEFAULT);
        this.passVar = passVar;
    }

    public String getLdapPassword() {
        return passVar.getText();
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }
        if (!passVar.isPlain()) {
            throw new AnalysisException("Only support set ldap password with plain text");
        }
        passVar.analyze();
    }

    @Override
    public void run(ConnectContext ctx) throws Exception {
        ctx.getEnv().getAuth().setLdapPassword(passVar.getText());
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SET LDAP_ADMIN_PASSWORD");
        sb.append(" = '*XXX'");
        return sb.toString();
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }
}
