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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;

public class SetLdapPassVar extends SetVar {
    private final PassVar passVar;

    public SetLdapPassVar(PassVar passVar) {
        this.passVar = passVar;
        this.varType = SetVarType.SET_LDAP_PASS_VAR;
    }

    public String getLdapPassword() {
        return passVar.getText();
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!ConnectContext.get().getCurrentUserIdentity().getQualifiedUser().equals(Auth.ROOT_USER)
                && !ConnectContext.get().getCurrentUserIdentity().getQualifiedUser().equals(Auth.ADMIN_USER)) {
            throw new AnalysisException("Only root and admin user can set ldap admin password.");
        }

        if (!passVar.isPlain()) {
            throw new AnalysisException("Only support set ldap password with plain text");
        }
        passVar.analyze();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SET LDAP_ADMIN_PASSWORD");
        sb.append(" = '*XXX'");
        return sb.toString();
    }
}
