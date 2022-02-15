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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

public class SetLdapPassVar extends SetVar {
    private final String passwd;

    public SetLdapPassVar(String passwd) {
        this.passwd = passwd;
    }

    public String getLdapPassword() {
        return passwd;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(analyzer.getClusterName())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER);
        }

        if (!ConnectContext.get().getCurrentUserIdentity().getQualifiedUser().equals(PaloAuth.ROOT_USER)
                && !ConnectContext.get().getCurrentUserIdentity().getQualifiedUser().equals(PaloAuth.ADMIN_USER)) {
            throw new AnalysisException("Only root and admin user can set ldap admin password.");
        }
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
