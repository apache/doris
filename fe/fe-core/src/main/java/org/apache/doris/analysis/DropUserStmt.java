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
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.authenticate.AuthenticateType;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

// drop user cmy@['domain'];
// drop user cmy  <==> drop user cmy@'%'
// drop user cmy@'192.168.1.%'
public class DropUserStmt extends DdlStmt {

    private boolean ifExists;
    private UserIdentity userIdent;

    public DropUserStmt(UserIdentity userIdent) {
        this.userIdent = userIdent;
    }

    public DropUserStmt(boolean ifExists, UserIdentity userIdent) {
        this.ifExists = ifExists;
        this.userIdent = userIdent;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public UserIdentity getUserIdentity() {
        return userIdent;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")
                && AuthenticateType.getAuthTypeConfig() == AuthenticateType.LDAP) {
            throw new AnalysisException("Drop user is prohibited when Ranger and LDAP are enabled at same time.");
        }

        userIdent.analyze();

        if (userIdent.isRootUser()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR, "Can not drop root user");
        }

        // only user with GLOBAL level's GRANT_PRIV can drop user.
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP USER");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP USER ").append(userIdent);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
