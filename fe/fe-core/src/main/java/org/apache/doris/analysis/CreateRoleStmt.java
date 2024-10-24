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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

public class CreateRoleStmt extends DdlStmt implements NotFallbackInParser {

    private boolean ifNotExists;
    private String role;

    private String comment;

    public CreateRoleStmt(String role) {
        this.role = role;
    }

    public CreateRoleStmt(boolean ifNotExists, String role, String comment) {
        this.ifNotExists = ifNotExists;
        this.role = role;
        this.comment = Strings.nullToEmpty(comment);
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getRole() {
        return role;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")) {
            throw new AnalysisException("Create role is prohibited when Ranger is enabled.");
        }

        FeNameFormat.checkRoleName(role, false /* can not be admin */, "Can not create role");

        // check if current user has GRANT priv on GLOBAL level.
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE ROLE");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ROLE ");
        sb.append(role);
        if (!StringUtils.isEmpty(comment)) {
            sb.append(" COMMENT \"").append(comment).append("\"");
        }
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
