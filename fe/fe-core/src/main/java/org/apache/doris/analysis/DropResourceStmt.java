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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

// DROP RESOURCE resource_name
public class DropResourceStmt extends DdlStmt {
    private boolean ifExists;
    private String resourceName;

    public DropResourceStmt(boolean ifExists, String resourceName) {
        this.ifExists = ifExists;
        this.resourceName = resourceName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getResourceName() {
        return resourceName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        FeNameFormat.checkResourceName(resourceName, ResourceTypeEnum.GENERAL);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP ");
        sb.append("RESOURCE `").append(resourceName).append("`");
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
