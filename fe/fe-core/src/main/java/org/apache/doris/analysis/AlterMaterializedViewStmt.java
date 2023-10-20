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
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

public class AlterMaterializedViewStmt extends DdlStmt  {
    private TableName mvName;
    private MVRefreshInfo info;

    public AlterMaterializedViewStmt(TableName mvName, MVRefreshInfo info) {
        this.mvName = mvName;
        this.info = info;
    }

    public TableName getTable() {
        return mvName;
    }

    public MVRefreshInfo getRefreshInfo() {
        return info;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        mvName.analyze(analyzer);
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), mvName.getDb(), mvName.getTbl(),
                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "MATERIALIZED VIEW",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    mvName.getDb() + ": " + mvName.getTbl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER MATERIALIZED VIEW ").append(mvName.toSql()).append(" ").append(info.toString());
        return sb.toString();

    }
}
