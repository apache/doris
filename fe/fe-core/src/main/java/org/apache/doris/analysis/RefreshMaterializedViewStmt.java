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

import org.apache.doris.analysis.MVRefreshInfo.RefreshMethod;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

/**
 * REFRESH MATERIALIZED VIEW [db_name].<mv_name> [complete|fast];
 *
 * Parameters
 * [complete|fast]: complete or incremental refresh.
 * mv_name: The name of the materialized view to refresh.
 */
public class RefreshMaterializedViewStmt extends DdlStmt {

    private TableName mvName;
    private RefreshMethod refreshMethod;

    public RefreshMaterializedViewStmt(TableName mvName, RefreshMethod refreshMethod) {
        this.mvName = mvName;
        if (refreshMethod == null) {
            this.refreshMethod = RefreshMethod.COMPLETE;
        }
        this.refreshMethod = refreshMethod;
    }

    public TableName getMvName() {
        return mvName;
    }

    public RefreshMethod getRefreshMethod() {
        return refreshMethod;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        mvName.analyze(analyzer);

        // check access
        if (!Env.getCurrentEnv().getAuth().checkTblPriv(ConnectContext.get(), mvName.getDb(),
                mvName.getTbl(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("REFRESH MATERIALIZED VIEW ")
                .append(mvName.toSql())
                .append(" ")
                .append(refreshMethod.toString());
        return stringBuilder.toString();
    }
}
