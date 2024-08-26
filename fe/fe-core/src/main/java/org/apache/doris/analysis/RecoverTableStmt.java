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
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

public class RecoverTableStmt extends DdlStmt {
    private TableName dbTblName;
    private long tableId = -1;
    private String newTableName = "";

    public RecoverTableStmt(TableName dbTblName, long tableId, String newTableName) {
        this.dbTblName = dbTblName;
        this.tableId = tableId;
        if (newTableName != null) {
            this.newTableName = newTableName;
        }
    }

    public String getDbName() {
        return dbTblName.getDb();
    }

    public String getTableName() {
        return dbTblName.getTbl();
    }

    public long getTableId() {
        return tableId;
    }

    public String getNewTableName() {
        return newTableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        dbTblName.analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(dbTblName.getCtl(), this.getClass().getSimpleName());

        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(
                ConnectContext.get(), dbTblName.getCtl(), dbTblName.getDb(), dbTblName.getTbl(),
                PrivPredicate.ALTER_CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "RECOVERY",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbTblName.getDb() + ": " + dbTblName.getTbl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("RECOVER TABLE ");
        if (!Strings.isNullOrEmpty(getDbName())) {
            sb.append(getDbName()).append(".");
        }
        sb.append(getTableName());
        if (this.tableId != -1) {
            sb.append(" ");
            sb.append(this.tableId);
        }
        if (!Strings.isNullOrEmpty(newTableName)) {
            sb.append(" AS ");
            sb.append(this.newTableName);
        }
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.RECOVER;
    }
}
