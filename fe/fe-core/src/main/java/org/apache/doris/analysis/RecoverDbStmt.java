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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

public class RecoverDbStmt extends DdlStmt {
    private String dbName;
    private long dbId = -1;
    private String newDbName = "";

    public RecoverDbStmt(String dbName, long dbId, String newDbName) {
        this.dbName = dbName;
        this.dbId = dbId;
        if (newDbName != null) {
            this.newDbName = newDbName;
        }
    }

    public String getDbName() {
        return dbName;
    }

    public long getDbId() {
        return dbId;
    }

    public String getNewDbName() {
        return newDbName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }

        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                        PrivPredicate.ALTER_CREATE)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, analyzer.getQualifiedUser(), dbName);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("RECOVER");
        sb.append(" DATABASE ");
        sb.append(this.dbName);
        if (this.dbId != -1) {
            sb.append(" ");
            sb.append(this.dbId);
        }
        if (!Strings.isNullOrEmpty(newDbName)) {
            sb.append(" AS ");
            sb.append(this.newDbName);
        }
        return sb.toString();
    }
}
