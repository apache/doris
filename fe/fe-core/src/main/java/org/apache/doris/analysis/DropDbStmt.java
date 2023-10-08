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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

// DROP DB表达式
public class DropDbStmt extends DdlStmt {
    private boolean ifExists;
    private String dbName;
    private boolean forceDrop;

    public DropDbStmt(boolean ifExists, String dbName, boolean forceDrop) {
        this.ifExists = ifExists;
        this.dbName = dbName;
        this.forceDrop = forceDrop;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getDbName() {
        return this.dbName;
    }

    public boolean isForceDrop() {
        return this.forceDrop;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }
        dbName = ClusterNamespace.getFullName(getClusterName(), dbName);

        // Don't allow to drop mysql compatible databases
        DatabaseIf db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
        if (db != null && (db instanceof Database) && ((Database) db).isMysqlCompatibleDatabase()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    analyzer.getQualifiedUser(), dbName);
        }

        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                                                ConnectContext.get().getQualifiedUser(), dbName);
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP DATABASE ").append("`").append(dbName).append("`");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

}
