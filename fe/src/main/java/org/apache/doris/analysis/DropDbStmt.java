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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
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

    public DropDbStmt(boolean ifExists, String dbName) {
        this.ifExists = ifExists;
        this.dbName = dbName;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getDbName() {
        return this.dbName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }
        dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        // Don't allowed to drop 'information_schema'
        if (dbName.equalsIgnoreCase(ClusterNamespace.getFullName(getClusterName(), InfoSchemaDb.DATABASE_NAME))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, analyzer.getQualifiedUser(), dbName);
        }

        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED,
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
