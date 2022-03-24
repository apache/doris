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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;

public class RefreshDbStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(RefreshDbStmt.class);

    private String dbName;

    public RefreshDbStmt(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }
        if (Strings.isNullOrEmpty(analyzer.getClusterName())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER);
        }
        dbName = ClusterNamespace.getFullName(getClusterName(), dbName);

        // Don't allow dropping 'information_schema' database
        if (dbName.equalsIgnoreCase(ClusterNamespace.getFullName(getClusterName(), InfoSchemaDb.DATABASE_NAME))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR, analyzer.getQualifiedUser(), dbName);
        }
        // check access
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    ConnectContext.get().getQualifiedUser(), dbName);
        }
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR, analyzer.getQualifiedUser(), dbName);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REFRESH DATABASE ").append("`").append(dbName).append("`");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
