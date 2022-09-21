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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogFlattenUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Representation of a USE db statement.
 */
public class UseStmt extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(UseStmt.class);
    private String catalogName;
    private String database;

    public UseStmt(String db) {
        database = db;
    }

    public UseStmt(String catalogName, String db) {
        this.catalogName = catalogName;
        this.database = db;
    }

    public String getDatabase() {
        return database;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("USE ");
        if (catalogName != null) {
            sb.append("`").append(catalogName).append("`.");
        }
        sb.append("`").append(database).append("`");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(database)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }

        // We support following formats:
        // 1. use db: Will use default catalog in context and the specific db
        // 2. use catalog.db: Will use specific catalog and db
        // 3. use __catalog__db: flattened catalog_db, will parse and use specific catalog and db
        if (Strings.isNullOrEmpty(catalogName)) {
            Pair<String, String> ctlDb = CatalogFlattenUtils.analyzeFlattenName(database);
            catalogName = ctlDb.first;
            database = ctlDb.second;
        }

        if (catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            database = ClusterNamespace.getFullName(getClusterName(), database);
        }

        if (!Env.getCurrentEnv().getAuth().checkCtlPriv(ConnectContext.get(), catalogName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_ACCESS_DENIED_ERROR,
                    analyzer.getQualifiedUser(), catalogName);
        }

        if (!Env.getCurrentEnv().getAuth().checkDbPriv(ConnectContext.get(), database, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    analyzer.getQualifiedUser(), database);
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
