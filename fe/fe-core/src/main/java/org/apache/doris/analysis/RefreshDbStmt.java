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
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class RefreshDbStmt extends DdlStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(RefreshDbStmt.class);
    private static final String INVALID_CACHE = "invalid_cache";

    private String catalogName;
    private String dbName;
    private Map<String, String> properties;
    private boolean invalidCache = false;

    public RefreshDbStmt(String dbName, Map<String, String> properties) {
        this.dbName = dbName;
        this.properties = properties;
    }

    public RefreshDbStmt(String catalogName, String dbName, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.properties = properties;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isInvalidCache() {
        return invalidCache;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(catalogName)) {
            catalogName = ConnectContext.get().getCurrentCatalog().getName();
        }
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }

        // Don't allow dropping 'information_schema' database
        if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, analyzer.getQualifiedUser(), dbName);
        }
        // Don't allow dropping 'mysql' database
        if (dbName.equalsIgnoreCase(MysqlDb.DATABASE_NAME)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, analyzer.getQualifiedUser(), dbName);
        }
        // check access
        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), catalogName,
                dbName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR,
                    PrivPredicate.SHOW.getPrivs().toString(), dbName);
        }
        String invalidConfig = properties == null ? null : properties.get(INVALID_CACHE);
        // Default is to invalid cache.
        invalidCache = invalidConfig == null ? true : invalidConfig.equalsIgnoreCase("true");
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REFRESH DATABASE ");
        if (catalogName != null) {
            sb.append("`").append(catalogName).append("`.");
        }
        sb.append("`").append(dbName).append("`");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REFRESH;
    }
}
