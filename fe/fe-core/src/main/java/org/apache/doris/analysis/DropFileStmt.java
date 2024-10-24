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
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Map;
import java.util.Optional;

public class DropFileStmt extends DdlStmt implements NotFallbackInParser {
    public static final String PROP_CATALOG = "catalog";

    private String fileName;
    private String dbName;
    private Map<String, String> properties;

    private String catalogName;

    public DropFileStmt(String fileName, String dbName, Map<String, String> properties) {
        this.fileName = fileName;
        this.dbName = dbName;
        this.properties = properties;
    }

    public String getFileName() {
        return fileName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check operation privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (dbName == null) {
            dbName = analyzer.getDefaultDb();
        }

        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }

        if (Strings.isNullOrEmpty(fileName)) {
            throw new AnalysisException("File name is not specified");
        }

        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !PROP_CATALOG.equals(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        catalogName = properties.get(PROP_CATALOG);
        if (Strings.isNullOrEmpty(catalogName)) {
            throw new AnalysisException("catalog name is missing");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP FILE \"").append(fileName).append("\"");
        if (dbName != null) {
            sb.append(" FROM ").append(dbName);
        }

        sb.append(" PROPERTIES(");
        PrintableMap<String, String> map = new PrintableMap<>(properties, ",", true, false);
        sb.append(map.toString());
        return sb.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
