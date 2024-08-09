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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

/**
 * RefreshCatalogStmt
 * Manually refresh the catalog metadata.
 */
public class RefreshCatalogStmt extends DdlStmt {
    private static final String INVALID_CACHE = "invalid_cache";

    private final String catalogName;
    private Map<String, String> properties;

    /**
     * Set default value to true, otherwise
     * {@link org.apache.doris.catalog.RefreshManager.RefreshTask} will lost the default value
     */
    private boolean invalidCache = true;

    public RefreshCatalogStmt(String catalogName, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.properties = properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isInvalidCache() {
        return invalidCache;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        Util.checkCatalogAllRules(catalogName);
        if (catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            throw new AnalysisException("Internal catalog name can't be refresh.");
        }

        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                ConnectContext.get(), catalogName, PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                    analyzer.getQualifiedUser(), catalogName);
        }

        // Set to false only if user set the property "invalid_cache"="false"
        invalidCache = !(properties.get(INVALID_CACHE) != null && properties.get(INVALID_CACHE)
                .equalsIgnoreCase("false"));
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("REFRESH CATALOG ").append("`").append(catalogName).append("`");
        return stringBuilder.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REFRESH;
    }
}
