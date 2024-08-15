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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

public class SwitchStmt extends StatementBase implements NotFallbackInParser {
    private final String catalogName;

    public SwitchStmt(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String toSql() {
        return "SWITCH `" + catalogName + "`";
    }

    @Override
    public String toString() {
        return toSql();
    }

    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        Util.checkCatalogAllRules(catalogName);

        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                ConnectContext.get(), catalogName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_CATALOG_ACCESS_DENIED, analyzer.getQualifiedUser(), catalogName);
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SWITCH;
    }
}
