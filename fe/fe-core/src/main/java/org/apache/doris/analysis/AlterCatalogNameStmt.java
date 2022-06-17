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


import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalDataSource;
import org.apache.doris.mysql.privilege.PaloPrivilege;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

/**
 * Statement for alter the catalog name.
 */
public class AlterCatalogNameStmt extends DdlStmt {
    private final String catalogName;
    private final String newCatalogName;

    public AlterCatalogNameStmt(String catalogName, String newCatalogName) {
        this.catalogName = catalogName;
        this.newCatalogName = newCatalogName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getNewCatalogName() {
        return newCatalogName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (!Config.enable_multi_catalog) {
            throw new AnalysisException("The multi-catalog feature is still in experiment, and you can enable it "
                    + "manually by set fe configuration named `enable_multi_catalog` to be ture.");
        }
        if (Strings.isNullOrEmpty(catalogName)) {
            throw new AnalysisException("Datasource name is not set");
        }

        if (catalogName.equals(InternalDataSource.INTERNAL_DS_NAME)) {
            throw new AnalysisException("Internal catalog can't be alter.");
        }

        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(),
                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ADMIN_PRIV, PaloPrivilege.ALTER_PRIV), Operator.OR))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    analyzer.getQualifiedUser(), catalogName);
        }

        if (Strings.isNullOrEmpty(newCatalogName)) {
            throw new AnalysisException("New catalog name is not set");
        }
        if (newCatalogName.equals(InternalDataSource.INTERNAL_DS_NAME)) {
            throw new AnalysisException("Cannot alter a catalog into a build-in name.");
        }
        FeNameFormat.checkCommonName("catalog", newCatalogName);
    }

    @Override
    public String toSql() {
        return "ALTER CATALOG " + catalogName + " RENAME " + newCatalogName;
    }
}
