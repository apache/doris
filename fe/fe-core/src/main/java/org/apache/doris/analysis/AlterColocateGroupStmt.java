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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Map;

public class AlterColocateGroupStmt extends DdlStmt implements NotFallbackInParser {
    private final ColocateGroupName colocateGroupName;
    private final Map<String, String> properties;

    public AlterColocateGroupStmt(ColocateGroupName colocateGroupName, Map<String, String> properties) {
        this.colocateGroupName = colocateGroupName;
        this.properties = properties;
    }

    public ColocateGroupName getColocateGroupName() {
        return colocateGroupName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze() throws UserException {
        super.analyze();
        colocateGroupName.analyze();

        String dbName = colocateGroupName.getDb();
        if (Strings.isNullOrEmpty(dbName)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(
                        ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
        } else {
            if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(
                    ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName, PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                        ConnectContext.get().getQualifiedUser(), dbName);
            }
        }

        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Colocate group properties can't be null");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER COLOCATE GROUP ").append(colocateGroupName.toSql()).append(" ");
        sb.append("PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false)).append(")");
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}
