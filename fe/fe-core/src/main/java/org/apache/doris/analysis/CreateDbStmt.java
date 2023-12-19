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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.HashMap;
import java.util.Map;

public class CreateDbStmt extends DdlStmt {
    private boolean ifNotExists;
    private String dbName;
    private Map<String, String> properties;

    public CreateDbStmt(boolean ifNotExists, String dbName, Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.dbName = dbName;
        this.properties = properties == null ? new HashMap<>() : properties;
    }

    public String getFullDbName() {
        return dbName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        FeNameFormat.checkDbName(dbName);

        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, analyzer.getQualifiedUser(), dbName);
        }
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE DATABASE ").append("`").append(dbName).append("`");
        if (properties.size() > 0) {
            stringBuilder.append("\nPROPERTIES (\n");
            stringBuilder.append(new PrintableMap<>(properties, "=", true, true, false));
            stringBuilder.append("\n)");
        }
        return stringBuilder.toString();
    }
}
