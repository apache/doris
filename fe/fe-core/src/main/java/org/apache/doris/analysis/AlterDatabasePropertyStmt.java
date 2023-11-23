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
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.Map;

public class AlterDatabasePropertyStmt extends DdlStmt {
    private String dbName;
    private Map<String, String> properties;

    public AlterDatabasePropertyStmt(String dbName, Map<String, String> properties) {
        this.dbName = dbName;
        this.properties = properties;
    }

    public String getDbName() {
        return dbName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    analyzer.getQualifiedUser(), dbName);
        }

        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }
        dbName = ClusterNamespace.getFullName(getClusterName(), dbName);

        if (properties == null || properties.isEmpty()) {
            throw new UserException("Properties is null or empty");
        }

        // clone properties for analyse
        Map<String, String> analysisProperties = new HashMap<String, String>(properties);
        PropertyAnalyzer.analyzeBinlogConfig(analysisProperties);
    }

    @Override
    public String toSql() {
        return "ALTER DATABASE " + dbName + " SET PROPERTIES ("
                + new PrintableMap<String, String>(properties, "=", true, false, ",") + ")";
    }
}
