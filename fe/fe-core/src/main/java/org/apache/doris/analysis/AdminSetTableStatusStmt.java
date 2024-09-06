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
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import lombok.Getter;

import java.util.Map;

public class AdminSetTableStatusStmt extends DdlStmt implements NotFallbackInParser {

    public static final String TABLE_STATE = "state";

    private final TableName tableName;
    private final Map<String, String> properties;
    @Getter
    private OlapTableState tableState;

    public AdminSetTableStatusStmt(TableName tableName, Map<String, String> properties) {
        this.tableName = tableName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        tableName.analyze(analyzer);
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

        checkProperties();
    }

    private void checkProperties() throws AnalysisException {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();

            if (key.equalsIgnoreCase(TABLE_STATE)) {
                try {
                    tableState = OlapTableState.valueOf(val.toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw new AnalysisException("Invalid table state: " + val);
                }
            } else {
                throw new AnalysisException("Unsupported property: " + key);
            }
        }

        if (tableState == null) {
            throw new AnalysisException("Should add properties: STATE.");
        }
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTblName() {
        return tableName.getTbl();
    }
}
