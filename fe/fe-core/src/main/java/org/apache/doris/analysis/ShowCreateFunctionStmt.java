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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.catalog.FunctionUtil;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

public class ShowCreateFunctionStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Function Signature", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Create Function", ScalarType.createVarchar(1024)))
                    .build();
    private String dbName;
    private final FunctionName functionName;
    private final FunctionArgsDef argsDef;
    private SetType type = SetType.DEFAULT;

    // set after analyzed
    private FunctionSearchDesc function;

    public ShowCreateFunctionStmt(SetType type, String dbName, FunctionName functionName, FunctionArgsDef argsDef) {
        this.type = type;
        this.dbName = dbName;
        this.functionName = functionName;
        this.argsDef = argsDef;
    }

    public String getDbName() {
        return dbName;
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public FunctionSearchDesc getFunction() {
        return function;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // the global function does not need to fetch/check the dbName
        if (!FunctionUtil.isGlobalFunction(this.type)) {
            dbName = FunctionUtil.reAcquireDbName(analyzer, dbName);
        }

        // analyze function name
        functionName.analyze(analyzer, this.type);

        // check operation privilege , except global function
        if (!FunctionUtil.isGlobalFunction(this.type) && !Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, ConnectContext.get().getQualifiedUser(), dbName);
        }
        // analyze arguments
        argsDef.analyze(analyzer);
        function = new FunctionSearchDesc(functionName, argsDef.getArgTypes(), argsDef.isVariadic());
    }


    /***
     * reAcquire dbName and check "No database selected"
     * @param analyzer
     * @throws AnalysisException
     */
    private void reAcquireDbName(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        if (FunctionUtil.isGlobalFunction(this.type)) {
            stringBuilder.append("SHOW CREATE GLOBAL FUNCTION ").append(functionName).append(argsDef);
        } else {
            stringBuilder.append("SHOW CREATE FUNCTION ").append(functionName).append(argsDef)
                    .append(" IN ").append(dbName);
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public SetType getType() {
        return type;
    }
}
