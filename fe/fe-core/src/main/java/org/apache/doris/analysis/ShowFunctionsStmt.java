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

public class ShowFunctionsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Signature", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Return Type", ScalarType.createVarchar(32)))
                    .addColumn(new Column("Function Type", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Intermediate Type", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(16)))
                    .build();

    private String dbName;

    private boolean isBuiltin;

    private boolean isVerbose;

    private String wild;

    private Expr expr;

    private SetType type = SetType.DEFAULT;

    public ShowFunctionsStmt(String dbName, boolean isBuiltin, boolean isVerbose, String wild,
            Expr expr) {
        this.dbName = dbName;
        this.isBuiltin = isBuiltin;
        this.isVerbose = isVerbose;
        this.wild = wild;
        this.expr = expr;
    }

    public ShowFunctionsStmt(boolean isVerbose, String wild, Expr expr) {
        this.type = SetType.GLOBAL;
        this.isVerbose = isVerbose;
        this.wild = wild;
        this.expr = expr;
    }

    public String getDbName() {
        return dbName;
    }

    public boolean getIsBuiltin() {
        return isBuiltin;
    }

    public boolean getIsVerbose() {
        return isVerbose;
    }

    public String getWild() {
        return wild;
    }

    public Expr getExpr() {
        return expr;
    }

    public SetType getType() {
        return type;
    }

    public boolean like(String str) {
        str = str.toLowerCase();
        return str.matches(wild.replace(".", "\\.").replace("?", ".").replace("%", ".*").toLowerCase());
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (!FunctionUtil.isGlobalFunction(this.type)) {
            this.dbName = FunctionUtil.reAcquireDbName(analyzer, dbName);
        }

        if (!FunctionUtil.isGlobalFunction(this.type) && !Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, ConnectContext.get().getQualifiedUser(), dbName);
        }

        if (expr != null) {
            throw new AnalysisException("Only support like 'function_pattern' syntax.");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }


    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ");
        if (FunctionUtil.isGlobalFunction(this.type)) {
            sb.append("GLOBAL ");
        }
        if (isVerbose) {
            sb.append("FULL ");
        }
        if (isBuiltin) {
            sb.append("BUILTIN ");
        }
        sb.append("FUNCTIONS FROM ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("`").append(dbName).append("` ");
        }
        if (wild != null) {
            sb.append("LIKE ").append("`").append(wild).append("`");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

}
