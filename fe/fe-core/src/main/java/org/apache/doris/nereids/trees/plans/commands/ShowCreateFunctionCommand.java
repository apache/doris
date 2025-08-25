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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.catalog.FunctionUtil;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.FunctionArgTypesInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/**
 * show create function
 */
public class ShowCreateFunctionCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("Function Signature", ScalarType.createVarchar(512)))
                .addColumn(new Column("Create Function", ScalarType.createVarchar(2048)))
                .build();
    private String dbName;
    private SetType scope;
    private FunctionName functionName;
    private FunctionArgTypesInfo argsDef;
    private FunctionSearchDesc function;

    public ShowCreateFunctionCommand(String dbName, SetType scope, FunctionName functionName,
                                         FunctionArgTypesInfo argsDef) {
        super(PlanType.SHOW_CREATE_FUNCTION_COMMAND);
        this.dbName = dbName;
        this.scope = scope;
        this.functionName = functionName;
        this.argsDef = argsDef;
    }

    private static String reAcquireDbName(ConnectContext ctx, String dbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        return dbName;
    }

    private boolean isValidCharacter(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    @VisibleForTesting
    protected void analyze(ConnectContext ctx, SetType type) throws AnalysisException {
        String fn = functionName.getFunction();
        if (fn.length() == 0) {
            throw new AnalysisException("Function name can not be empty.");
        }
        for (int i = 0; i < fn.length(); ++i) {
            if (!isValidCharacter(fn.charAt(i))) {
                throw new AnalysisException("Function names must be all alphanumeric or underscore. "
                    + "Invalid name: " + fn);
            }
        }
        if (Character.isDigit(fn.charAt(0))) {
            throw new AnalysisException("Function cannot start with a digit: " + fn);
        }
        if (dbName == null) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName) && type != SetType.GLOBAL) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
    }

    private List<List<String>> getResultRowSetByFunction(Function function) {
        if (Objects.isNull(function)) {
            return Lists.newArrayList();
        }
        List<List<String>> resultRowSet = Lists.newArrayList();
        List<String> resultRow = Lists.newArrayList();
        resultRow.add(function.signatureString());
        resultRow.add(function.toSql(false));
        resultRowSet.add(resultRow);
        return resultRowSet;
    }

    private List<List<String>> getResultRowSet(ConnectContext ctx) throws AnalysisException {
        Function resultFunction = getFunction(ctx);
        return getResultRowSetByFunction(resultFunction);
    }

    @VisibleForTesting
    protected Function getFunction(ConnectContext ctx) throws AnalysisException {
        if (!FunctionUtil.isGlobalFunction(scope)) {
            Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), this.getClass().getSimpleName());
            DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);
            if (db instanceof Database) {
                return ((Database) db).getFunction(function);
            }
        } else {
            return Env.getCurrentEnv().getGlobalFunctionMgr().getFunction(function);
        }
        return null;
    }

    @VisibleForTesting
    protected ShowResultSet handleShowCreateFunction(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // the global function does not need to fetch/check the dbName
        if (!FunctionUtil.isGlobalFunction(scope)) {
            dbName = reAcquireDbName(ctx, dbName);
        }

        // analyze function name
        analyze(ctx, scope);

        // check operation privilege , except global function
        if (!FunctionUtil.isGlobalFunction(scope) && !Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, ConnectContext.get().getQualifiedUser(), dbName);
        }

        // check argument
        argsDef.analyze();

        if (!FunctionUtil.isGlobalFunction(scope)) {
            functionName.setDb(dbName);
        }
        function = new FunctionSearchDesc(functionName, argsDef.getArgTypes(), argsDef.isVariadic());

        // get result
        List<List<String>> resultRowSet = getResultRowSet(ctx);
        return new ShowResultSet(getMetaData(), resultRowSet);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowCreateFunction(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateFunctionCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
