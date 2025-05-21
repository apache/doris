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

import org.apache.doris.analysis.SetType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.FunctionUtil;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * show functions command
 */
public class ShowFunctionsCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("Signature", ScalarType.STRING))
                .addColumn(new Column("Return Type", ScalarType.STRING))
                .addColumn(new Column("Function Type", ScalarType.STRING))
                .addColumn(new Column("Intermediate Type", ScalarType.STRING))
                .addColumn(new Column("Properties", ScalarType.STRING))
                .build();

    private String dbName;
    private boolean isBuiltin;
    private boolean isVerbose;
    private String likeCondition;
    private SetType type = SetType.DEFAULT;

    /**
     * constructor
     */
    public ShowFunctionsCommand(String dbName, boolean isBuiltin, boolean isVerbose, String likeCondition) {
        super(PlanType.SHOW_FUNCTIONS_COMMAND);
        this.dbName = dbName;
        this.isBuiltin = isBuiltin;
        this.isVerbose = isVerbose;
        this.likeCondition = likeCondition;
    }

    /**
     * constructor for global functions
     */
    public ShowFunctionsCommand(boolean isVerbose, String likeCondition, boolean isGlobal) {
        super(PlanType.SHOW_GLOBAL_FUNCTIONS_COMMAND);
        this.isVerbose = isVerbose;
        this.likeCondition = likeCondition;
        if (isGlobal) {
            this.type = SetType.GLOBAL;
        }
    }

    /***
     * get Info by nereids.
     * To make the code in nereids more concise, all irrelevant information here will use an empty string.
     */
    private List<Comparable> getInfo(boolean isVerbose, String funcName) {
        List<Comparable> row = Lists.newArrayList();
        if (isVerbose) {
            // signature
            row.add(funcName);
            // return type
            row.add("");
            // function type
            // intermediate type
            row.add("");
            row.add("");
            // property
            row.add("");
        } else {
            row.add(funcName);
        }
        return row;
    }

    @VisibleForTesting
    protected boolean like(String funcName, String likeCondition) {
        funcName = funcName.toLowerCase();
        return funcName.matches(likeCondition.replace(".", "\\.").replace("?", ".").replace("%", ".*").toLowerCase());
    }

    /***
     * get resultRowSet
     */
    @VisibleForTesting
    protected List<List<String>> getResultRowSetByFunctions(List<String> functions) {
        List<List<String>> resultRowSet = Lists.newArrayList();
        List<List<Comparable>> rowSet = Lists.newArrayList();
        for (String function : functions) {
            List<Comparable> row = getInfo(isVerbose, function);
            // like predicate
            if (likeCondition == null || like(function, likeCondition)) {
                rowSet.add(row);
            }
        }

        // sort function rows by first column asc
        ListComparator<List<Comparable>> comparator = null;
        OrderByPair orderByPair = new OrderByPair(0, false);
        comparator = new ListComparator<>(orderByPair);
        Collections.sort(rowSet, comparator);

        Set<String> functionNameSet = new HashSet<>();
        for (List<Comparable> row : rowSet) {
            List<String> resultRow = Lists.newArrayList();
            // if not verbose, remove duplicate function name
            if (functionNameSet.contains(row.get(0).toString())) {
                continue;
            }
            for (Comparable column : row) {
                resultRow.add(column.toString());
            }
            resultRowSet.add(resultRow);
            functionNameSet.add(resultRow.get(0));
        }
        return resultRowSet;
    }

    /***
     * get resultRowSet
     */
    private List<List<String>> getResultRowSet(ConnectContext ctx) throws AnalysisException {
        List<String> functions = getFunctions(ctx);
        return getResultRowSetByFunctions(functions);
    }

    /***
     * get functions by nereids.
     * All functions including builtin and udf are registered in FunctionRegistry
     */
    @VisibleForTesting
    protected List<String> getFunctions(ConnectContext ctx) throws AnalysisException {
        List<String> functions = Lists.newArrayList();
        if (ctx == null || ctx.getEnv() == null || ctx.getEnv().getFunctionRegistry() == null) {
            return functions;
        }

        FunctionRegistry functionRegistry = ctx.getEnv().getFunctionRegistry();
        Map<String, Map<String, List<FunctionBuilder>>> udfFunctions = functionRegistry.getName2UdfBuilders();
        if (!FunctionUtil.isGlobalFunction(type)) {
            Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), this.getClass().getSimpleName());
            DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);
            if (db instanceof Database) {
                Map<String, List<FunctionBuilder>> builtinFunctions = functionRegistry.getName2BuiltinBuilders();
                functions = isBuiltin ? new ArrayList<>(builtinFunctions.keySet()) :
                        new ArrayList<>(udfFunctions.getOrDefault(dbName, new HashMap<>()).keySet());
            }
        } else {
            functions = new ArrayList<>(udfFunctions
                .getOrDefault(functionRegistry.getGlobalFunctionDbName(), new HashMap<>()).keySet());
        }
        return functions;
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

    public ShowResultSetMetaData getMetaData() {
        if (isVerbose) {
            return META_DATA;
        }
        return ShowResultSetMetaData.builder().addColumn(new Column("Function Name", ScalarType.STRING)).build();
    }

    /**
     * handle show functions
     */
    @VisibleForTesting
    protected ShowResultSet handleShowFunctions(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!FunctionUtil.isGlobalFunction(type)) {
            this.dbName = reAcquireDbName(ctx, dbName);
        }

        if (!FunctionUtil.isGlobalFunction(type) && !Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    ConnectContext.get().getQualifiedUser(), dbName);
        }

        List<List<String>> resultRowSet = getResultRowSet(ctx);
        // Only success
        ShowResultSetMetaData showMetaData = getMetaData();
        return new ShowResultSet(showMetaData, resultRowSet);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowFunctions(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowFunctionsCommand(this, context);
    }

}
