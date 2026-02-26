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
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionUtil;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    private boolean isVerbose;
    private String likeCondition;
    private SetType type = SetType.DEFAULT;

    /**
     * constructor
     */
    public ShowFunctionsCommand(String dbName, boolean isVerbose, String likeCondition) {
        super(PlanType.SHOW_FUNCTIONS_COMMAND);
        this.dbName = dbName;
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
    private List<Comparable> getInfo(boolean isVerbose, Function function) {
        List<Comparable> row = Lists.newArrayList();
        if (isVerbose) {
            // signature
            row.add(function.signatureString());
            // return type
            row.add(function.getReturnType().toString());
            // function type
            // intermediate type
            row.add(buildFunctionType(function));
            row.add(buildIntermediateType(function));
            // property
            row.add(buildProperties(function));
        } else {
            row.add(function.functionName());
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
    protected List<List<String>> getResultRowSetByFunctions(List<Function> functions) {
        List<List<String>> resultRowSet = Lists.newArrayList();
        List<List<Comparable>> rowSet = Lists.newArrayList();
        for (Function function : functions) {
            List<Comparable> row = getInfo(isVerbose, function);
            // like predicate
            if (likeCondition == null || like(function.functionName(), likeCondition)) {
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
        List<Function> functions = getFunctions(ctx);
        return getResultRowSetByFunctions(functions);
    }

    /***
     * get functions by nereids.
     * All functions including builtin and udf are registered in FunctionRegistry
     */
    @VisibleForTesting
    protected List<Function> getFunctions(ConnectContext ctx) throws AnalysisException {
        List<Function> functions = Lists.newArrayList();
        if (ctx == null || ctx.getEnv() == null || ctx.getEnv().getFunctionRegistry() == null) {
            return functions;
        }

        if (!FunctionUtil.isGlobalFunction(type)) {
            dbName = reAcquireDbName(ctx, dbName);
        }

        if (FunctionUtil.isGlobalFunction(type)) {
            functions = Env.getCurrentEnv().getGlobalFunctionMgr().getFunctions();
        } else {
            Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), this.getClass().getSimpleName());
            DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);
            if (db instanceof Database) {
                functions = ((Database) db).getFunctions();
            }
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

        if (!FunctionUtil.isGlobalFunction(type)) {
            if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                    InternalCatalog.INTERNAL_CATALOG_NAME, dbName, PrivPredicate.SELECT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR,
                        PrivPredicate.SELECT.getPrivs().toString(), dbName);
            }
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

    private String buildFunctionType(Function function) {
        if (function instanceof AggregateFunction) {
            return "AGGREGATE/" + function.getBinaryType();
        }
        if (function.isUDTFunction()) {
            return "UDTF/" + function.getBinaryType();
        }
        if (function instanceof AliasFunction) {
            return "ALIAS/" + function.getBinaryType();
        }
        return "SCALAR/" + function.getBinaryType();
    }

    private String buildIntermediateType(Function function) {
        if (function instanceof AggregateFunction) {
            AggregateFunction aggregateFunction = (AggregateFunction) function;
            return aggregateFunction.getIntermediateType() == null
                    ? ""
                    : aggregateFunction.getIntermediateType().toString();
        }
        return "";
    }

    private String buildProperties(Function function) {
        Map<String, String> properties = new LinkedHashMap<>();
        if (function.getId() > 0) {
            properties.put("ID", String.valueOf(function.getId()));
        }
        if (!Strings.isNullOrEmpty(function.getChecksum())) {
            properties.put("CHECKSUM", function.getChecksum());
        }
        if (function.getLocation() != null) {
            String locationKey = function.getBinaryType() == null
                    || function.getBinaryType().name().startsWith("JAVA")
                            ? "FILE"
                            : "OBJECT_FILE";
            properties.put(locationKey, function.getLocation().toString());
        }
        properties.put("NULLABLE_MODE", function.getNullableMode().name());
        if (function.isStaticLoad()) {
            properties.put("STATIC_LOAD", String.valueOf(function.isStaticLoad()));
        }
        if (!Strings.isNullOrEmpty(function.getRuntimeVersion())) {
            properties.put("RUNTIME_VERSION", function.getRuntimeVersion());
        }

        if (function instanceof ScalarFunction) {
            ScalarFunction scalarFunction = (ScalarFunction) function;
            properties.put("SYMBOL", Strings.nullToEmpty(scalarFunction.getSymbolName()));
            if (scalarFunction.getPrepareFnSymbol() != null) {
                properties.put("PREPARE_FN", scalarFunction.getPrepareFnSymbol());
            }
            if (scalarFunction.getCloseFnSymbol() != null) {
                properties.put("CLOSE_FN", scalarFunction.getCloseFnSymbol());
            }
        }

        if (function instanceof AggregateFunction) {
            AggregateFunction aggregateFunction = (AggregateFunction) function;
            properties.put("INIT_FN", Strings.nullToEmpty(aggregateFunction.getInitFnSymbol()));
            properties.put("UPDATE_FN", Strings.nullToEmpty(aggregateFunction.getUpdateFnSymbol()));
            properties.put("MERGE_FN", Strings.nullToEmpty(aggregateFunction.getMergeFnSymbol()));
            if (aggregateFunction.getSerializeFnSymbol() != null) {
                properties.put("SERIALIZE_FN", aggregateFunction.getSerializeFnSymbol());
            }
            if (aggregateFunction.getFinalizeFnSymbol() != null) {
                properties.put("FINALIZE_FN", aggregateFunction.getFinalizeFnSymbol());
            }
            if (aggregateFunction.getGetValueFnSymbol() != null) {
                properties.put("GET_VALUE_FN", aggregateFunction.getGetValueFnSymbol());
            }
            if (aggregateFunction.getRemoveFnSymbol() != null) {
                properties.put("REMOVE_FN", aggregateFunction.getRemoveFnSymbol());
            }
            if (aggregateFunction.getSymbolName() != null) {
                properties.put("SYMBOL", aggregateFunction.getSymbolName());
            }
        }

        if (function instanceof AliasFunction) {
            AliasFunction aliasFunction = (AliasFunction) function;
            properties.put("ALIAS_OF", aliasFunction.getOriginFunction().toSqlWithoutTbl());
            if (aliasFunction.getParameters() != null && !aliasFunction.getParameters().isEmpty()) {
                properties.put("PARAMETERS", String.join(",", aliasFunction.getParameters()));
            }
        }

        // inline python UDF/UDTF/UDAF code
        if (function.getBinaryType() != null && function.getBinaryType().name().contains("PYTHON")
                && !Strings.isNullOrEmpty(function.getFunctionCode())) {
            properties.put("INLINE_CODE", function.getFunctionCode());
        }

        return properties.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", "));
    }

}
