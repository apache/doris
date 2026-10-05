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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
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
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * show builtin functions command
 */
public class ShowBuiltinFunctionsCommand extends ShowCommand {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("Signature", ScalarType.STRING))
                .addColumn(new Column("Return Type", ScalarType.STRING))
                .addColumn(new Column("Function Type", ScalarType.STRING))
                .addColumn(new Column("Intermediate Type", ScalarType.STRING))
                .addColumn(new Column("Properties", ScalarType.STRING))
                .build();
    private boolean isVerbose;
    private String likeCondition;

    public ShowBuiltinFunctionsCommand(boolean isVerbose, String likeCondition) {
        super(PlanType.SHOW_BUILTIN_FUNCTIONS_COMMAND);
        this.isVerbose = isVerbose;
        this.likeCondition = likeCondition;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (isVerbose) {
            return META_DATA;
        }
        return ShowResultSetMetaData.builder().addColumn(new Column("Function Name", ScalarType.STRING)).build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowBuiltinFunctions(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowBuiltinFunctionsCommand(this, context);
    }

    /**
     * handle show builtin functions
     */
    @VisibleForTesting
    protected ShowResultSet handleShowBuiltinFunctions(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // show builtin functions only need information_schema priv
        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                InternalCatalog.INTERNAL_CATALOG_NAME, InfoSchemaDb.DATABASE_NAME, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR,
                    PrivPredicate.SELECT.getPrivs().toString(), InfoSchemaDb.DATABASE_NAME);
        }
        List<List<String>> resultRowSet = getResultRowSet(ctx);
        ShowResultSetMetaData showMetaData = getMetaData();
        return new ShowResultSet(showMetaData, resultRowSet);
    }

    /***
     * get resultRowSet
     */
    private List<List<String>> getResultRowSet(ConnectContext ctx) throws AnalysisException {
        List<String> functions = getFunctions(ctx);
        return getResultRowSetByFunctions(functions);
    }

    @VisibleForTesting
    protected List<String> getFunctions(ConnectContext ctx) throws AnalysisException {
        List<String> functions = Lists.newArrayList();
        if (ctx == null || ctx.getEnv() == null || ctx.getEnv().getFunctionRegistry() == null) {
            return functions;
        }
        FunctionRegistry functionRegistry = ctx.getEnv().getFunctionRegistry();
        Map<String, List<FunctionBuilder>> builtinFunctions = functionRegistry.getName2BuiltinBuilders();
        functions = new ArrayList<>(builtinFunctions.keySet());
        return functions;
    }

    @VisibleForTesting
    protected List<List<String>> getResultRowSetByFunctions(List<String> functions) {
        List<List<String>> resultRowSet = Lists.newArrayList();
        List<List<Comparable>> rowSet = Lists.newArrayList();
        for (String function : functions) {
            List<Comparable> row = Lists.newArrayList();
            if (isVerbose) {
                // signature
                row.add(function);
                // return type
                row.add("");
                // function type
                row.add("");
                // intermediate type
                row.add("");
                // property
                row.add("");
            } else {
                row.add(function);
            }
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

    @VisibleForTesting
    protected boolean like(String funcName, String likeCondition) {
        funcName = funcName.toLowerCase();
        return funcName.matches(likeCondition.replace(".", "\\.").replace("?", ".").replace("%", ".*").toLowerCase());
    }

}
