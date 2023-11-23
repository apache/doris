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

package org.apache.doris.planner.external.jdbc;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.thrift.TOdbcTableType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Predicate;

public class JdbcFunctionPushDownRule {
    private static final Logger LOG = LogManager.getLogger(JdbcFunctionPushDownRule.class);
    private static final TreeSet<String> MYSQL_UNSUPPORTED_FUNCTIONS = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        MYSQL_UNSUPPORTED_FUNCTIONS.add("date_trunc");
        MYSQL_UNSUPPORTED_FUNCTIONS.add("money_format");
    }

    private static final TreeSet<String> CLICKHOUSE_SUPPORTED_FUNCTIONS = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        CLICKHOUSE_SUPPORTED_FUNCTIONS.add("from_unixtime");
        CLICKHOUSE_SUPPORTED_FUNCTIONS.add("unix_timestamp");
    }

    private static boolean isMySQLFunctionUnsupported(String functionName) {
        return MYSQL_UNSUPPORTED_FUNCTIONS.contains(functionName.toLowerCase());
    }

    private static boolean isClickHouseFunctionUnsupported(String functionName) {
        return !CLICKHOUSE_SUPPORTED_FUNCTIONS.contains(functionName.toLowerCase());
    }


    private static final Map<String, String> REPLACE_MYSQL_FUNCTIONS = Maps.newHashMap();

    static {
        REPLACE_MYSQL_FUNCTIONS.put("nvl", "ifnull");
    }

    private static boolean isReplaceMysqlFunctions(String functionName) {
        return REPLACE_MYSQL_FUNCTIONS.containsKey(functionName.toLowerCase());
    }

    private static final Map<String, String> REPLACE_CLICKHOUSE_FUNCTIONS = Maps.newHashMap();

    static {
        REPLACE_CLICKHOUSE_FUNCTIONS.put("from_unixtime", "FROM_UNIXTIME");
        REPLACE_CLICKHOUSE_FUNCTIONS.put("unix_timestamp", "toUnixTimestamp");
    }

    private static boolean isReplaceClickHouseFunctions(String functionName) {
        return REPLACE_CLICKHOUSE_FUNCTIONS.containsKey(functionName.toLowerCase());
    }

    public static Expr processFunctions(TOdbcTableType tableType, Expr expr, List<String> errors) {
        if (tableType == null || expr == null) {
            return expr;
        }

        Predicate<String> checkFunction;
        Predicate<String> replaceFunction;

        if (TOdbcTableType.MYSQL.equals(tableType)) {
            replaceFunction = JdbcFunctionPushDownRule::isReplaceMysqlFunctions;
            checkFunction = JdbcFunctionPushDownRule::isMySQLFunctionUnsupported;
        } else if (TOdbcTableType.CLICKHOUSE.equals(tableType)) {
            replaceFunction = JdbcFunctionPushDownRule::isReplaceClickHouseFunctions;
            checkFunction = JdbcFunctionPushDownRule::isClickHouseFunctionUnsupported;
        } else {
            return expr;
        }

        return processFunctionsRecursively(expr, checkFunction, replaceFunction, errors, tableType);
    }

    private static Expr processFunctionsRecursively(Expr expr, Predicate<String> checkFunction,
            Predicate<String> replaceFunction, List<String> errors, TOdbcTableType tableType) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            String func = functionCallExpr.getFnName().getFunction();

            Preconditions.checkArgument(!func.isEmpty(), "function can not be empty");

            if (checkFunction.test(func)) {
                String errMsg = "Unsupported function: " + func + " in expr: " + expr.toMySql()
                        + " in JDBC Table Type: " + tableType;
                LOG.warn(errMsg);
                errors.add(errMsg);
            }

            replaceFunctionNameIfNecessary(func, replaceFunction, functionCallExpr, tableType);
        }

        List<Expr> children = expr.getChildren();
        for (int i = 0; i < children.size(); i++) {
            Expr child = children.get(i);
            Expr newChild = processFunctionsRecursively(child, checkFunction, replaceFunction, errors, tableType);
            expr.setChild(i, newChild);
        }

        return expr;
    }

    private static String replaceFunctionNameIfNecessary(String func, Predicate<String> replaceFunction,
            FunctionCallExpr functionCallExpr, TOdbcTableType tableType) {
        if (replaceFunction.test(func)) {
            String newFunc;
            if (TOdbcTableType.MYSQL.equals(tableType)) {
                newFunc = REPLACE_MYSQL_FUNCTIONS.get(func.toLowerCase());
            } else if (TOdbcTableType.CLICKHOUSE.equals(tableType)) {
                newFunc = REPLACE_CLICKHOUSE_FUNCTIONS.get(func);
            } else {
                newFunc = null;
            }
            if (newFunc != null) {
                functionCallExpr.setFnName(FunctionName.createBuiltinName(newFunc));
                func = functionCallExpr.getFnName().getFunction();
            }
        }
        return func;
    }
}
