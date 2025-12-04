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

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

public class LambdaFunctionCallExpr extends FunctionCallExpr {

    private LambdaFunctionCallExpr() {
        // use for serde only
    }

    public LambdaFunctionCallExpr(LambdaFunctionCallExpr other) {
        super(other);
    }

    // nereids high order function call expr constructor without finalize/analyze
    public LambdaFunctionCallExpr(Function function, FunctionParams functionParams) {
        super(function, functionParams, null, false, functionParams.exprs());
    }

    @Override
    public Expr clone() {
        return new LambdaFunctionCallExpr(this);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.LAMBDA_FUNCTION_CALL_EXPR;
    }

    @Override
    public String toSqlImpl() {
        StringBuilder sb = new StringBuilder();

        String fnName = getFnName().getFunction();
        if (fn != null) {
            // `array_last` will be replaced with `element_at` function after analysis.
            // At this moment, using the name `array_last` would generate invalid SQL.
            fnName = fn.getFunctionName().getFunction();
        }
        sb.append(fnName);
        sb.append("(");
        int childSize = children.size();
        Expr lastExpr = getChild(childSize - 1);
        // eg: select array_map(x->x>10, k1) from table,
        // but we need analyze each param, so change the function like this in parser
        // array_map(x->x>10, k1) ---> array_map(k1, x->x>10),
        // so maybe the lambda expr is the end position. and need this check.
        boolean lastIsLambdaExpr = (lastExpr instanceof LambdaFunctionExpr);
        if (lastIsLambdaExpr) {
            sb.append(lastExpr.toSql());
            sb.append(", ");
        }
        for (int i = 0; i < childSize - 1; ++i) {
            sb.append(getChild(i).toSql());
            if (i != childSize - 2) {
                sb.append(", ");
            }
        }
        // and some functions is only implement as a normal array function;
        // but also want use as lambda function, select array_sortby(x->x,['b','a','c']);
        // so we convert to: array_sortby(array('b', 'a', 'c'), array_map(x -> `x`, array('b', 'a', 'c')))
        if (!lastIsLambdaExpr) {
            if (childSize > 1) {
                // some functions don't have lambda expr, so don't need to add ","
                // such as array_exists(array_map(x->x>3, [1,2,3,6,34,3,11]))
                sb.append(", ");
            }
            sb.append(lastExpr.toSql());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        StringBuilder sb = new StringBuilder();

        String fnName = getFnName().getFunction();
        if (fn != null) {
            // `array_last` will be replaced with `element_at` function after analysis.
            // At this moment, using the name `array_last` would generate invalid SQL.
            fnName = fn.getFunctionName().getFunction();
        }
        sb.append(fnName);
        sb.append("(");
        int childSize = children.size();
        Expr lastExpr = getChild(childSize - 1);
        // eg: select array_map(x->x>10, k1) from table,
        // but we need analyze each param, so change the function like this in parser
        // array_map(x->x>10, k1) ---> array_map(k1, x->x>10),
        // so maybe the lambda expr is the end position. and need this check.
        boolean lastIsLambdaExpr = (lastExpr instanceof LambdaFunctionExpr);
        if (lastIsLambdaExpr) {
            sb.append(lastExpr.toSql(disableTableName, needExternalSql, tableType, table));
            sb.append(", ");
        }
        for (int i = 0; i < childSize - 1; ++i) {
            sb.append(getChild(i).toSql(disableTableName, needExternalSql, tableType, table));
            if (i != childSize - 2) {
                sb.append(", ");
            }
        }
        // and some functions is only implement as a normal array function;
        // but also want use as lambda function, select array_sortby(x->x,['b','a','c']);
        // so we convert to: array_sortby(array('b', 'a', 'c'), array_map(x -> `x`, array('b', 'a', 'c')))
        if (!lastIsLambdaExpr) {
            if (childSize > 1) {
                // some functions don't have lambda expr, so don't need to add ","
                // such as array_exists(array_map(x->x>3, [1,2,3,6,34,3,11]))
                sb.append(", ");
            }
            sb.append(lastExpr.toSql(disableTableName, needExternalSql, tableType, table));
        }
        sb.append(")");
        return sb.toString();
    }
}
