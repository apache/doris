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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class LambdaFunctionCallExpr extends FunctionCallExpr {
    public static final ImmutableSet<String> LAMBDA_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER).add("array_map").build();

    private static final Logger LOG = LogManager.getLogger(LambdaFunctionCallExpr.class);

    public LambdaFunctionCallExpr(String functionName, List<Expr> params) {
        super(functionName, params);
    }

    public LambdaFunctionCallExpr(FunctionName functionName, List<Expr> params) {
        super(functionName, params);
    }

    public LambdaFunctionCallExpr(LambdaFunctionCallExpr other) {
        super(other);
    }

    @Override
    public Expr clone() {
        return new LambdaFunctionCallExpr(this);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        FunctionName fnName = getFnName();
        FunctionParams fnParams = getFnParams();
        if (!LAMBDA_FUNCTION_SET.contains(fnName.getFunction().toLowerCase())) {
            throw new AnalysisException(
                    "Function {} maybe not in the LAMBDA_FUNCTION_SET, should check the implement" + fnName
                            .getFunction());
        }

        int childSize = this.children.size();
        Type[] argTypes = new Type[childSize];
        for (int i = 0; i < childSize; ++i) {
            this.children.get(i).analyze(analyzer);
            argTypes[i] = this.children.get(i).getType();
        }

        if (fnName.getFunction().equalsIgnoreCase("array_map")) {
            if (fnParams.exprs() == null || fnParams.exprs().size() < 2) {
                throw new AnalysisException("The " + fnName.getFunction() + " function must have at least two params");
            }

            // change the lambda expr to the first args position
            if (getChild(childSize - 1) instanceof LambdaFunctionExpr) {
                Type lastType = argTypes[childSize - 1];
                Expr lastChild = getChild(childSize - 1);
                for (int i = childSize - 1; i > 0; --i) {
                    argTypes[i] = getChild(i - 1).getType();
                    this.setChild(i, getChild(i - 1));
                }
                argTypes[0] = lastType;
                this.setChild(0, lastChild);
            }

            fn = getBuiltinFunction(fnName.getFunction(), argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            Expr lambda = this.children.get(0);
            if (fn == null) {
                LOG.warn("fn {} not exists", this.toSqlImpl());
                throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
            }
            fn.setReturnType(ArrayType.create(lambda.getChild(0).getType(), true));
        }
        this.type = fn.getReturnType();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.LAMBDA_FUNCTION_CALL_EXPR;
    }
}
