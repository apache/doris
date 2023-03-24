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

import java.util.ArrayList;
import java.util.List;

public class LambdaFunctionCallExpr extends FunctionCallExpr {
    public static final ImmutableSet<String> LAMBDA_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER).add("array_map").add("array_filter").add("array_exists").add("array_sortby")
            .build();
    // The functions in this set are all normal array functions when implemented initially.
    // and then wants add lambda expr as the input param, so we rewrite it to contains an array_map lambda function
    // rather than reimplementing a lambda function, this will be reused the implementation of normal array function
    public static final ImmutableSet<String> LAMBDA_MAPPED_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER).add("array_exists").add("array_sortby").build();

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
            // we always put the lambda expr at last position during the parser to get param type first,
            // so here need change the lambda expr to the first args position for BE.
            // array_map(x->x>1,[1,2,3]) ---> array_map([1,2,3], x->x>1) --->
            // array_map(x->x>1, [1,2,3])
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
        } else if (fnName.getFunction().equalsIgnoreCase("array_exists")) {
            if (fnParams.exprs() == null || fnParams.exprs().size() < 1) {
                throw new AnalysisException("The " + fnName.getFunction() + " function must have at least one param");
            }
            // array_exists(x->x>3, [1,2,3,6,34,3,11])
            // ---> array_exists(array_map(x->x>3, [1,2,3,6,34,3,11]))
            Type[] newArgTypes = new Type[1];
            if (getChild(childSize - 1) instanceof LambdaFunctionExpr) {
                List<Expr> params = new ArrayList<>();
                for (int i = 0; i <= childSize - 1; ++i) {
                    params.add(getChild(i));
                }
                LambdaFunctionCallExpr arrayMapFunc = new LambdaFunctionCallExpr("array_map",
                        params);
                arrayMapFunc.analyzeImpl(analyzer);
                Expr castExpr = arrayMapFunc.castTo(ArrayType.create(Type.BOOLEAN, true));
                this.clearChildren();
                this.addChild(castExpr);
                newArgTypes[0] = castExpr.getType();
            }

            if (!(getChild(0) instanceof CastExpr)) {
                Expr castExpr = getChild(0).castTo(ArrayType.create(Type.BOOLEAN, true));
                this.setChild(0, castExpr);
                newArgTypes[0] = castExpr.getType();
            }

            fn = getBuiltinFunction(fnName.getFunction(), newArgTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (fn == null) {
                LOG.warn("fn {} not exists", this.toSqlImpl());
                throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
            }
            fn.setReturnType(getChild(0).getType());
        } else if (fnName.getFunction().equalsIgnoreCase("array_filter")) {
            if (fnParams.exprs() == null || fnParams.exprs().size() != 2) {
                throw new AnalysisException("The " + fnName.getFunction() + " function must have two params");
            }
            /*
             * array_filter(x->x>3, [1,2,3,6,34,3,11]) --->
             * array_filter([1,2,3,6,34,3,11],x->x>3)
             * ---> array_filter([1,2,3,6,34,3,11], array_map(x->x>3, [1,2,3,6,34,3,11]))
             */
            if (getChild(1) instanceof LambdaFunctionExpr) {
                List<Expr> params = new ArrayList<>();
                params.add(getChild(1));
                params.add(getChild(0));
                LambdaFunctionCallExpr arrayMapFunc = new LambdaFunctionCallExpr("array_map",
                        params);
                arrayMapFunc.analyzeImpl(analyzer);
                Expr castExpr = arrayMapFunc.castTo(ArrayType.create(Type.BOOLEAN, true));
                this.setChild(1, castExpr);
                argTypes[1] = castExpr.getType();
            }
            if (!(getChild(1) instanceof CastExpr)) {
                Expr castExpr = getChild(1).castTo(ArrayType.create(Type.BOOLEAN, true));
                this.setChild(1, castExpr);
                argTypes[1] = castExpr.getType();
            }

            fn = getBuiltinFunction(fnName.getFunction(), argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (fn == null) {
                LOG.warn("fn {} not exists", this.toSqlImpl());
                throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
            }
            fn.setReturnType(getChild(0).getType());
        } else if (fnName.getFunction().equalsIgnoreCase("array_sortby")) {
            if (fnParams.exprs() == null || fnParams.exprs().size() < 2) {
                throw new AnalysisException("The " + fnName.getFunction() + " function must have at least two params");
            }
            /*
             * array_sortby((x,y)->(x+y), [1,-2,3], [10,11,12]) --->
             * array_sortby([1,-2,3],[10,11,12], (x,y)->(x+y))
             * ---> array_sortby([1,-2,3], array_map((x,y)->(x+y), [1,-2,3], [10,11,12]))
             */
            if (getChild(childSize - 1) instanceof LambdaFunctionExpr) {
                List<Expr> params = new ArrayList<>();
                for (int i = 0; i <= childSize - 1; ++i) {
                    params.add(getChild(i));
                }
                LambdaFunctionCallExpr arrayMapFunc = new LambdaFunctionCallExpr("array_map",
                        params);
                arrayMapFunc.analyzeImpl(analyzer);
                Expr firstExpr = getChild(0);
                this.clearChildren();
                this.addChild(firstExpr);
                this.addChild(arrayMapFunc);
                argTypes = new Type[2];
                argTypes[0] = getChild(0).getType();
                argTypes[1] = getChild(1).getType();
            }
            fn = getBuiltinFunction(fnName.getFunction(), argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (fn == null) {
                LOG.warn("fn {} not exists", this.toSqlImpl());
                throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
            }
            fn.setReturnType(getChild(0).getType());
        }
        LOG.info("fn string: " + fn.signatureString() + ". return type: " + fn.getReturnType());
        if (fn == null) {
            LOG.warn("fn {} not exists", this.toSqlImpl());
            throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
        }
        this.type = fn.getReturnType();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        FunctionName fnName = getFnName();
        if (LAMBDA_MAPPED_FUNCTION_SET.contains(fnName.getFunction().toLowerCase())) {
            msg.node_type = TExprNodeType.FUNCTION_CALL;
        } else {
            msg.node_type = TExprNodeType.LAMBDA_FUNCTION_CALL_EXPR;
        }
    }
}
