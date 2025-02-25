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
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LambdaFunctionCallExpr extends FunctionCallExpr {
    public static final ImmutableSet<String> LAMBDA_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER).add("array_map").add("array_filter").add("array_exists").add("array_sortby")
            .add("array_first_index").add("array_last_index").add("array_first").add("array_last").add("array_count")
            .add("array_split").add("array_reverse_split")
            .build();
    // The functions in this set are all normal array functions when implemented initially.
    // and then wants add lambda expr as the input param, so we rewrite it to contains an array_map lambda function
    // rather than reimplementing a lambda function, this will be reused the implementation of normal array function
    public static final ImmutableSet<String> LAMBDA_MAPPED_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER).add("array_exists").add("array_sortby")
            .add("array_first_index").add("array_last_index").add("array_first").add("array_last").add("array_count")
            .add("element_at").add("array_split").add("array_reverse_split").add("array_match_any")
            .add("array_match_all").build();

    private static final Logger LOG = LogManager.getLogger(LambdaFunctionCallExpr.class);

    private LambdaFunctionCallExpr() {
        // use for serde only
    }

    public LambdaFunctionCallExpr(String functionName, List<Expr> params) {
        super(functionName, params);
    }

    public LambdaFunctionCallExpr(FunctionName functionName, List<Expr> params) {
        super(functionName, params);
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

            Expr lambda = this.children.get(0);
            if (!(lambda instanceof LambdaFunctionExpr)) {
                throw new AnalysisException("array_map must use lambda as first input params, now is"
                        + lambda.debugString());
            }
            fn = new Function(fnName, Arrays.asList(argTypes), ArrayType.create(lambda.getChild(0).getType(), true),
                    true, true, NullableMode.CUSTOM);
        } else if (fnName.getFunction().equalsIgnoreCase("array_exists")
                || fnName.getFunction().equalsIgnoreCase("array_first_index")
                || fnName.getFunction().equalsIgnoreCase("array_last_index")
                || fnName.getFunction().equalsIgnoreCase("array_count")) {
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
        } else if (fnName.getFunction().equalsIgnoreCase("array_split")
                || fnName.getFunction().equalsIgnoreCase("array_reverse_split")) {
            if (fnParams.exprs() == null || fnParams.exprs().size() < 2) {
                throw new AnalysisException("The " + fnName.getFunction() + " function must have at least two params");
            }
            /*
             * array_split((x,y)->y, [1,-2,3], [0,1,1])
             * ---> array_split([1,-2,3],[0,1,1], (x,y)->y)
             * ---> array_split([1,-2,3], array_map((x,y)->y, [1,-2,3], [0,1,1]))
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
        } else if (fnName.getFunction().equalsIgnoreCase("array_last")) {
            // array_last(lambda,array)--->array_last(array,lambda)--->element_at(array_filter,-1)
            if (getChild(childSize - 1) instanceof LambdaFunctionExpr) {
                List<Expr> params = new ArrayList<>();
                for (int i = 0; i <= childSize - 1; ++i) {
                    params.add(getChild(i));
                }
                LambdaFunctionCallExpr arrayFilterFunc = new LambdaFunctionCallExpr("array_filter", params);
                arrayFilterFunc.analyzeImpl(analyzer);
                IntLiteral indexParam = new IntLiteral(-1, Type.INT);

                argTypes = new Type[2];
                argTypes[0] = getChild(0).getType();
                argTypes[1] = indexParam.getType();
                this.children.clear();
                this.children.add(arrayFilterFunc);
                this.children.add(indexParam);
            }
            fnName = new FunctionName(null, "element_at");
            fn = getBuiltinFunction(fnName.getFunction(), argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.getFunction().equalsIgnoreCase("array_first")) {
            // array_last(lambda,array)--->array_first(array,lambda)--->element_at(array_filter,1)
            if (getChild(childSize - 1) instanceof LambdaFunctionExpr) {
                List<Expr> params = new ArrayList<>();
                for (int i = 0; i <= childSize - 1; ++i) {
                    params.add(getChild(i));
                }
                LambdaFunctionCallExpr arrayFilterFunc = new LambdaFunctionCallExpr("array_filter", params);
                arrayFilterFunc.analyzeImpl(analyzer);
                IntLiteral indexParam = new IntLiteral(1, Type.INT);

                argTypes = new Type[2];
                argTypes[0] = getChild(0).getType();
                argTypes[1] = indexParam.getType();
                this.children.clear();
                this.children.add(arrayFilterFunc);
                this.children.add(indexParam);
            }
            fnName = new FunctionName(null, "element_at");
            fn = getBuiltinFunction(fnName.getFunction(), argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }
        if (fn == null) {
            LOG.warn("fn {} not exists", this.toSqlImpl());
            throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("fn string: " + fn.signatureString() + ". return type: " + fn.getReturnType());
        }
        this.type = fn.getReturnType();
        if (this.type.isArrayType() && ((ArrayType) this.type).getItemType().isDecimalV3()
                && getChild(0).getType().isArrayType()
                && ((ArrayType) getChild(0).getType()).getItemType().isDecimalV3()) {
            this.type = new ArrayType(((ArrayType) getChild(0).getType()).getItemType());
        } else if (this.type.isDecimalV3()
                && getChild(0).getType().isArrayType()
                && ((ArrayType) getChild(0).getType()).getItemType().isDecimalV3()) {
            this.type = ((ArrayType) getChild(0).getType()).getItemType();
        }
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
}
