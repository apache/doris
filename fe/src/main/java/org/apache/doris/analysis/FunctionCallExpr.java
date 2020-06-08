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

import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TAggregateExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

// TODO: for aggregations, we need to unify the code paths for builtins and UDAs.
public class FunctionCallExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(FunctionCallExpr.class);
    private FunctionName fnName;
    // private BuiltinAggregateFunction.Operator aggOp;
    private FunctionParams fnParams;

    // check analytic function
    private boolean isAnalyticFnCall = false;

    // Indicates whether this is a merge aggregation function that should use the merge
    // instead of the update symbol. This flag also affects the behavior of
    // resetAnalysisState() which is used during expr substitution.
    private boolean isMergeAggFn;

    private static final ImmutableSet<String> STDDEV_FUNCTION_SET =
            new ImmutableSortedSet.Builder(String.CASE_INSENSITIVE_ORDER)
                    .add("stddev").add("stddev_val").add("stddev_samp")
                    .add("variance").add("variance_pop").add("variance_pop").add("var_samp").add("var_pop").build();

    public void setIsAnalyticFnCall(boolean v) {
        isAnalyticFnCall = v;
    }

    public Function getFn() {
        return fn;
    }

    public FunctionName getFnName() {
        return fnName;
    }

    // only used restore from readFields.
    private FunctionCallExpr() {
        super();
    }

    public FunctionCallExpr(String functionName, List<Expr> params) {
        this(new FunctionName(functionName), new FunctionParams(false, params));
    }

    public FunctionCallExpr(FunctionName fnName, List<Expr> params) {
        this(fnName, new FunctionParams(false, params));
    }

    public FunctionCallExpr(String fnName, FunctionParams params) {
        this(new FunctionName(fnName), params);
    }

    public FunctionCallExpr(FunctionName fnName, FunctionParams params) {
        this(fnName, params, false);
    }

    private FunctionCallExpr(
        FunctionName fnName, FunctionParams params, boolean isMergeAggFn) {
                super();
        this.fnName = fnName;
        fnParams = params;
        this.isMergeAggFn = isMergeAggFn;
        if (params.exprs() != null) {
            children.addAll(params.exprs());
        }
    }

    // Constructs the same agg function with new params.
    public FunctionCallExpr(FunctionCallExpr e, FunctionParams params) {
        Preconditions.checkState(e.isAnalyzed);
        Preconditions.checkState(e.isAggregateFunction() || e.isAnalyticFnCall);
        fnName = e.fnName;
        // aggOp = e.aggOp;
        isAnalyticFnCall = e.isAnalyticFnCall;
        fnParams = params;
        // Just inherit the function object from 'e'.
        fn = e.fn;
        this.isMergeAggFn = e.isMergeAggFn;
        if (params.exprs() != null) {
            children.addAll(params.exprs());
        }
    }

    protected FunctionCallExpr(FunctionCallExpr other) {
        super(other);
        fnName = other.fnName;
        isAnalyticFnCall = other.isAnalyticFnCall;
     //   aggOp = other.aggOp;
        // fnParams = other.fnParams;
        // Clone the params in a way that keeps the children_ and the params.exprs()
        // in sync. The children have already been cloned in the super c'tor.
        if (other.fnParams.isStar()) {
            Preconditions.checkState(children.isEmpty());
            fnParams = FunctionParams.createStarParam();
        } else {
            fnParams = new FunctionParams(other.fnParams.isDistinct(), children);
        }
        this.isMergeAggFn = other.isMergeAggFn;
        fn = other.fn;
    }

    public boolean isMergeAggFn() {
        return isMergeAggFn;
    }

    @Override
    public Expr clone() {
        return new FunctionCallExpr(this);
    }

    @Override
    public void resetAnalysisState() {
        isAnalyzed = false;
        // Resolving merge agg functions after substitution may fail e.g., if the
        // intermediate agg type is not the same as the output type. Preserve the original
        // fn_ such that analyze() hits the special-case code for merge agg fns that
        // handles this case.
        if (!isMergeAggFn) {
            fn = null;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        FunctionCallExpr o = (FunctionCallExpr) obj;
        return /*opcode == o.opcode && aggOp == o.aggOp &&*/ fnName.equals(o.fnName)
                && fnParams.isDistinct() == o.fnParams.isDistinct()
                && fnParams.isStar() == o.fnParams.isStar();
    }

    @Override
    public String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append(fnName).append("(");
        if (fnParams.isStar()) {
            sb.append("*");
        }
        if (fnParams.isDistinct()) {
            sb.append("DISTINCT ");
        }
        sb.append(Joiner.on(", ").join(childrenToSql())).append(")");
        return sb.toString();
    }

    @Override
    public String debugString() {
        return MoreObjects.toStringHelper(this)/*.add("op", aggOp)*/.add("name", fnName).add("isStar",
                fnParams.isStar()).add("isDistinct", fnParams.isDistinct()).addValue(
                super.debugString()).toString();
    }

    public FunctionParams getParams() {
        return fnParams;
    }

    public boolean isScalarFunction() {
        Preconditions.checkState(fn != null);
        return fn instanceof ScalarFunction ;
    }

    public boolean isAggregateFunction() {
        Preconditions.checkState(fn != null);
        return fn instanceof AggregateFunction && !isAnalyticFnCall;
    }

    public boolean isBuiltin() {
        Preconditions.checkState(fn != null);
        return fn instanceof BuiltinAggregateFunction && !isAnalyticFnCall;
    }
    /**
     * Returns true if this is a call to an aggregate function that returns
     * non-null on an empty input (e.g. count).
     */
    public boolean returnsNonNullOnEmpty() {
        Preconditions.checkNotNull(fn);
        return fn instanceof AggregateFunction
            && ((AggregateFunction) fn).returnsNonNullOnEmpty();
    }

    public boolean isDistinct() {
        Preconditions.checkState(isAggregateFunction());
        return fnParams.isDistinct();
    }

    public boolean isCountStar() {
        if (fnName.getFunction().equalsIgnoreCase("count")) {
            if (fnParams.isStar()) {
                return true;
            } else if (fnParams.exprs() == null || fnParams.exprs().isEmpty()) {
                return true;
            } else {
                for (Expr expr : fnParams.exprs()) {
                    if (expr.isConstant()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean isCountDistinctBitmapOrHLL() {
        if (!fnParams.isDistinct()) {
            return false;
        }

        if (!fnName.getFunction().equalsIgnoreCase("count")) {
            return false;
        }

        if (children.size() != 1) {
            return false;
        }

        Type type = getChild(0).getType();
        return type.isBitmapType() || type.isHllType();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // TODO: we never serialize this to thrift if it's an aggregate function
        // except in test cases that do it explicitly.
        if (isAggregate() || isAnalyticFnCall) {
            msg.node_type = TExprNodeType.AGG_EXPR;
            if (!isAnalyticFnCall) {
                msg.setAgg_expr(new TAggregateExpr(isMergeAggFn));
            }
        } else {
            msg.node_type = TExprNodeType.FUNCTION_CALL;
        }
    }

    private void analyzeBuiltinAggFunction(Analyzer analyzer) throws AnalysisException {
        if (fnParams.isStar() && !fnName.getFunction().equalsIgnoreCase("count")) {
            throw new AnalysisException(
                    "'*' can only be used in conjunction with COUNT: " + this.toSql());
        }

        if (fnName.getFunction().equalsIgnoreCase("count")) {
            // for multiple exprs count must be qualified with distinct
            if (children.size() > 1 && !fnParams.isDistinct()) {
                throw new AnalysisException(
                        "COUNT must have DISTINCT for multiple arguments: " + this.toSql());
            }

            for (Expr child : children) {
                if (child.type.isOnlyMetricType()) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase("group_concat")) {
            if (children.size() > 2 || children.isEmpty()) {
                throw new AnalysisException(
                         "group_concat requires one or two parameters: " + this.toSql());
            }

            if (fnParams.isDistinct()) {
                throw new AnalysisException("group_concat does not support DISTINCT");
            }

            Expr arg0 = getChild(0);
            if (!arg0.type.isStringType() && !arg0.type.isNull()) {
                throw new AnalysisException(
                         "group_concat requires first parameter to be of type STRING: " + this.toSql());
            }

            if (children.size() == 2) {
                Expr arg1 = getChild(1);
                if (!arg1.type.isStringType() && !arg1.type.isNull()) {
                    throw new AnalysisException(
                            "group_concat requires second parameter to be of type STRING: " + this.toSql());
                }
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase("lag")
                || fnName.getFunction().equalsIgnoreCase("lead")) {
            if (!isAnalyticFnCall) {
                throw new AnalysisException(fnName.getFunction() + " only used in analytic function");
            } else {
                if (children.size() > 2) {
                    if (!getChild(2).isConstant()) {
                        throw new AnalysisException(
                                "The default parameter (parameter 3) of LAG must be a constant: "
                                        + this.toSql());
                    }
                }
                return;
            }
        }

        if (fnName.getFunction().equalsIgnoreCase("dense_rank")
                || fnName.getFunction().equalsIgnoreCase("rank")
                || fnName.getFunction().equalsIgnoreCase("row_number")
                || fnName.getFunction().equalsIgnoreCase("first_value")
                || fnName.getFunction().equalsIgnoreCase("last_value")
                || fnName.getFunction().equalsIgnoreCase("first_value_rewrite")) {
            if (!isAnalyticFnCall) {
                throw new AnalysisException(fnName.getFunction() + " only used in analytic function");
            }
        }

        // Function's arg can't be null for the following functions.
        Expr arg = getChild(0);
        if (arg == null) {
            return;
        }

        // SUM and AVG cannot be applied to non-numeric types
        if ((fnName.getFunction().equalsIgnoreCase("sum")
                || fnName.getFunction().equalsIgnoreCase("avg"))
                && ((!arg.type.isNumericType() && !arg.type.isNull()) || arg.type.isOnlyMetricType())) {
            throw new AnalysisException(fnName.getFunction() + " requires a numeric parameter: " + this.toSql());
        }
        if (fnName.getFunction().equalsIgnoreCase("sum_distinct")
                && ((!arg.type.isNumericType() && !arg.type.isNull()) || arg.type.isOnlyMetricType())) {
            throw new AnalysisException(
                    "SUM_DISTINCT requires a numeric parameter: " + this.toSql());
        }

        if ((fnName.getFunction().equalsIgnoreCase("min")
                || fnName.getFunction().equalsIgnoreCase("max")
                || fnName.getFunction().equalsIgnoreCase("DISTINCT_PC")
                || fnName.getFunction().equalsIgnoreCase("DISTINCT_PCSA")
                || fnName.getFunction().equalsIgnoreCase("NDV"))
                && arg.type.isOnlyMetricType()) {
            throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
        }

        if ((fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION_INT) && !arg.type.isInteger32Type())) {
            throw new AnalysisException("BITMAP_UNION_INT params only support TINYINT or SMALLINT or INT");
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.INTERSECT_COUNT)) {
            if (children.size() <= 2) {
                throw new AnalysisException("intersect_count(bitmap_column, column_to_filter, filter_values) " +
                        "function requires at least three parameters");
            }

            Type inputType = getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new AnalysisException("intersect_count function first argument should be of BITMAP type, but was " + inputType);
            }

            for(int i = 2; i < children.size(); i++) {
                if (!getChild(i).isConstant()) {
                    throw new AnalysisException("intersect_count function filter_values arg must be constant");
                }
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_COUNT)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION_COUNT)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_INTERSECT)) {
            if (children.size() != 1) {
                throw new AnalysisException(fnName + " function could only have one child");
            }
            Type inputType = getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new AnalysisException(fnName + " function's argument should be of BITMAP type, but was " + inputType);
            }
            return;
        }

        if ((fnName.getFunction().equalsIgnoreCase("HLL_UNION_AGG")
                || fnName.getFunction().equalsIgnoreCase("HLL_CARDINALITY")
                || fnName.getFunction().equalsIgnoreCase("HLL_RAW_AGG"))
                && !arg.type.isHllType()) {
            throw new AnalysisException(
                    "HLL_UNION_AGG, HLL_RAW_AGG and HLL_CARDINALITY's params must be hll column");
        }

        if (fnName.getFunction().equalsIgnoreCase("min")
                || fnName.getFunction().equalsIgnoreCase("max")) {
            fnParams.setIsDistinct(false);  // DISTINCT is meaningless here
        } else if (fnName.getFunction().equalsIgnoreCase("DISTINCT_PC")
                || fnName.getFunction().equalsIgnoreCase("DISTINCT_PCSA")
                || fnName.getFunction().equalsIgnoreCase("NDV")
                || fnName.getFunction().equalsIgnoreCase("HLL_UNION_AGG")) {
            fnParams.setIsDistinct(false);
        }

        if (fnName.getFunction().equalsIgnoreCase("percentile_approx")) {
            if (children.size() != 2 && children.size() != 3) {
                throw new AnalysisException("percentile_approx(expr, DOUBLE [, B]) requires two or three parameters");
            }
            if (!getChild(1).isConstant()) {
                throw new AnalysisException("percentile_approx requires second parameter must be a constant : "
                        + this.toSql());
            }
            if (children.size() == 3) {
                if (!getChild(2).isConstant()) {
                    throw new AnalysisException("percentile_approx requires the third parameter must be a constant : "
                            + this.toSql());
                }
            }
        }
    }

    // Provide better error message for some aggregate builtins. These can be
    // a bit more user friendly than a generic function not found.
    // TODO: should we bother to do this? We could also improve the general
    // error messages. For example, listing the alternatives.
    protected String getFunctionNotFoundError(Type[] argTypes) {
        // Some custom error message for builtins
        if (fnParams.isStar()) {
            return "'*' can only be used in conjunction with COUNT";
        }

        if (fnName.getFunction().equalsIgnoreCase("count")) {
            if (!fnParams.isDistinct() && argTypes.length > 1) {
                return "COUNT must have DISTINCT for multiple arguments: " + toSql();
            }
        }

        if (fnName.getFunction().equalsIgnoreCase("sum")) {
            return "SUM requires a numeric parameter: " + toSql();
        }

        if (fnName.getFunction().equalsIgnoreCase("avg")) {
            return "AVG requires a numeric or timestamp parameter: " + toSql();
        }

        String[] argTypesSql = new String[argTypes.length];
        for (int i = 0; i < argTypes.length; ++i) {
            argTypesSql[i] = argTypes[i].toSql();
        }

        return String.format(
                "No matching function with signature: %s(%s).",
                fnName, fnParams.isStar() ? "*" : Joiner.on(", ").join(argTypesSql));
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (isMergeAggFn) {
            // This is the function call expr after splitting up to a merge aggregation.
            // The function has already been analyzed so just do the minimal sanity
            // check here.
            AggregateFunction aggFn = (AggregateFunction) fn;
            Preconditions.checkNotNull(aggFn);
            Type intermediateType = aggFn.getIntermediateType();
            if (intermediateType == null) {
                intermediateType = type;
            }
            // Preconditions.checkState(!type.isWildcardDecimal());
            return;
        }

        if (fnName.getFunction().equals("count") && fnParams.isDistinct()) {
            // Treat COUNT(DISTINCT ...) special because of how we do the rewrite.
            // There is no version of COUNT() that takes more than 1 argument but after
            // the rewrite, we only need count(*).
            // TODO: fix how we rewrite count distinct.
            fn = getBuiltinFunction(analyzer, fnName.getFunction(), new Type[0],
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            type = fn.getReturnType();

            // Make sure BE doesn't see any TYPE_NULL exprs
            for (int i = 0; i < children.size(); ++i) {
                if (getChild(i).getType().isNull()) {
                    uncheckedCastChild(Type.BOOLEAN, i);
                }
            }
            return;
        }
        Type[] argTypes = new Type[this.children.size()];
        for (int i = 0; i < this.children.size(); ++i) {
            this.children.get(i).analyze(analyzer);
            argTypes[i] = this.children.get(i).getType();
        }

        analyzeBuiltinAggFunction(analyzer);

        if (fnName.getFunction().equalsIgnoreCase("sum")) {
            if (this.children.isEmpty()) {
                throw new AnalysisException("The " + fnName + " function must has one input param");
            }
            Type type = getChild(0).type.getMaxResolutionType();
            fn = getBuiltinFunction(analyzer, fnName.getFunction(), new Type[]{type},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.getFunction().equalsIgnoreCase("count_distinct")) {
            Type compatibleType = this.children.get(0).getType();
            for (int i = 1; i < this.children.size(); ++i) {
                Type type = this.children.get(i).getType();
                compatibleType = Type.getAssignmentCompatibleType(compatibleType, type, true);
                if (compatibleType.isInvalid()) {
                    compatibleType = Type.VARCHAR;
                    break;
                }
            }

            fn = getBuiltinFunction(analyzer, fnName.getFunction(), new Type[]{compatibleType},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else {
            // now first find function in built-in functions
            if (Strings.isNullOrEmpty(fnName.getDb())) {
                Type[] childTypes = collectChildReturnTypes();
                fn = getBuiltinFunction(analyzer, fnName.getFunction(), childTypes,
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            }

            // find user defined functions
            if (fn == null) {
                if (!analyzer.isUDFAllowed()) {
                    throw new AnalysisException(
                            "Does not support non-builtin functions, or function does not exist: " + this.toSqlImpl());
                }

                String dbName = fnName.analyzeDb(analyzer);
                if (!Strings.isNullOrEmpty(dbName)) {
                    // check operation privilege
                    if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(
                            ConnectContext.get(), dbName, PrivPredicate.SELECT)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SELECT");
                    }
                    Database db = Catalog.getCurrentCatalog().getDb(dbName);
                    if (db != null) {
                        Function searchDesc = new Function(
                                fnName, collectChildReturnTypes(), Type.INVALID, false);
                        fn = db.getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    }
                }
            }
        }

        if (fn == null) {
            LOG.warn("fn {} not exists", this.toSqlImpl());
            throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
        }

        if (fnName.getFunction().equalsIgnoreCase("from_unixtime")
        || fnName.getFunction().equalsIgnoreCase("date_format")) {
            // if has only one child, it has default time format: yyyy-MM-dd HH:mm:ss.SSSSSS
            if (children.size() > 1) {
                final StringLiteral fmtLiteral = (StringLiteral) children.get(1);
                if (fmtLiteral.getStringValue().equals("yyyyMMdd")) {
                    children.set(1, new StringLiteral("%Y%m%d"));
                } else if (fmtLiteral.getStringValue().equals("yyyy-MM-dd")) {
                    children.set(1, new StringLiteral("%Y-%m-%d"));
                } else if (fmtLiteral.getStringValue().equals("yyyy-MM-dd HH:mm:ss")) {
                    children.set(1, new StringLiteral("%Y-%m-%d %H:%i:%s"));
                }
            }
        }

        if (fn.getFunctionName().getFunction().equals("time_diff")) {
            fn.getReturnType().getPrimitiveType().setTimeType();
            return;
        }

        if (isAggregateFunction()) {
            final String functionName = fnName.getFunction();
            // subexprs must not contain aggregates
            if (Expr.containsAggregate(children)) {
                throw new AnalysisException(
                        "aggregate function cannot contain aggregate parameters: " + this.toSql());
            }

            if (STDDEV_FUNCTION_SET.contains(functionName) && argTypes[0].isDateType()) {
                throw new AnalysisException("Stddev/variance function do not support Date/Datetime type");
            }

            if (functionName.equalsIgnoreCase("multi_distinct_sum") && argTypes[0].isDateType()) {
                throw new AnalysisException("Sum in multi distinct functions do not support Date/Datetime type");
            }

        } else {
            if (fnParams.isStar()) {
                throw new AnalysisException("Cannot pass '*' to scalar function.");
            }
            if (fnParams.isDistinct()) {
                throw new AnalysisException("Cannot pass 'DISTINCT' to scalar function.");
            }
        }

        Type[] args = fn.getArgs();
        if (args.length > 0) {
            // Implicitly cast all the children to match the function if necessary
            for (int i = 0; i < argTypes.length; ++i) {
                // For varargs, we must compare with the last type in callArgs.argTypes.
                int ix = Math.min(args.length - 1, i);
                if (!argTypes[i].matchesType(args[ix]) && !(
                        argTypes[i].isDateType() && args[ix].isDateType())) {
                    uncheckedCastChild(args[ix], i);
                    //if (argTypes[i] != args[ix]) castChild(args[ix], i);
                }
            }
        }
        this.type = fn.getReturnType();
    }

    @Override
    public boolean isVectorized() {
        return false;
    }

    public static FunctionCallExpr createMergeAggCall(
            FunctionCallExpr agg, List<Expr> params) {
        Preconditions.checkState(agg.isAnalyzed);
        Preconditions.checkState(agg.isAggregateFunction());
        FunctionCallExpr result = new FunctionCallExpr(
                agg.fnName, new FunctionParams(false, params), true);
        // Inherit the function object from 'agg'.
        result.fn = agg.fn;
        result.type = agg.type;
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        fnName.write(out);
        fnParams.write(out);
        out.writeBoolean(isAnalyticFnCall);
        out.writeBoolean(isMergeAggFn);
    }

    public void readFields(DataInput in) throws IOException {
        fnName = FunctionName.read(in);
        fnParams = FunctionParams.read(in);
        if (fnParams.exprs() != null) {
            children.addAll(fnParams.exprs());
        }
        isAnalyticFnCall = in.readBoolean();
        isMergeAggFn = in.readBoolean();
    }

    public static FunctionCallExpr read(DataInput in) throws IOException {
        FunctionCallExpr func = new FunctionCallExpr();
        func.readFields(in);
        return func;
    }

    // Used for store load
    public boolean supportSerializable() {
        for (Expr child : children) {
            if (!child.supportSerializable()) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean isConstantImpl() {
        // TODO: we can't correctly determine const-ness before analyzing 'fn_'. We should
        // rework logic so that we do not call this function on unanalyzed exprs.
        // Aggregate functions are never constant.
        if (fn instanceof AggregateFunction) return false;

        final String fnName = this.fnName.getFunction();
        // Non-deterministic functions are never constant.
        if (isNondeterministicBuiltinFnName(fnName)) {
            return false;
        }
        // Sleep is a special function for testing.
        if (fnName.equalsIgnoreCase("sleep")) return false;
        return super.isConstantImpl();
    }

    private static boolean isNondeterministicBuiltinFnName(String fnName) {
        if (fnName.equalsIgnoreCase("rand") || fnName.equalsIgnoreCase("random")
                || fnName.equalsIgnoreCase("uuid")) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hashCode(opcode);
        result = 31 * result + Objects.hashCode(fnName);
        result = 31 * result + Objects.hashCode(fnParams);
        return result;
    }
}
