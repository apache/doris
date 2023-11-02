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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateFunctionStmt;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.common.io.IOUtils;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.URI;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TAggregateFunction;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Internal representation of an aggregate function.
 * TODO: Create separate AnalyticFunction class
 */
public class AggregateFunction extends Function {

    private static final Logger LOG = LogManager.getLogger(AggregateFunction.class);

    public static ImmutableSet<String> NOT_NULLABLE_AGGREGATE_FUNCTION_NAME_SET = ImmutableSet.of("row_number", "rank",
            "dense_rank", "multi_distinct_count", "multi_distinct_sum", FunctionSet.HLL_UNION_AGG,
            FunctionSet.HLL_UNION, FunctionSet.HLL_RAW_AGG, FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_INTERSECT,
            FunctionSet.ORTHOGONAL_BITMAP_INTERSECT, FunctionSet.ORTHOGONAL_BITMAP_INTERSECT_COUNT,
            FunctionSet.ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT, FunctionSet.ORTHOGONAL_BITMAP_EXPR_CALCULATE,
            FunctionSet.INTERSECT_COUNT, FunctionSet.ORTHOGONAL_BITMAP_UNION_COUNT,
            FunctionSet.COUNT, "approx_count_distinct", "ndv", FunctionSet.BITMAP_UNION_INT,
            FunctionSet.BITMAP_UNION_COUNT, "ndv_no_finalize", FunctionSet.WINDOW_FUNNEL, FunctionSet.RETENTION,
            FunctionSet.SEQUENCE_MATCH, FunctionSet.SEQUENCE_COUNT, FunctionSet.MAP_AGG, FunctionSet.BITMAP_AGG,
            FunctionSet.ARRAY_AGG, FunctionSet.COLLECT_LIST, FunctionSet.COLLECT_SET);

    public static ImmutableSet<String> ALWAYS_NULLABLE_AGGREGATE_FUNCTION_NAME_SET =
            ImmutableSet.of("stddev_samp", "variance_samp", "var_samp", "percentile_approx");

    public static ImmutableSet<String> CUSTOM_AGGREGATE_FUNCTION_NAME_SET =
            ImmutableSet.of("group_concat");

    public static ImmutableSet<String> SUPPORT_ORDER_BY_AGGREGATE_FUNCTION_NAME_SET = ImmutableSet.of("group_concat");

    @SerializedName(value = "functionType")
    private final FunctionType functionType = FunctionType.AGGREGATE;

    // Set if different from retType_, null otherwise.
    @SerializedName(value = "intermediateType")
    private Type intermediateType;

    // The symbol inside the binary at location_ that contains this particular.
    // They can be null if it is not required.
    @SerializedName(value = "updateFnSymbol")
    private String updateFnSymbol;
    @SerializedName(value = "initFnSymbol")
    private String initFnSymbol;
    @SerializedName(value = "serializeFnSymbol")
    private String serializeFnSymbol;
    @SerializedName(value = "mergeFnSymbol")
    private String mergeFnSymbol;
    @SerializedName(value = "getValueFnSymbol")
    private String getValueFnSymbol;
    @SerializedName(value = "removeFnSymbol")
    private String removeFnSymbol;
    @SerializedName(value = "finalizeFnSymbol")
    private String finalizeFnSymbol;

    private static String BE_BUILTINS_CLASS = "AggregateFunctions";

    // If true, this aggregate function should ignore distinct.
    // e.g. min(distinct col) == min(col).
    // TODO: currently it is not possible for user functions to specify this. We should
    // extend the create aggregate function stmt to allow additional metadata like this.
    @SerializedName(value = "ignoresDistinct")
    private boolean ignoresDistinct;

    // True if this function can appear within an analytic expr (fn() OVER(...)).
    // TODO: Instead of manually setting this flag for all builtin aggregate functions
    // we should identify this property from the function itself (e.g., based on which
    // functions of the UDA API are implemented).
    // Currently, there is no reliable way of doing that.
    @SerializedName(value = "isAnalyticFn")
    private boolean isAnalyticFn;

    // True if this function can be used for aggregation (without an OVER() clause).
    @SerializedName(value = "isAggregateFn")
    private boolean isAggregateFn;

    // True if this function returns a non-null value on an empty input. It is used
    // primarily during the equal of scalar subqueries.
    // TODO: Instead of manually setting this flag, we should identify this
    // property from the function itself (e.g. evaluating the function on an
    // empty input in BE).
    @SerializedName(value = "returnsNonNullOnEmpty")
    private boolean returnsNonNullOnEmpty;

    // use for java-udaf to point the class of user define
    @SerializedName(value = "symbolName")
    private String symbolName;

    // only used for serialization
    protected AggregateFunction() {
    }

    public AggregateFunction(FunctionName fnName, List<Type> argTypes,
            Type retType, Type intermediateType,
            URI location, String updateFnSymbol, String initFnSymbol,
            String serializeFnSymbol, String mergeFnSymbol, String getValueFnSymbol,
            String removeFnSymbol, String finalizeFnSymbol) {
        this(fnName, argTypes, retType, intermediateType, location, updateFnSymbol, initFnSymbol, serializeFnSymbol,
                mergeFnSymbol, getValueFnSymbol, removeFnSymbol, finalizeFnSymbol, false);
    }

    public AggregateFunction(FunctionName fnName, List<Type> argTypes,
            Type retType, Type intermediateType, boolean hasVarArgs) {
        super(fnName, argTypes, retType, hasVarArgs);
        this.intermediateType = (intermediateType != null && intermediateType.equals(retType))
                ? null : intermediateType;
        ignoresDistinct = false;
        isAnalyticFn = false;
        isAggregateFn = true;
        returnsNonNullOnEmpty = false;
    }

    public static AggregateFunction createBuiltin(String name,
            List<Type> argTypes, Type retType, Type intermediateType,
            boolean ignoresDistinct,
            boolean isAnalyticFn,
            boolean returnsNonNullOnEmpty) {
        return createBuiltin(name, argTypes, retType, intermediateType, false,
                ignoresDistinct, isAnalyticFn, returnsNonNullOnEmpty);
    }

    public static AggregateFunction createBuiltin(String name,
            List<Type> argTypes, Type retType, Type intermediateType,
            boolean hasVarArgs, boolean ignoresDistinct,
            boolean isAnalyticFn,
            boolean returnsNonNullOnEmpty) {
        AggregateFunction fn = new AggregateFunction(new FunctionName(name),
                argTypes, retType, intermediateType, hasVarArgs);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        fn.ignoresDistinct = ignoresDistinct;
        fn.isAnalyticFn = isAnalyticFn;
        fn.isAggregateFn = true;
        fn.returnsNonNullOnEmpty = returnsNonNullOnEmpty;
        return fn;
    }

    public AggregateFunction(FunctionName fnName, List<Type> argTypes,
            Type retType, Type intermediateType,
            URI location, String updateFnSymbol, String initFnSymbol,
            String serializeFnSymbol, String mergeFnSymbol, String getValueFnSymbol,
            String removeFnSymbol, String finalizeFnSymbol, boolean vectorized) {
        this(fnName, argTypes, retType, intermediateType, false, location,
                updateFnSymbol, initFnSymbol, serializeFnSymbol,
                mergeFnSymbol, getValueFnSymbol, removeFnSymbol, finalizeFnSymbol, vectorized);
    }

    public AggregateFunction(FunctionName fnName, List<Type> argTypes,
            Type retType, Type intermediateType, boolean hasVarArgs,
            URI location, String updateFnSymbol, String initFnSymbol,
            String serializeFnSymbol, String mergeFnSymbol, String getValueFnSymbol,
            String removeFnSymbol, String finalizeFnSymbol, boolean vectorized) {
        // only `count` is always not nullable, other aggregate function is always nullable
        super(fnName, argTypes, retType, hasVarArgs, vectorized,
                AggregateFunction.NOT_NULLABLE_AGGREGATE_FUNCTION_NAME_SET.contains(fnName.getFunction())
                        ? NullableMode.ALWAYS_NOT_NULLABLE :
                        AggregateFunction.ALWAYS_NULLABLE_AGGREGATE_FUNCTION_NAME_SET.contains(fnName.getFunction())
                                ? NullableMode.ALWAYS_NULLABLE :
                                AggregateFunction.CUSTOM_AGGREGATE_FUNCTION_NAME_SET.contains(fnName.getFunction())
                                        ? NullableMode.CUSTOM : NullableMode.DEPEND_ON_ARGUMENT);
        setLocation(location);
        this.intermediateType = (intermediateType.equals(retType)) ? null : intermediateType;
        this.updateFnSymbol = updateFnSymbol;
        this.initFnSymbol = initFnSymbol;
        this.serializeFnSymbol = serializeFnSymbol;
        this.mergeFnSymbol = mergeFnSymbol;
        this.getValueFnSymbol = getValueFnSymbol;
        this.removeFnSymbol = removeFnSymbol;
        this.finalizeFnSymbol = finalizeFnSymbol;
        ignoresDistinct = false;
        isAnalyticFn = false;
        isAggregateFn = true;
        returnsNonNullOnEmpty = false;
    }

    public AggregateFunction(FunctionName fnName, List<Type> argTypes,
            Type retType, Type intermediateType, boolean hasVarArgs,
            URI location, String updateFnSymbol, String initFnSymbol,
            String serializeFnSymbol, String mergeFnSymbol, String getValueFnSymbol,
            String removeFnSymbol, String finalizeFnSymbol, boolean ignoresDistinct,
            boolean isAnalyticFn, boolean returnsNonNullOnEmpty, TFunctionBinaryType binaryType,
            boolean userVisible, boolean vectorized, NullableMode nullableMode) {
        // only `count` is always not nullable, other aggregate function is always nullable
        super(0, fnName, argTypes, retType, hasVarArgs, binaryType, userVisible, vectorized, nullableMode);
        setLocation(location);
        this.intermediateType = (intermediateType.equals(retType)) ? null : intermediateType;
        this.updateFnSymbol = updateFnSymbol;
        this.initFnSymbol = initFnSymbol;
        this.serializeFnSymbol = serializeFnSymbol;
        this.mergeFnSymbol = mergeFnSymbol;
        this.getValueFnSymbol = getValueFnSymbol;
        this.removeFnSymbol = removeFnSymbol;
        this.finalizeFnSymbol = finalizeFnSymbol;
        this.ignoresDistinct = ignoresDistinct;
        this.isAnalyticFn = isAnalyticFn;
        this.isAggregateFn = true;
        this.returnsNonNullOnEmpty = returnsNonNullOnEmpty;
    }

    public static AggregateFunction createBuiltin(String name,
            List<Type> argTypes, Type retType, Type intermediateType,
            String initFnSymbol, String updateFnSymbol, String mergeFnSymbol,
            String serializeFnSymbol, String finalizeFnSymbol, boolean ignoresDistinct,
            boolean isAnalyticFn, boolean returnsNonNullOnEmpty, boolean vectorized) {
        return createBuiltin(name, argTypes, retType, intermediateType, initFnSymbol,
                updateFnSymbol, mergeFnSymbol, serializeFnSymbol, null, null, finalizeFnSymbol,
                ignoresDistinct, isAnalyticFn, returnsNonNullOnEmpty, vectorized);
    }

    public static AggregateFunction createBuiltin(String name,
            List<Type> argTypes, Type retType, Type intermediateType,
            String initFnSymbol, String updateFnSymbol, String mergeFnSymbol,
            String serializeFnSymbol, String getValueFnSymbol, String removeFnSymbol,
            String finalizeFnSymbol, boolean ignoresDistinct, boolean isAnalyticFn,
            boolean returnsNonNullOnEmpty, boolean vectorized) {
        return createBuiltin(name, argTypes, retType, intermediateType, false,
                initFnSymbol, updateFnSymbol, mergeFnSymbol,
                serializeFnSymbol, getValueFnSymbol, removeFnSymbol,
                finalizeFnSymbol, ignoresDistinct, isAnalyticFn, returnsNonNullOnEmpty, vectorized);
    }

    public static AggregateFunction createBuiltin(String name,
            List<Type> argTypes, Type retType, Type intermediateType, boolean hasVarArgs,
            String initFnSymbol, String updateFnSymbol, String mergeFnSymbol,
            String serializeFnSymbol, String getValueFnSymbol, String removeFnSymbol,
            String finalizeFnSymbol, boolean ignoresDistinct, boolean isAnalyticFn,
            boolean returnsNonNullOnEmpty, boolean vectorized) {
        AggregateFunction fn = new AggregateFunction(new FunctionName(name),
                argTypes, retType, intermediateType, hasVarArgs, null, updateFnSymbol, initFnSymbol,
                serializeFnSymbol, mergeFnSymbol, getValueFnSymbol, removeFnSymbol,
                finalizeFnSymbol, vectorized);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        fn.ignoresDistinct = ignoresDistinct;
        fn.isAnalyticFn = isAnalyticFn;
        fn.isAggregateFn = true;
        fn.returnsNonNullOnEmpty = returnsNonNullOnEmpty;
        return fn;
    }

    public static AggregateFunction createAnalyticBuiltin(String name,
            List<Type> argTypes, Type retType, Type intermediateType, boolean vectorized) {
        return createAnalyticBuiltin(name, argTypes, retType, intermediateType, null,
                null, null, null, null, true, vectorized);
    }

    public static AggregateFunction createAnalyticBuiltin(String name,
            List<Type> argTypes, Type retType, Type intermediateType,
            String initFnSymbol, String updateFnSymbol, String removeFnSymbol,
            String getValueFnSymbol, String finalizeFnSymbol) {
        return createAnalyticBuiltin(name, argTypes, retType, intermediateType,
                initFnSymbol, updateFnSymbol, removeFnSymbol, getValueFnSymbol, finalizeFnSymbol,
                true, true);
    }

    public static AggregateFunction createAnalyticBuiltin(String name,
            List<Type> argTypes, Type retType, Type intermediateType,
            String initFnSymbol, String updateFnSymbol, String removeFnSymbol,
            String getValueFnSymbol, String finalizeFnSymbol, boolean vectorized) {
        return createAnalyticBuiltin(name, argTypes, retType, intermediateType,
                initFnSymbol, updateFnSymbol, removeFnSymbol, getValueFnSymbol, finalizeFnSymbol,
                true, vectorized);
    }

    public static AggregateFunction createAnalyticBuiltin(String name,
            List<Type> argTypes, Type retType, Type intermediateType,
            String initFnSymbol, String updateFnSymbol, String removeFnSymbol,
            String getValueFnSymbol, String finalizeFnSymbol, boolean isUserVisible, boolean vectorized) {
        AggregateFunction fn = new AggregateFunction(new FunctionName(name),
                argTypes, retType, intermediateType, null, updateFnSymbol, initFnSymbol,
                null, null, getValueFnSymbol, removeFnSymbol, finalizeFnSymbol, vectorized);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        fn.ignoresDistinct = false;
        fn.isAnalyticFn = true;
        fn.isAggregateFn = false;
        fn.returnsNonNullOnEmpty = false;
        fn.setUserVisible(isUserVisible);
        return fn;
    }

    public AggregateFunction(AggregateFunction other) {
        super(other);
        if (other == null) {
            return;
        }
        ignoresDistinct = other.ignoresDistinct;
        isAnalyticFn = other.isAnalyticFn;
        isAggregateFn = other.isAggregateFn;
        returnsNonNullOnEmpty = other.returnsNonNullOnEmpty;
    }

    @Override
    public Function clone() {
        return new AggregateFunction(this);
    }

    // Used to create UDAF
    public AggregateFunction(FunctionName fnName, Type[] argTypes,
            Type retType, boolean hasVarArgs, Type intermediateType, URI location,
            String initFnSymbol, String updateFnSymbol, String mergeFnSymbol,
            String serializeFnSymbol, String finalizeFnSymbol,
            String getValueFnSymbol, String removeFnSymbol) {
        super(fnName, Arrays.asList(argTypes), retType, hasVarArgs);
        this.setLocation(location);
        this.intermediateType = (intermediateType.equals(retType)) ? null : intermediateType;
        this.updateFnSymbol = updateFnSymbol;
        this.initFnSymbol = initFnSymbol;
        this.serializeFnSymbol = serializeFnSymbol;
        this.mergeFnSymbol = mergeFnSymbol;
        this.getValueFnSymbol = getValueFnSymbol;
        this.removeFnSymbol = removeFnSymbol;
        this.finalizeFnSymbol = finalizeFnSymbol;
        ignoresDistinct = false;
        isAnalyticFn = true;
        isAggregateFn = true;
        returnsNonNullOnEmpty = false;
    }

    public static class AggregateFunctionBuilder {
        TFunctionBinaryType binaryType;
        FunctionName name;
        Type[] argTypes;
        Type retType;
        boolean hasVarArgs;
        Type intermediateType;
        URI location;
        String initFnSymbol;
        String updateFnSymbol;
        String serializeFnSymbol;
        String finalizeFnSymbol;
        String mergeFnSymbol;
        String removeFnSymbol;
        String getValueFnSymbol;
        String symbolName;

        private AggregateFunctionBuilder(TFunctionBinaryType binaryType) {
            this.binaryType = binaryType;
        }

        public static AggregateFunctionBuilder createUdfBuilder() {
            return new AggregateFunctionBuilder(TFunctionBinaryType.JAVA_UDF);
        }

        public AggregateFunctionBuilder name(FunctionName name) {
            this.name = name;
            return this;
        }

        public AggregateFunctionBuilder argsType(Type[] argTypes) {
            this.argTypes = argTypes;
            return this;
        }

        public AggregateFunctionBuilder retType(Type type) {
            this.retType = type;
            return this;
        }

        public AggregateFunctionBuilder hasVarArgs(boolean hasVarArgs) {
            this.hasVarArgs = hasVarArgs;
            return this;
        }

        public AggregateFunctionBuilder intermediateType(Type type) {
            this.intermediateType = type;
            return this;
        }

        public AggregateFunctionBuilder location(URI location) {
            this.location = location;
            return this;
        }

        public AggregateFunctionBuilder initFnSymbol(String symbol) {
            this.initFnSymbol = symbol;
            return this;
        }

        public AggregateFunctionBuilder updateFnSymbol(String symbol) {
            this.updateFnSymbol = symbol;
            return this;
        }

        public AggregateFunctionBuilder mergeFnSymbol(String symbol) {
            this.mergeFnSymbol = symbol;
            return this;
        }

        public AggregateFunctionBuilder serializeFnSymbol(String symbol) {
            this.serializeFnSymbol = symbol;
            return this;
        }

        public AggregateFunctionBuilder finalizeFnSymbol(String symbol) {
            this.finalizeFnSymbol = symbol;
            return this;
        }

        public AggregateFunctionBuilder getValueFnSymbol(String symbol) {
            this.getValueFnSymbol = symbol;
            return this;
        }

        public AggregateFunctionBuilder removeFnSymbol(String symbol) {
            this.removeFnSymbol = symbol;
            return this;
        }

        public AggregateFunctionBuilder binaryType(TFunctionBinaryType binaryType) {
            this.binaryType = binaryType;
            return this;
        }

        public AggregateFunctionBuilder symbolName(String symbol) {
            this.symbolName = symbol;
            return this;
        }

        public AggregateFunction build() {
            AggregateFunction fn = new AggregateFunction(name, argTypes, retType, hasVarArgs, intermediateType,
                    location, initFnSymbol, updateFnSymbol, mergeFnSymbol,
                    serializeFnSymbol, finalizeFnSymbol,
                    getValueFnSymbol, removeFnSymbol);
            fn.setBinaryType(binaryType);
            fn.symbolName = symbolName;
            return fn;
        }
    }

    public String getUpdateFnSymbol() {
        return updateFnSymbol;
    }

    public String getInitFnSymbol() {
        return initFnSymbol;
    }

    public String getSerializeFnSymbol() {
        return serializeFnSymbol;
    }

    public String getMergeFnSymbol() {
        return mergeFnSymbol;
    }

    public String getGetValueFnSymbol() {
        return getValueFnSymbol;
    }

    public String getRemoveFnSymbol() {
        return removeFnSymbol;
    }

    public String getFinalizeFnSymbol() {
        return finalizeFnSymbol;
    }

    public String getSymbolName() {
        return symbolName;
    }

    public boolean ignoresDistinct() {
        return ignoresDistinct;
    }

    public boolean isAnalyticFn() {
        return isAnalyticFn;
    }

    public void setIsAnalyticFn(boolean isAnalyticFn) {
        this.isAnalyticFn = isAnalyticFn;
    }

    public boolean isAggregateFn() {
        return isAggregateFn;
    }

    public boolean returnsNonNullOnEmpty() {
        return returnsNonNullOnEmpty;
    }

    /**
     * Returns the intermediate type of this aggregate function or null
     * if it is identical to the return type.
     */
    public Type getIntermediateType() {
        return intermediateType;
    }

    public void setUpdateFnSymbol(String fn) {
        updateFnSymbol = fn;
    }

    public void setInitFnSymbol(String fn) {
        initFnSymbol = fn;
    }

    public void setSerializeFnSymbol(String fn) {
        serializeFnSymbol = fn;
    }

    public void setMergeFnSymbol(String fn) {
        mergeFnSymbol = fn;
    }

    public void setGetValueFnSymbol(String fn) {
        getValueFnSymbol = fn;
    }

    public void setRemoveFnSymbol(String fn) {
        removeFnSymbol = fn;
    }

    public void setFinalizeFnSymbol(String fn) {
        finalizeFnSymbol = fn;
    }

    public void setSymbolName(String fn) {
        symbolName = fn;
    }

    public void setIntermediateType(Type t) {
        intermediateType = t;
    }

    @Override
    public String toSql(boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE ");

        if (this.isGlobal) {
            sb.append("GLOBAL ");
        }
        sb.append("AGGREGATE FUNCTION ");

        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }

        sb.append(signatureString()).append(" RETURNS " + getReturnType());
        if (getIntermediateType() != null) {
            sb.append(" INTERMEDIATE " + getIntermediateType());
        }

        sb.append(" PROPERTIES (");
        if (getBinaryType() != TFunctionBinaryType.JAVA_UDF) {
            sb.append("\n  \"INIT_FN\"=\"" + getInitFnSymbol() + "\",")
                    .append("\n  \"UPDATE_FN\"=\"" + getUpdateFnSymbol() + "\",")
                    .append("\n  \"MERGE_FN\"=\"" + getMergeFnSymbol() + "\",");
            if (getSerializeFnSymbol() != null) {
                sb.append("\n  \"SERIALIZE_FN\"=\"" + getSerializeFnSymbol() + "\",");
            }
            if (getFinalizeFnSymbol() != null) {
                sb.append("\n  \"FINALIZE_FN\"=\"" + getFinalizeFnSymbol() + "\",");
            }
        }
        if (getSymbolName() != null) {
            sb.append("\n  \"SYMBOL\"=\"" + getSymbolName() + "\",");
        }

        if (getBinaryType() == TFunctionBinaryType.JAVA_UDF) {
            sb.append("\n  \"FILE\"=")
                    .append("\"" + (getLocation() == null ? "" : getLocation().toString()) + "\",");
            boolean isReturnNull = this.getNullableMode() == NullableMode.ALWAYS_NULLABLE;
            sb.append("\n  \"ALWAYS_NULLABLE\"=").append("\"" + isReturnNull + "\",");
        } else {
            sb.append("\n  \"OBJECT_FILE\"=")
                    .append("\"" + (getLocation() == null ? "" : getLocation().toString()) + "\",");
        }
        sb.append("\n  \"TYPE\"=").append("\"" + this.getBinaryType() + "\"");
        sb.append("\n);");
        return sb.toString();
    }

    @Override
    public TFunction toThrift(Type realReturnType, Type[] realArgTypes, Boolean[] realArgTypeNullables) {
        TFunction fn = super.toThrift(realReturnType, realArgTypes, realArgTypeNullables);
        TAggregateFunction aggFn = new TAggregateFunction();
        aggFn.setIsAnalyticOnlyFn(isAnalyticFn && !isAggregateFn);
        aggFn.setUpdateFnSymbol(updateFnSymbol);
        aggFn.setInitFnSymbol(initFnSymbol);
        if (serializeFnSymbol != null) {
            aggFn.setSerializeFnSymbol(serializeFnSymbol);
        }
        aggFn.setMergeFnSymbol(mergeFnSymbol);
        if (getValueFnSymbol != null) {
            aggFn.setGetValueFnSymbol(getValueFnSymbol);
        }
        if (removeFnSymbol != null) {
            aggFn.setRemoveFnSymbol(removeFnSymbol);
        }
        if (finalizeFnSymbol != null) {
            aggFn.setFinalizeFnSymbol(finalizeFnSymbol);
        }
        if (symbolName != null) {
            aggFn.setSymbol(symbolName);
        }
        if (intermediateType != null) {
            aggFn.setIntermediateType(intermediateType.toThrift());
        } else {
            aggFn.setIntermediateType(getReturnType().toThrift());
        }
        //    agg_fn.setIgnores_distinct(ignoresDistinct);
        fn.setAggregateFn(aggFn);
        return fn;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Deprecated
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);

        if (input.readBoolean()) {
            intermediateType = ColumnType.read(input);
        }
        updateFnSymbol = IOUtils.readOptionStringOrNull(input);
        initFnSymbol = IOUtils.readOptionStringOrNull(input);
        serializeFnSymbol = IOUtils.readOptionStringOrNull(input);
        mergeFnSymbol = IOUtils.readOptionStringOrNull(input);
        getValueFnSymbol = IOUtils.readOptionStringOrNull(input);
        removeFnSymbol = IOUtils.readOptionStringOrNull(input);
        finalizeFnSymbol = IOUtils.readOptionStringOrNull(input);
        symbolName = IOUtils.readOptionStringOrNull(input);

        ignoresDistinct = input.readBoolean();
        isAnalyticFn = input.readBoolean();
        isAggregateFn = input.readBoolean();
        returnsNonNullOnEmpty = input.readBoolean();
    }

    @Override
    public String getProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateFunctionStmt.OBJECT_FILE_KEY, getLocation() == null ? "" : getLocation().toString());
        properties.put(CreateFunctionStmt.MD5_CHECKSUM, checksum);
        properties.put(CreateFunctionStmt.INIT_KEY, initFnSymbol);
        properties.put(CreateFunctionStmt.UPDATE_KEY, updateFnSymbol);
        properties.put(CreateFunctionStmt.MERGE_KEY, mergeFnSymbol);
        properties.put(CreateFunctionStmt.SERIALIZE_KEY, serializeFnSymbol);
        properties.put(CreateFunctionStmt.FINALIZE_KEY, finalizeFnSymbol);

        // getValueFn and removeFn may be null if not analytic agg
        if (getValueFnSymbol != null) {
            properties.put(CreateFunctionStmt.GET_VALUE_KEY, getValueFnSymbol);
        }
        if (removeFnSymbol != null) {
            properties.put(CreateFunctionStmt.REMOVE_KEY, removeFnSymbol);
        }
        if (symbolName != null) {
            properties.put(CreateFunctionStmt.SYMBOL_KEY, symbolName);
        }
        return new Gson().toJson(properties);
    }
}
