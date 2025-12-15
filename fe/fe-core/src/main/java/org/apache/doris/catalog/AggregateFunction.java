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

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.common.util.URI;
import org.apache.doris.nereids.trees.plans.commands.CreateFunctionCommand;
import org.apache.doris.thrift.TAggregateFunction;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Internal representation of an aggregate function.
 * TODO: Create separate AnalyticFunction class
 */
public class AggregateFunction extends Function {

    // Set if different from retType_, null otherwise.
    @SerializedName("it")
    private Type intermediateType;

    // The symbol inside the binary at location_ that contains this particular.
    // They can be null if it is not required.
    @SerializedName("ufs")
    private String updateFnSymbol;
    @SerializedName("ifs")
    private String initFnSymbol;
    @SerializedName("sfs")
    private String serializeFnSymbol;
    @SerializedName("mfs")
    private String mergeFnSymbol;
    @SerializedName("gvfs")
    private String getValueFnSymbol;
    @SerializedName("rfs")
    private String removeFnSymbol;
    @SerializedName("ffs")
    private String finalizeFnSymbol;

    // True if this function can appear within an analytic expr (fn() OVER(...)).
    // TODO: Instead of manually setting this flag for all builtin aggregate functions
    // we should identify this property from the function itself (e.g., based on which
    // functions of the UDA API are implemented).
    // Currently, there is no reliable way of doing that.
    @SerializedName("isAn")
    private boolean isAnalyticFn;

    // True if this function can be used for aggregation (without an OVER() clause).
    @SerializedName("isAg")
    private boolean isAggregateFn;

    // use for java-udaf to point the class of user define
    @SerializedName("sn")
    private String symbolName;

    // only used for serialization
    protected AggregateFunction() {
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
        this.isAnalyticFn = isAnalyticFn;
        this.isAggregateFn = true;
    }

    public AggregateFunction(AggregateFunction other) {
        super(other);
        if (other == null) {
            return;
        }
        isAnalyticFn = other.isAnalyticFn;
        isAggregateFn = other.isAggregateFn;
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
        isAnalyticFn = true;
        isAggregateFn = true;
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

    public boolean isAnalyticFn() {
        return isAnalyticFn;
    }

    public void setIsAnalyticFn(boolean isAnalyticFn) {
        this.isAnalyticFn = isAnalyticFn;
    }

    public boolean isAggregateFn() {
        return isAggregateFn;
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
    public String getProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateFunctionCommand.OBJECT_FILE_KEY, getLocation() == null ? "" : getLocation().toString());
        properties.put(CreateFunctionCommand.MD5_CHECKSUM, checksum);
        properties.put(CreateFunctionCommand.INIT_KEY, initFnSymbol);
        properties.put(CreateFunctionCommand.UPDATE_KEY, updateFnSymbol);
        properties.put(CreateFunctionCommand.MERGE_KEY, mergeFnSymbol);
        properties.put(CreateFunctionCommand.SERIALIZE_KEY, serializeFnSymbol);
        properties.put(CreateFunctionCommand.FINALIZE_KEY, finalizeFnSymbol);

        // getValueFn and removeFn may be null if not analytic agg
        if (getValueFnSymbol != null) {
            properties.put(CreateFunctionCommand.GET_VALUE_KEY, getValueFnSymbol);
        }
        if (removeFnSymbol != null) {
            properties.put(CreateFunctionCommand.REMOVE_KEY, removeFnSymbol);
        }
        if (symbolName != null) {
            properties.put(CreateFunctionCommand.SYMBOL_KEY, symbolName);
        }
        return new Gson().toJson(properties);
    }
}
