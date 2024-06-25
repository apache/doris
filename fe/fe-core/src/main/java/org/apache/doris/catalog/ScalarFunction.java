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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.URI;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;
import org.apache.doris.thrift.TScalarFunction;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

// import org.apache.doris.thrift.TSymbolType;

/**
 * Internal representation of a scalar function.
 */
public class ScalarFunction extends Function {
    private static final Logger LOG = LogManager.getLogger(ScalarFunction.class);
    // The name inside the binary at location_ that contains this particular
    // function. e.g. org.example.MyUdf.class.
    @SerializedName("sn")
    private String symbolName;
    @SerializedName("pfs")
    private String prepareFnSymbol;
    @SerializedName("cfs")
    private String closeFnSymbol;

    // Only used for serialization
    protected ScalarFunction() {
    }

    public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs,
            boolean userVisible) {
        this(fnName, argTypes, retType, hasVarArgs, TFunctionBinaryType.BUILTIN, userVisible, true);
    }

    public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs,
            boolean userVisible, boolean isVec) {
        this(fnName, argTypes, retType, hasVarArgs, TFunctionBinaryType.BUILTIN, userVisible, isVec);
    }

    public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs,
            TFunctionBinaryType binaryType, boolean userVisible, boolean isVec) {
        super(0, fnName, argTypes, retType, hasVarArgs, binaryType, userVisible, isVec,
                NullableMode.DEPEND_ON_ARGUMENT);
    }

    /**
     * nerieds custom scalar function
     */
    public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs, String symbolName,
            TFunctionBinaryType binaryType, boolean userVisible, boolean isVec, NullableMode nullableMode) {
        super(0, fnName, argTypes, retType, hasVarArgs, binaryType, userVisible, isVec, nullableMode);
        this.symbolName = symbolName;
    }

    public ScalarFunction(FunctionName fnName, List<Type> argTypes,
            Type retType, URI location, String symbolName, String initFnSymbol,
            String closeFnSymbol) {
        super(fnName, argTypes, retType, false);
        setLocation(location);
        setSymbolName(symbolName);
        setPrepareFnSymbol(initFnSymbol);
        setCloseFnSymbol(closeFnSymbol);
    }

    /**
     * Creates a builtin scalar function. This is a helper that wraps a few steps
     * into one call.
     */
    public static ScalarFunction createBuiltin(String name, Type retType,
            ArrayList<Type> argTypes, boolean hasVarArgs,
            String symbol, String prepareFnSymbol, String closeFnSymbol,
            boolean userVisible) {
        return createBuiltin(name, retType, NullableMode.DEPEND_ON_ARGUMENT, argTypes, hasVarArgs,
                symbol, prepareFnSymbol, closeFnSymbol, userVisible);
    }

    public static ScalarFunction createBuiltin(
            String name, Type retType, NullableMode nullableMode,
            ArrayList<Type> argTypes, boolean hasVarArgs,
            String symbol, String prepareFnSymbol, String closeFnSymbol, boolean userVisible) {
        ScalarFunction fn = new ScalarFunction(
                new FunctionName(name), argTypes, retType, hasVarArgs, userVisible);
        fn.symbolName = symbol;
        fn.prepareFnSymbol = prepareFnSymbol;
        fn.closeFnSymbol = closeFnSymbol;
        fn.nullableMode = nullableMode;
        return fn;
    }

    public static ScalarFunction createBuiltinOperator(
            String name, ArrayList<Type> argTypes, Type retType) {
        return createBuiltinOperator(name, argTypes, retType, NullableMode.DEPEND_ON_ARGUMENT);
    }

    /**
     * Creates a builtin scalar operator function. This is a helper that wraps a few
     * steps
     * into one call.
     * TODO: this needs to be kept in sync with what generates the be operator
     * implementations. (gen_functions.py). Is there a better way to coordinate
     * this.
     */
    public static ScalarFunction createBuiltinOperator(
            String name, ArrayList<Type> argTypes, Type retType, NullableMode nullableMode) {
        return createBuiltinOperator(name, null, argTypes, retType, nullableMode);
    }

    public static ScalarFunction createBuiltinOperator(
            String name, String symbol, ArrayList<Type> argTypes, Type retType) {
        return createBuiltinOperator(name, symbol, argTypes, retType, NullableMode.DEPEND_ON_ARGUMENT);
    }

    public static ScalarFunction createBuiltinOperator(
            String name, String symbol, ArrayList<Type> argTypes, Type retType, NullableMode nullableMode) {
        return createBuiltin(name, symbol, argTypes, false, retType, false, nullableMode);
    }

    public static ScalarFunction createBuiltin(
            String name, String symbol, ArrayList<Type> argTypes,
            boolean hasVarArgs, Type retType, boolean userVisible, NullableMode nullableMode) {
        ScalarFunction fn = new ScalarFunction(
                new FunctionName(name), argTypes, retType, hasVarArgs, userVisible);
        fn.symbolName = symbol;
        fn.nullableMode = nullableMode;
        return fn;
    }

    public static ScalarFunction createUdf(
            TFunctionBinaryType binaryType,
            FunctionName name, Type[] args,
            Type returnType, boolean isVariadic,
            URI location, String symbol, String prepareFnSymbol, String closeFnSymbol) {
        ScalarFunction fn = new ScalarFunction(name, Arrays.asList(args), returnType, isVariadic, binaryType,
                true, false);
        fn.symbolName = symbol;
        fn.prepareFnSymbol = prepareFnSymbol;
        fn.closeFnSymbol = closeFnSymbol;
        fn.setLocation(location);
        return fn;
    }

    public ScalarFunction(ScalarFunction other) {
        super(other);
        if (other == null) {
            return;
        }
        symbolName = other.symbolName;
        prepareFnSymbol = other.prepareFnSymbol;
        closeFnSymbol = other.closeFnSymbol;
    }

    @Override
    public Function clone() {
        return new ScalarFunction(this);
    }

    public void setSymbolName(String s) {
        symbolName = s;
    }

    public void setPrepareFnSymbol(String s) {
        prepareFnSymbol = s;
    }

    public void setCloseFnSymbol(String s) {
        closeFnSymbol = s;
    }

    public String getSymbolName() {
        return symbolName;
    }

    public String getPrepareFnSymbol() {
        return prepareFnSymbol;
    }

    public String getCloseFnSymbol() {
        return closeFnSymbol;
    }

    @Override
    public String toSql(boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE ");
        if (this.isGlobal) {
            sb.append("GLOBAL ");
        }
        sb.append("FUNCTION ");

        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(signatureString())
                .append(" RETURNS " + getReturnType())
                .append(" PROPERTIES (");
        sb.append("\n  \"SYMBOL\"=").append("\"" + getSymbolName() + "\"");
        if (getPrepareFnSymbol() != null) {
            sb.append(",\n  \"PREPARE_FN\"=").append("\"" + getPrepareFnSymbol() + "\"");
        }
        if (getCloseFnSymbol() != null) {
            sb.append(",\n  \"CLOSE_FN\"=").append("\"" + getCloseFnSymbol() + "\"");
        }

        if (getBinaryType() == TFunctionBinaryType.JAVA_UDF) {
            sb.append(",\n  \"FILE\"=")
                    .append("\"" + (getLocation() == null ? "" : getLocation().toString()) + "\"");
            boolean isReturnNull = this.getNullableMode() == NullableMode.ALWAYS_NULLABLE;
            sb.append(",\n  \"ALWAYS_NULLABLE\"=").append("\"" + isReturnNull + "\"");
        } else {
            sb.append(",\n  \"OBJECT_FILE\"=")
                    .append("\"" + (getLocation() == null ? "" : getLocation().toString()) + "\"");
        }
        sb.append(",\n  \"TYPE\"=").append("\"" + this.getBinaryType() + "\"");
        sb.append("\n);");
        return sb.toString();
    }

    @Override
    public TFunction toThrift(Type realReturnType, Type[] realArgTypes, Boolean[] realArgTypeNullables) {
        TFunction fn = super.toThrift(realReturnType, realArgTypes, realArgTypeNullables);
        fn.setScalarFn(new TScalarFunction());
        if (getBinaryType() == TFunctionBinaryType.JAVA_UDF || getBinaryType() == TFunctionBinaryType.RPC) {
            fn.getScalarFn().setSymbol(symbolName);
        } else {
            fn.getScalarFn().setSymbol("");
        }
        return fn;
    }

    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        symbolName = Text.readString(input);
        if (input.readBoolean()) {
            prepareFnSymbol = Text.readString(input);
        }
        if (input.readBoolean()) {
            closeFnSymbol = Text.readString(input);
        }
    }

    @Override
    public String getProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateFunctionStmt.OBJECT_FILE_KEY, getLocation() == null ? "" : getLocation().toString());
        properties.put(CreateFunctionStmt.MD5_CHECKSUM, checksum);
        properties.put(CreateFunctionStmt.SYMBOL_KEY, symbolName);
        return new Gson().toJson(properties);
    }
}
