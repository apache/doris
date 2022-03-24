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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import static org.apache.doris.common.io.IOUtils.writeOptionString;

// import org.apache.doris.thrift.TSymbolType;

/**
 * Internal representation of a scalar function.
 */
public class ScalarFunction extends Function {
    private static final Logger LOG = LogManager.getLogger(ScalarFunction.class);
    // The name inside the binary at location_ that contains this particular
    // function. e.g. org.example.MyUdf.class.
    private String symbolName;
    private String prepareFnSymbol;
    private String closeFnSymbol;

    // Only used for serialization
    protected ScalarFunction() {
    }

    public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs,
             boolean userVisible) {
        this(fnName, argTypes, retType, hasVarArgs, TFunctionBinaryType.BUILTIN, userVisible, false);
    }

    public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs,
            boolean userVisible, boolean isVec) {
        this(fnName, argTypes, retType, hasVarArgs, TFunctionBinaryType.BUILTIN, userVisible, isVec);
    }

    public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs,
                          TFunctionBinaryType binaryType, boolean userVisible, boolean isVec) {
        super(0, fnName, argTypes, retType, hasVarArgs, binaryType, userVisible, isVec, NullableMode.DEPEND_ON_ARGUMENT);
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
        Preconditions.checkNotNull(symbol);
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
     * Creates a builtin scalar operator function. This is a helper that wraps a few steps
     * into one call.
     * TODO: this needs to be kept in sync with what generates the be operator
     * implementations. (gen_functions.py). Is there a better way to coordinate this.
     */
    public static ScalarFunction createBuiltinOperator(
            String name, ArrayList<Type> argTypes, Type retType, NullableMode nullableMode) {
        // Operators have a well defined symbol based on the function name and type.
        // Convert Add(TINYINT, TINYINT) --> Add_TinyIntVal_TinyIntVal
        String beFn = name;
        boolean usesDecimal = false;
        boolean usesDecimalV2 = false;
        for (int i = 0; i < argTypes.size(); ++i) {
            switch (argTypes.get(i).getPrimitiveType()) {
                case BOOLEAN:
                    beFn += "_boolean_val";
                    break;
                case TINYINT:
                    beFn += "_tiny_int_val";
                    break;
                case SMALLINT:
                    beFn += "_small_int_val";
                    break;
                case INT:
                    beFn += "_int_val";
                    break;
                case BIGINT:
                    beFn += "_big_int_val";
                    break;
                case LARGEINT:
                    beFn += "_large_int_val";
                    break;
                case FLOAT:
                    beFn += "_float_val";
                    break;
                case DOUBLE:
                case TIME:
                    beFn += "_double_val";
                    break;
                case CHAR:
                case VARCHAR:
                case HLL:
                case BITMAP:
                case STRING:
                case QUANTILE_STATE:
                    beFn += "_string_val";
                    break;
                case DATE:
                case DATETIME:
                    beFn += "_datetime_val";
                    break;
                case DECIMALV2:
                    beFn += "_decimalv2_val";
                    usesDecimalV2 = true;
                    break;
                default:
                    Preconditions.checkState(false, "Argument type not supported: " + argTypes.get(i));
            }
        }
        String beClass = usesDecimal ? "DecimalOperators" : "Operators";
        if (usesDecimalV2) beClass = "DecimalV2Operators";
        String symbol = "doris::" + beClass + "::" + beFn;
        return createBuiltinOperator(name, symbol, argTypes, retType, nullableMode);
    }

    public static ScalarFunction createVecBuiltinOperator(
            String name, ArrayList<Type> argTypes, Type retType) {
        return createVecBuiltinOperator(name, argTypes, retType, NullableMode.DEPEND_ON_ARGUMENT);
    }

    /**
     * Creates a builtin scala vec operator function. This is a helper that wraps a few steps
     * into one call.
     * TODO: this needs to be kept in sync with what generates the be operator
     * implementations. (gen_functions.py). Is there a better way to coordinate this.
     */
    public static ScalarFunction createVecBuiltinOperator(
            String name, ArrayList<Type> argTypes, Type retType, NullableMode nullableMode) {
        StringBuilder beFn = new StringBuilder(name);
        boolean usesDecimal = false;
        boolean usesDecimalV2 = false;

        // just mock a fake symbol for vec function, we treat
        // all argument is same as first argument
        for (int i = 0; i < argTypes.size(); ++i) {
            switch (argTypes.get(0).getPrimitiveType()) {
                case BOOLEAN:
                    beFn.append("_boolean_val");
                    break;
                case TINYINT:
                    beFn.append("_tiny_int_val");
                    break;
                case SMALLINT:
                    beFn.append("_small_int_val");
                    break;
                case INT:
                    beFn.append("_int_val");
                    break;
                case BIGINT:
                    beFn.append("_big_int_val");
                    break;
                case LARGEINT:
                    beFn.append("_large_int_val");
                    break;
                case FLOAT:
                    beFn.append("_float_val");
                    break;
                case DOUBLE:
                case TIME:
                    beFn.append("_double_val");
                    break;
                case CHAR:
                case VARCHAR:
                case HLL:
                case BITMAP:
                    beFn.append("_string_val");
                    break;
                case DATE:
                case DATETIME:
                    beFn.append("_datetime_val");
                    break;
                case DECIMALV2:
                    beFn.append("_decimalv2_val");
                    usesDecimalV2 = true;
                    break;
                default:
                    Preconditions.checkState(false, "Argument type not supported: " + argTypes.get(i));
            }
        }
        String beClass = usesDecimal ? "DecimalOperators" : "Operators";
        if (usesDecimalV2) beClass = "DecimalV2Operators";
        String symbol = "doris::" + beClass + "::" + beFn;
        return createVecBuiltinOperator(name, symbol, argTypes, retType, nullableMode);
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

//        try {
//            fn.symbolName_ = fn.lookupSymbol(symbol, TSymbolType.UDF_EVALUATE, null,
//                    fn.hasVarArgs(), fn.getArgs());
//        } catch (AnalysisException e) {
//            // This should never happen
//            Preconditions.checkState(false, "Builtin symbol '" + symbol + "'" + argTypes
//                    + " not found!" + e.getStackTrace());
//            throw new RuntimeException("Builtin symbol not found!", e);
//        }
        return fn;
    }

    public static ScalarFunction createVecBuiltinOperator(
            String name, String symbol, ArrayList<Type> argTypes, Type retType, NullableMode nullableMode) {
        return createVecBuiltin(name, symbol, argTypes, false, retType, false, nullableMode);
    }

    //TODO: This method should not be here, move to other place in the future
    public static ScalarFunction createVecBuiltin(
            String name, String symbol, ArrayList<Type> argTypes,
            boolean hasVarArgs, Type retType, boolean userVisible, NullableMode nullableMode) {
        ScalarFunction fn = new ScalarFunction(
                new FunctionName(name), argTypes, retType, hasVarArgs, userVisible, true);
        fn.symbolName = symbol;
        fn.nullableMode = nullableMode;
        return fn;
    }

    /**
     * Create a function that is used to search the catalog for a matching builtin. Only
     * the fields necessary for matching function prototypes are specified.
     */
    public static ScalarFunction createBuiltinSearchDesc(
            String name, Type[] argTypes, boolean hasVarArgs) {
        ArrayList<Type> fnArgs =
                (argTypes == null) ? new ArrayList<Type>() : Lists.newArrayList(argTypes);
        ScalarFunction fn = new ScalarFunction(
                new FunctionName(name), fnArgs, Type.INVALID, hasVarArgs, true);
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

    public void setSymbolName(String s) { symbolName = s; }
    public void setPrepareFnSymbol(String s) { prepareFnSymbol = s; }
    public void setCloseFnSymbol(String s) { closeFnSymbol = s; }

    public String getSymbolName() { return symbolName; }
    public String getPrepareFnSymbol() { return prepareFnSymbol; }
    public String getCloseFnSymbol() { return closeFnSymbol; }

    @Override
    public String toSql(boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE FUNCTION ");
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
        sb.append(",\n  \"OBJECT_FILE\"=")
                .append("\"" + (getLocation() == null ? "" : getLocation().toString()) + "\"");
        sb.append(",\n  \"MD5\"=").append("\"" + getChecksum() + "\"");
        sb.append("\n);");
        return sb.toString();
    }

    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        fn.setScalarFn(new TScalarFunction());
        fn.getScalarFn().setSymbol(symbolName);
        if (prepareFnSymbol != null) {
            fn.getScalarFn().setPrepareFnSymbol(prepareFnSymbol);
        }
        if (closeFnSymbol != null) {
            fn.getScalarFn().setCloseFnSymbol(closeFnSymbol);
        }
        return fn;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 1. type
        FunctionType.SCALAR.write(output);
        // 2. parent
        super.writeFields(output);
        // 3.symbols
        Text.writeString(output, symbolName);
        writeOptionString(output, prepareFnSymbol);
        writeOptionString(output, closeFnSymbol);
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
