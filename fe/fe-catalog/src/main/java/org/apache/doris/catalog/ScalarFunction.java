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

import org.apache.doris.common.URI;

import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.List;

// import org.apache.doris.thrift.TSymbolType;

/**
 * Internal representation of a scalar function.
 */
public class ScalarFunction extends Function {
    // The name inside the binary at location_ that contains this particular
    // function. e.g. org.example.MyUdf.class.
    @SerializedName("sn")
    private String symbolName;
    @SerializedName("pfs")
    private String prepareFnSymbol;
    @SerializedName("cfs")
    private String closeFnSymbol;

    DictFunction dictFunction = null;

    public static class DictFunction {
        private final long id;
        private final long version;

        public DictFunction(long id, long version) {
            this.id = id;
            this.version = version;
        }

        public long getId() {
            return id;
        }

        public long getVersion() {
            return version;
        }
    }

    // Only used for serialization
    protected ScalarFunction() {
    }

    public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs,
            boolean userVisible) {
        this(fnName, argTypes, retType, hasVarArgs, Function.BinaryType.BUILTIN, userVisible, true);
    }

    private ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs,
            Function.BinaryType binaryType, boolean userVisible, boolean isVec) {
        super(0, fnName, argTypes, retType, hasVarArgs, binaryType, userVisible, isVec,
                NullableMode.DEPEND_ON_ARGUMENT);
    }

    /**
     * nerieds custom scalar function
     */
    public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs, String symbolName,
            BinaryType binaryType, boolean userVisible, boolean isVec, NullableMode nullableMode) {
        super(0, fnName, argTypes, retType, hasVarArgs, binaryType, userVisible, isVec, nullableMode);
        this.symbolName = symbolName;
    }

    public static ScalarFunction createUdf(
            Function.BinaryType binaryType,
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
        dictFunction = other.dictFunction;
    }

    @Override
    public Function clone() {
        return new ScalarFunction(this);
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

    public DictFunction getDictFunction() {
        return dictFunction;
    }

    public void setDictFunction(DictFunction dictFunction) {
        this.dictFunction = dictFunction;
    }
}
