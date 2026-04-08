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

import org.apache.doris.thrift.TAggregateFunction;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;
import org.apache.doris.thrift.TFunctionName;
import org.apache.doris.thrift.TScalarFunction;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Converts {@link Function} and its subclasses to their Thrift representations.
 */
public class FunctionToThriftConverter {

    /**
     * Converts a {@link Function.BinaryType} to its Thrift representation.
     */
    public static TFunctionBinaryType toThriftBinaryType(Function.BinaryType binaryType) {
        switch (binaryType) {
            case BUILTIN:    return TFunctionBinaryType.BUILTIN;
            case HIVE:       return TFunctionBinaryType.HIVE;
            case NATIVE:     return TFunctionBinaryType.NATIVE;
            case IR:         return TFunctionBinaryType.IR;
            case RPC:        return TFunctionBinaryType.RPC;
            case JAVA_UDF:   return TFunctionBinaryType.JAVA_UDF;
            case AGG_STATE:  return TFunctionBinaryType.AGG_STATE;
            case PYTHON_UDF: return TFunctionBinaryType.PYTHON_UDF;
            default: throw new IllegalArgumentException("Unknown BinaryType: " + binaryType);
        }
    }

    /**
     * Converts a Thrift {@link TFunctionBinaryType} to {@link Function.BinaryType}.
     */
    public static Function.BinaryType fromThriftBinaryType(TFunctionBinaryType thriftType) {
        switch (thriftType) {
            case BUILTIN:    return Function.BinaryType.BUILTIN;
            case HIVE:       return Function.BinaryType.HIVE;
            case NATIVE:     return Function.BinaryType.NATIVE;
            case IR:         return Function.BinaryType.IR;
            case RPC:        return Function.BinaryType.RPC;
            case JAVA_UDF:   return Function.BinaryType.JAVA_UDF;
            case AGG_STATE:  return Function.BinaryType.AGG_STATE;
            case PYTHON_UDF: return Function.BinaryType.PYTHON_UDF;
            default: throw new IllegalArgumentException("Unknown TFunctionBinaryType: " + thriftType);
        }
    }

    /**
     * Converts a {@link Function} (or subclass) to its Thrift representation.
     * Uses instanceof checks to dispatch to the appropriate subclass handler.
     */
    public static TFunction toThrift(Function fn, Type realReturnType, Type[] realArgTypes,
            Boolean[] realArgTypeNullables) {
        if (fn instanceof ScalarFunction) {
            return toThrift((ScalarFunction) fn, realReturnType, realArgTypes, realArgTypeNullables);
        } else if (fn instanceof AggregateFunction) {
            return toThrift((AggregateFunction) fn, realReturnType, realArgTypes, realArgTypeNullables);
        }
        return toThriftBase(fn, realReturnType, realArgTypes, realArgTypeNullables);
    }

    /**
     * Converts a {@link ScalarFunction} to its Thrift representation.
     */
    public static TFunction toThrift(ScalarFunction fn, Type realReturnType, Type[] realArgTypes,
            Boolean[] realArgTypeNullables) {
        TFunction tfn = toThriftBase(fn, realReturnType, realArgTypes, realArgTypeNullables);
        tfn.setScalarFn(new TScalarFunction());
        if (fn.getBinaryType() == Function.BinaryType.JAVA_UDF || fn.getBinaryType() == Function.BinaryType.RPC
                || fn.getBinaryType() == Function.BinaryType.PYTHON_UDF) {
            tfn.getScalarFn().setSymbol(fn.getSymbolName());
        } else {
            tfn.getScalarFn().setSymbol("");
        }
        if (fn.getBinaryType() == Function.BinaryType.PYTHON_UDF) {
            if (!Strings.isNullOrEmpty(fn.getFunctionCode())) {
                tfn.setFunctionCode(fn.getFunctionCode());
            }
            tfn.setRuntimeVersion(fn.getRuntimeVersion());
        }
        if (fn.getDictFunction() != null) {
            tfn.setDictFunction(fn.getDictFunction());
        }
        return tfn;
    }

    /**
     * Converts an {@link AggregateFunction} to its Thrift representation.
     */
    public static TFunction toThrift(AggregateFunction fn, Type realReturnType, Type[] realArgTypes,
            Boolean[] realArgTypeNullables) {
        TFunction tfn = toThriftBase(fn, realReturnType, realArgTypes, realArgTypeNullables);
        TAggregateFunction aggFn = new TAggregateFunction();
        aggFn.setIsAnalyticOnlyFn(fn.isAnalyticFn() && !fn.isAggregateFn());
        aggFn.setUpdateFnSymbol(fn.getUpdateFnSymbol());
        aggFn.setInitFnSymbol(fn.getInitFnSymbol());
        if (fn.getSerializeFnSymbol() != null) {
            aggFn.setSerializeFnSymbol(fn.getSerializeFnSymbol());
        }
        aggFn.setMergeFnSymbol(fn.getMergeFnSymbol());
        if (fn.getGetValueFnSymbol() != null) {
            aggFn.setGetValueFnSymbol(fn.getGetValueFnSymbol());
        }
        if (fn.getRemoveFnSymbol() != null) {
            aggFn.setRemoveFnSymbol(fn.getRemoveFnSymbol());
        }
        if (fn.getFinalizeFnSymbol() != null) {
            aggFn.setFinalizeFnSymbol(fn.getFinalizeFnSymbol());
        }
        if (fn.getSymbolName() != null) {
            aggFn.setSymbol(fn.getSymbolName());
        }
        if (fn.getIntermediateType() != null) {
            aggFn.setIntermediateType(fn.getIntermediateType().toThrift());
        } else {
            aggFn.setIntermediateType(fn.getReturnType().toThrift());
        }
        //    agg_fn.setIgnores_distinct(ignoresDistinct);
        tfn.setAggregateFn(aggFn);

        // Set runtime_version and function_code for Python UDAF
        if (fn.getBinaryType() == Function.BinaryType.PYTHON_UDF) {
            if (!Strings.isNullOrEmpty(fn.getFunctionCode())) {
                tfn.setFunctionCode(fn.getFunctionCode());
            }
            tfn.setRuntimeVersion(fn.getRuntimeVersion());
        }

        return tfn;
    }

    private static TFunction toThriftBase(Function fn, Type realReturnType, Type[] realArgTypes,
            Boolean[] realArgTypeNullables) {
        TFunction tfn = new TFunction();
        tfn.setSignature(fn.signatureString());
        TFunctionName tName = new TFunctionName(fn.getFunctionName().getFunction());
        tName.setDbName(fn.getFunctionName().getDb());
        tName.setFunctionName(fn.getFunctionName().getFunction());
        tfn.setName(tName);
        tfn.setBinaryType(toThriftBinaryType(fn.getBinaryType()));
        if (fn.getLocation() != null) {
            tfn.setHdfsLocation(fn.getLocation().getLocation());
        }
        // `realArgTypes.length != argTypes.length` is true iff this is an aggregation
        // function.
        // For aggregation functions, `argTypes` here is already its real type with true
        // precision and scale.
        Type[] argTypes = fn.getArgs();
        if (realArgTypes.length != argTypes.length) {
            tfn.setArgTypes(Type.toThrift(Lists.newArrayList(argTypes)));
        } else {
            tfn.setArgTypes(Type.toThrift(Lists.newArrayList(argTypes), Lists.newArrayList(realArgTypes)));
        }

        // For types with different precisions and scales, return type only indicates a
        // type with default
        // precision and scale so we need to transform it to the correct type.
        if (realReturnType.typeContainsPrecision() || realReturnType.isAggStateType()) {
            tfn.setRetType(realReturnType.toThrift());
        } else {
            tfn.setRetType(fn.getReturnType().toThrift());
        }
        tfn.setHasVarArgs(fn.hasVarArgs());
        // TODO: Comment field is missing?
        // tfn.setComment(comment)
        tfn.setId(fn.getId());
        if (!fn.getChecksum().isEmpty()) {
            tfn.setChecksum(fn.getChecksum());
        }
        tfn.setVectorized(fn.isVectorized());
        tfn.setIsUdtfFunction(fn.isUDTFunction());
        tfn.setIsStaticLoad(fn.isStaticLoad());
        tfn.setExpirationTime(fn.getExpirationTime());
        return tfn;
    }
}
