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

import org.apache.doris.catalog.Function.BinaryType;
import org.apache.doris.thrift.TAggregateFunction;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FunctionToThriftConverterTest {

    // ======================== ScalarFunction Tests ========================

    @Test
    void testScalarFunctionJavaUdf_symbolIsPopulated() {
        FunctionName name = new FunctionName("db1", "java_udf_fn");
        Type[] argTypes = {Type.INT, Type.STRING};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.DOUBLE, false, null, "com.example.MyFn", null, null);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.DOUBLE, argTypes, new Boolean[]{true, true});

        Assertions.assertTrue(result.isSetScalarFn());
        Assertions.assertFalse(result.isSetAggregateFn());
        Assertions.assertEquals("com.example.MyFn", result.getScalarFn().getSymbol());
    }

    @Test
    void testScalarFunctionRpc_symbolIsPopulated() {
        FunctionName name = new FunctionName("db1", "rpc_fn");
        Type[] argTypes = {Type.BIGINT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.RPC, name, argTypes,
                Type.INT, false, null, "rpc_symbol_name", null, null);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.INT, argTypes, new Boolean[]{true});

        Assertions.assertTrue(result.isSetScalarFn());
        Assertions.assertEquals("rpc_symbol_name", result.getScalarFn().getSymbol());
    }

    @Test
    void testScalarFunctionNative_symbolIsEmpty() {
        FunctionName name = new FunctionName("db1", "native_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.NATIVE, name, argTypes,
                Type.INT, false, null, "ignored_symbol", null, null);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.INT, argTypes, new Boolean[]{true});

        Assertions.assertTrue(result.isSetScalarFn());
        Assertions.assertEquals("", result.getScalarFn().getSymbol());
    }

    // ======================== AggregateFunction Tests ========================

    @Test
    void testAggregateFunctionWithAllSymbols() {
        FunctionName name = new FunctionName("db1", "my_agg");
        Type[] argTypes = {Type.BIGINT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.BIGINT, false,
                Type.BIGINT, null,
                "init_fn", "update_fn", "merge_fn",
                "serialize_fn", "finalize_fn", "get_value_fn", "remove_fn");
        fn.setBinaryType(BinaryType.NATIVE);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.BIGINT, argTypes, new Boolean[]{true});

        Assertions.assertTrue(result.isSetAggregateFn());
        Assertions.assertFalse(result.isSetScalarFn());

        TAggregateFunction aggFn = result.getAggregateFn();
        Assertions.assertEquals("init_fn", aggFn.getInitFnSymbol());
        Assertions.assertEquals("update_fn", aggFn.getUpdateFnSymbol());
        Assertions.assertEquals("merge_fn", aggFn.getMergeFnSymbol());
        Assertions.assertEquals("serialize_fn", aggFn.getSerializeFnSymbol());
        Assertions.assertEquals("finalize_fn", aggFn.getFinalizeFnSymbol());
        Assertions.assertEquals("get_value_fn", aggFn.getGetValueFnSymbol());
        Assertions.assertEquals("remove_fn", aggFn.getRemoveFnSymbol());
    }

    @Test
    void testAggregateFunctionWithIntermediateType() {
        FunctionName name = new FunctionName("db1", "agg_with_intermediate");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.BIGINT, false,
                Type.STRING, null,
                "init", "update", "merge", null, null, null, null);
        fn.setBinaryType(BinaryType.NATIVE);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.BIGINT, argTypes, new Boolean[]{true});

        TAggregateFunction aggFn = result.getAggregateFn();
        // intermediateType is set explicitly to STRING, not the return type BIGINT
        Assertions.assertEquals(Type.STRING.toThrift(), aggFn.getIntermediateType());
    }

    @Test
    void testAggregateFunctionWithoutIntermediateType_usesReturnType() {
        FunctionName name = new FunctionName("db1", "agg_no_intermediate");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.BIGINT, false,
                Type.BIGINT, null,
                "init", "update", "merge", null, null, null, null);
        fn.setBinaryType(BinaryType.NATIVE);
        // Clear intermediateType so the converter falls back to returnType
        fn.setIntermediateType(null);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.BIGINT, argTypes, new Boolean[]{true});

        TAggregateFunction aggFn = result.getAggregateFn();
        // When intermediateType is null, the converter falls back to the return type
        Assertions.assertEquals(Type.BIGINT.toThrift(), aggFn.getIntermediateType());
    }

    @Test
    void testAggregateFunctionWithNullOptionalSymbols() {
        FunctionName name = new FunctionName("db1", "agg_partial");
        Type[] argTypes = {Type.DOUBLE};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.DOUBLE, false,
                Type.DOUBLE, null,
                "init", "update", "merge",
                null, null, null, null);
        fn.setBinaryType(BinaryType.NATIVE);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.DOUBLE, argTypes, new Boolean[]{true});

        TAggregateFunction aggFn = result.getAggregateFn();
        Assertions.assertEquals("init", aggFn.getInitFnSymbol());
        Assertions.assertEquals("update", aggFn.getUpdateFnSymbol());
        Assertions.assertEquals("merge", aggFn.getMergeFnSymbol());
        // Optional symbols should not be set when null
        Assertions.assertFalse(aggFn.isSetSerializeFnSymbol());
        Assertions.assertFalse(aggFn.isSetFinalizeFnSymbol());
        Assertions.assertFalse(aggFn.isSetGetValueFnSymbol());
        Assertions.assertFalse(aggFn.isSetRemoveFnSymbol());
    }

    // ======================== Base Field Tests ========================

    @Test
    void testBaseFields_nameHandling() {
        FunctionName name = new FunctionName("test_db", "test_func");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, false, null, "sym", null, null);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.INT, argTypes, new Boolean[]{true});

        Assertions.assertNotNull(result.getName());
        Assertions.assertEquals("test_db", result.getName().getDbName());
        Assertions.assertEquals("test_func", result.getName().getFunctionName());
    }

    @Test
    void testBaseFields_signatureAndBinaryType() {
        FunctionName name = new FunctionName("db1", "sig_fn");
        Type[] argTypes = {Type.INT, Type.DOUBLE};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.STRING, false, null, "sym", null, null);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.STRING, argTypes, new Boolean[]{true, true});

        Assertions.assertNotNull(result.getSignature());
        Assertions.assertTrue(result.getSignature().contains("sig_fn"));
        Assertions.assertEquals(TFunctionBinaryType.JAVA_UDF, result.getBinaryType());
    }

    @Test
    void testBaseFields_functionProperties() {
        FunctionName name = new FunctionName("db1", "prop_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, true, null, "sym", null, null);
        fn.setId(42L);
        fn.setChecksum("abc123");
        fn.setUDTFunction(true);
        fn.setStaticLoad(true);
        fn.setExpirationTime(9999L);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.INT, argTypes, new Boolean[]{true});

        Assertions.assertTrue(result.isHasVarArgs());
        Assertions.assertEquals(42L, result.getId());
        Assertions.assertEquals("abc123", result.getChecksum());
        Assertions.assertTrue(result.isIsUdtfFunction());
        Assertions.assertTrue(result.isIsStaticLoad());
        Assertions.assertEquals(9999L, result.getExpirationTime());
    }

    @Test
    void testBaseFields_emptyChecksum_notSet() {
        FunctionName name = new FunctionName("db1", "no_checksum_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.NATIVE, name, argTypes,
                Type.INT, false, null, "", null, null);
        // Default checksum is "" — should NOT be set on the thrift object

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.INT, argTypes, new Boolean[]{true});

        Assertions.assertFalse(result.isSetChecksum());
    }

    // ======================== Return Type Precision Tests ========================

    @Test
    void testRealReturnTypeWithPrecision_usesRealReturnType() {
        FunctionName name = new FunctionName("db1", "decimal_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.NATIVE, name, argTypes,
                Type.DOUBLE, false, null, "", null, null);

        ScalarType realReturnType = ScalarType.createDecimalV3Type(18, 6);

        TFunction result = FunctionToThriftConverter.toThrift(
                fn, realReturnType, argTypes, new Boolean[]{true});

        // realReturnType has precision, so it should be used instead of fn.getReturnType()
        Assertions.assertEquals(realReturnType.toThrift(), result.getRetType());
    }

    @Test
    void testRealReturnTypeWithoutPrecision_usesFunctionReturnType() {
        FunctionName name = new FunctionName("db1", "simple_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.NATIVE, name, argTypes,
                Type.DOUBLE, false, null, "", null, null);

        // Type.INT does not contain precision, so fn.getReturnType() (DOUBLE) should be used
        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.INT, argTypes, new Boolean[]{true});

        Assertions.assertEquals(fn.getReturnType().toThrift(), result.getRetType());
    }

    // ======================== Dispatcher Tests ========================

    @Test
    void testDispatcher_routesScalarFunctionCorrectly() {
        FunctionName name = new FunctionName("db1", "dispatch_scalar");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, false, null, "sym", null, null);

        // Call the dispatcher overload that takes Function (not ScalarFunction)
        TFunction result = FunctionToThriftConverter.toThrift(
                (Function) fn, Type.INT, argTypes, new Boolean[]{true});

        Assertions.assertTrue(result.isSetScalarFn());
        Assertions.assertFalse(result.isSetAggregateFn());
        Assertions.assertEquals("sym", result.getScalarFn().getSymbol());
    }

    @Test
    void testDispatcher_routesAggregateFunctionCorrectly() {
        FunctionName name = new FunctionName("db1", "dispatch_agg");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.INT, false,
                Type.INT, null,
                "init", "update", "merge", null, null, null, null);
        fn.setBinaryType(BinaryType.NATIVE);

        // Call the dispatcher overload that takes Function (not AggregateFunction)
        TFunction result = FunctionToThriftConverter.toThrift(
                (Function) fn, Type.INT, argTypes, new Boolean[]{true});

        Assertions.assertTrue(result.isSetAggregateFn());
        Assertions.assertFalse(result.isSetScalarFn());
    }

    // ======================== Arg Type Tests ========================

    @Test
    void testArgTypes_realArgTypesSameLength() {
        FunctionName name = new FunctionName("db1", "arg_fn");
        Type[] argTypes = {Type.INT, Type.BIGINT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.NATIVE, name, argTypes,
                Type.INT, false, null, "", null, null);

        Type[] realArgTypes = {Type.INT, Type.BIGINT};
        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.INT, realArgTypes, new Boolean[]{true, true});

        Assertions.assertNotNull(result.getArgTypes());
        Assertions.assertEquals(2, result.getArgTypes().size());
    }

    @Test
    void testArgTypes_realArgTypesDifferentLength() {
        FunctionName name = new FunctionName("db1", "vararg_fn");
        Type[] argTypes = {Type.INT, Type.BIGINT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.NATIVE, name, argTypes,
                Type.INT, true, null, "", null, null);

        // Pass different number of realArgTypes to trigger the alternate branch
        Type[] realArgTypes = {Type.INT, Type.BIGINT, Type.DOUBLE};
        TFunction result = FunctionToThriftConverter.toThrift(
                fn, Type.INT, realArgTypes, new Boolean[]{true, true, true});

        Assertions.assertNotNull(result.getArgTypes());
        // Should use fn.getArgs() which has 2 arg types
        Assertions.assertEquals(2, result.getArgTypes().size());
    }
}
