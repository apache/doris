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
import org.apache.doris.catalog.Function.NullableMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class FunctionToSqlConverterTest {

    // ======================== ScalarFunction — JAVA_UDF ========================

    @Test
    void testScalarFunction_javaUdf_basicSql() {
        FunctionName name = new FunctionName("testDb", "my_add");
        Type[] argTypes = {Type.INT, Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, false, null, "com.example.MyAdd", null, null);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.startsWith("CREATE FUNCTION "));
        Assertions.assertTrue(sql.contains("my_add(int, int)"));
        Assertions.assertTrue(sql.contains("RETURNS int"));
        Assertions.assertTrue(sql.contains("\"SYMBOL\"=\"com.example.MyAdd\""));
        Assertions.assertTrue(sql.contains("\"FILE\"=\"\""));
        Assertions.assertTrue(sql.contains("\"TYPE\"=\"JAVA_UDF\""));
        Assertions.assertTrue(sql.contains("\"ALWAYS_NULLABLE\"="));
        Assertions.assertFalse(sql.contains("OBJECT_FILE"));
        Assertions.assertFalse(sql.contains("IF NOT EXISTS"));
        Assertions.assertFalse(sql.contains("GLOBAL"));
    }

    @Test
    void testScalarFunction_javaUdf_alwaysNullable() {
        FunctionName name = new FunctionName("testDb", "nullable_fn");
        Type[] argTypes = {Type.STRING};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.STRING, false, null, "com.example.NullFn", null, null);
        fn.setNullableMode(NullableMode.ALWAYS_NULLABLE);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.contains("\"ALWAYS_NULLABLE\"=\"true\""));
    }

    @Test
    void testScalarFunction_javaUdf_dependOnArgument() {
        FunctionName name = new FunctionName("testDb", "notnull_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, false, null, "com.example.NotNullFn", null, null);
        // Default NullableMode is DEPEND_ON_ARGUMENT

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.contains("\"ALWAYS_NULLABLE\"=\"false\""));
    }

    @Test
    void testScalarFunction_javaUdf_withPrepareFnAndCloseFn() {
        FunctionName name = new FunctionName("testDb", "prepared_fn");
        Type[] argTypes = {Type.DOUBLE};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.DOUBLE, false, null, "com.example.Fn", "com.example.Prepare", "com.example.Close");

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.contains("\"PREPARE_FN\"=\"com.example.Prepare\""));
        Assertions.assertTrue(sql.contains("\"CLOSE_FN\"=\"com.example.Close\""));
    }

    @Test
    void testScalarFunction_javaUdf_withoutPrepareFnAndCloseFn() {
        FunctionName name = new FunctionName("testDb", "simple_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, false, null, "sym", null, null);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertFalse(sql.contains("PREPARE_FN"));
        Assertions.assertFalse(sql.contains("CLOSE_FN"));
    }

    // ======================== ScalarFunction — IF NOT EXISTS ========================

    @Test
    void testScalarFunction_ifNotExists() {
        FunctionName name = new FunctionName("testDb", "my_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, false, null, "sym", null, null);

        String sql = FunctionToSqlConverter.toSql(fn, true);

        Assertions.assertTrue(sql.contains("IF NOT EXISTS"));
        Assertions.assertTrue(sql.startsWith("CREATE FUNCTION IF NOT EXISTS "));
    }

    // ======================== ScalarFunction — NATIVE ========================

    @Test
    void testScalarFunction_native_usesObjectFile() {
        FunctionName name = new FunctionName("testDb", "native_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.NATIVE, name, argTypes,
                Type.INT, false, null, "native_sym", null, null);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.contains("\"OBJECT_FILE\"="));
        Assertions.assertTrue(sql.contains("\"TYPE\"=\"NATIVE\""));
        // NATIVE uses OBJECT_FILE, not plain FILE — and should not have ALWAYS_NULLABLE
        Assertions.assertFalse(sql.contains("ALWAYS_NULLABLE"));
    }

    // ======================== ScalarFunction — GLOBAL ========================

    @Test
    void testScalarFunction_global() {
        FunctionName name = new FunctionName("testDb", "global_fn");
        Type[] argTypes = {Type.BIGINT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.BIGINT, false, null, "com.example.GlobalFn", null, null);
        fn.setGlobal(true);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.startsWith("CREATE GLOBAL FUNCTION "));
    }

    @Test
    void testScalarFunction_global_ifNotExists() {
        FunctionName name = new FunctionName("testDb", "global_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, false, null, "sym", null, null);
        fn.setGlobal(true);

        String sql = FunctionToSqlConverter.toSql(fn, true);

        Assertions.assertTrue(sql.startsWith("CREATE GLOBAL FUNCTION IF NOT EXISTS "));
    }

    // ======================== AggregateFunction — JAVA_UDF ========================

    @Test
    void testAggregateFunction_javaUdf_basicSql() {
        FunctionName name = new FunctionName("testDb", "my_sum");
        Type[] argTypes = {Type.BIGINT};
        AggregateFunction fn = AggregateFunction.AggregateFunctionBuilder.createUdfBuilder()
                .name(name)
                .argsType(argTypes)
                .retType(Type.BIGINT)
                .intermediateType(Type.BIGINT)
                .hasVarArgs(false)
                .symbolName("com.example.MySum")
                .build();

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.startsWith("CREATE AGGREGATE FUNCTION "));
        Assertions.assertTrue(sql.contains("my_sum(bigint)"));
        Assertions.assertTrue(sql.contains("RETURNS bigint"));
        Assertions.assertTrue(sql.contains("\"SYMBOL\"=\"com.example.MySum\""));
        Assertions.assertTrue(sql.contains("\"FILE\"=\"\""));
        Assertions.assertTrue(sql.contains("\"TYPE\"=\"JAVA_UDF\""));
        Assertions.assertTrue(sql.contains("\"ALWAYS_NULLABLE\"="));
        Assertions.assertFalse(sql.contains("INIT_FN"));
        Assertions.assertFalse(sql.contains("UPDATE_FN"));
        Assertions.assertFalse(sql.contains("MERGE_FN"));
        Assertions.assertFalse(sql.contains("IF NOT EXISTS"));
        Assertions.assertFalse(sql.contains("GLOBAL"));
    }

    @Test
    void testAggregateFunction_javaUdf_ifNotExists() {
        FunctionName name = new FunctionName("testDb", "my_agg");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = AggregateFunction.AggregateFunctionBuilder.createUdfBuilder()
                .name(name)
                .argsType(argTypes)
                .retType(Type.INT)
                .intermediateType(Type.INT)
                .hasVarArgs(false)
                .build();

        String sql = FunctionToSqlConverter.toSql(fn, true);

        Assertions.assertTrue(sql.contains("CREATE AGGREGATE FUNCTION IF NOT EXISTS "));
    }

    // ======================== AggregateFunction — NATIVE ========================

    @Test
    void testAggregateFunction_native_includesSymbolFunctions() {
        FunctionName name = new FunctionName("testDb", "native_agg");
        Type[] argTypes = {Type.BIGINT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.BIGINT, false,
                Type.BIGINT, null,
                "my_init", "my_update", "my_merge",
                "my_serialize", "my_finalize",
                "my_get_value", "my_remove");
        fn.setBinaryType(BinaryType.NATIVE);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.contains("\"INIT_FN\"=\"my_init\""));
        Assertions.assertTrue(sql.contains("\"UPDATE_FN\"=\"my_update\""));
        Assertions.assertTrue(sql.contains("\"MERGE_FN\"=\"my_merge\""));
        Assertions.assertTrue(sql.contains("\"SERIALIZE_FN\"=\"my_serialize\""));
        Assertions.assertTrue(sql.contains("\"FINALIZE_FN\"=\"my_finalize\""));
        Assertions.assertTrue(sql.contains("\"OBJECT_FILE\"="));
        Assertions.assertTrue(sql.contains("\"TYPE\"=\"NATIVE\""));
        // NATIVE uses OBJECT_FILE, not plain FILE
        Assertions.assertFalse(sql.contains("ALWAYS_NULLABLE"));
    }

    @Test
    void testAggregateFunction_native_withoutOptionalSymbols() {
        FunctionName name = new FunctionName("testDb", "simple_agg");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.INT, false,
                Type.INT, null,
                "init_fn", "update_fn", "merge_fn",
                null, null, null, null);
        fn.setBinaryType(BinaryType.NATIVE);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.contains("\"INIT_FN\"=\"init_fn\""));
        Assertions.assertTrue(sql.contains("\"UPDATE_FN\"=\"update_fn\""));
        Assertions.assertTrue(sql.contains("\"MERGE_FN\"=\"merge_fn\""));
        Assertions.assertFalse(sql.contains("SERIALIZE_FN"));
        Assertions.assertFalse(sql.contains("FINALIZE_FN"));
    }

    @Test
    void testAggregateFunction_withIntermediateType() {
        FunctionName name = new FunctionName("testDb", "agg_intermediate");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.BIGINT, false,
                Type.STRING, null,
                "init", "update", "merge",
                null, null, null, null);
        fn.setBinaryType(BinaryType.NATIVE);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        // intermediateType != retType, so it should be printed
        Assertions.assertTrue(sql.contains("INTERMEDIATE text"));
    }

    @Test
    void testAggregateFunction_withoutIntermediateType() {
        // When intermediateType == retType, the constructor sets it to null
        FunctionName name = new FunctionName("testDb", "agg_no_intermediate");
        Type[] argTypes = {Type.BIGINT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.BIGINT, false,
                Type.BIGINT, null,
                "init", "update", "merge",
                null, null, null, null);
        fn.setBinaryType(BinaryType.NATIVE);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertFalse(sql.contains("INTERMEDIATE"));
    }

    // ======================== AggregateFunction — GLOBAL ========================

    @Test
    void testAggregateFunction_global() {
        FunctionName name = new FunctionName("testDb", "global_agg");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = AggregateFunction.AggregateFunctionBuilder.createUdfBuilder()
                .name(name)
                .argsType(argTypes)
                .retType(Type.INT)
                .intermediateType(Type.INT)
                .hasVarArgs(false)
                .build();
        fn.setGlobal(true);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.startsWith("CREATE GLOBAL AGGREGATE FUNCTION "));
    }

    // ======================== Base Function → empty string ========================

    @Test
    void testBaseFunction_returnsEmptyString() {
        // A plain Function (not ScalarFunction/AggregateFunction/AliasFunction) should return ""
        Function fn = new Function(
                new FunctionName("testDb", "plain_fn"),
                Arrays.asList(Type.INT), Type.INT, false);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertEquals("", sql);
    }

    @Test
    void testBaseFunction_ifNotExists_returnsEmptyString() {
        Function fn = new Function(
                new FunctionName("testDb", "plain_fn"),
                Arrays.asList(Type.INT), Type.INT, false);

        String sql = FunctionToSqlConverter.toSql(fn, true);

        Assertions.assertEquals("", sql);
    }

    // ======================== Dispatcher Tests ========================

    @Test
    void testDispatcher_routesScalarFunction() {
        FunctionName name = new FunctionName("testDb", "dispatch_scalar");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, false, null, "sym", null, null);

        // Call via the dispatcher overload: toSql(Function, boolean)
        String sql = FunctionToSqlConverter.toSql((Function) fn, false);

        Assertions.assertTrue(sql.startsWith("CREATE FUNCTION "));
        Assertions.assertFalse(sql.contains("AGGREGATE"));
        Assertions.assertFalse(sql.contains("ALIAS"));
    }

    @Test
    void testDispatcher_routesAggregateFunction() {
        FunctionName name = new FunctionName("testDb", "dispatch_agg");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = AggregateFunction.AggregateFunctionBuilder.createUdfBuilder()
                .name(name)
                .argsType(argTypes)
                .retType(Type.INT)
                .intermediateType(Type.INT)
                .hasVarArgs(false)
                .build();

        // Call via the dispatcher overload: toSql(Function, boolean)
        String sql = FunctionToSqlConverter.toSql((Function) fn, false);

        Assertions.assertTrue(sql.startsWith("CREATE AGGREGATE FUNCTION "));
    }

    @Test
    void testDispatcher_baseFunctionReturnsEmpty() {
        Function fn = new Function(
                new FunctionName("testDb", "base_fn"),
                Arrays.asList(Type.INT), Type.INT, false);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertEquals("", sql);
    }

    // ======================== AggregateFunction — symbolName ========================

    @Test
    void testAggregateFunction_native_withSymbolName() {
        FunctionName name = new FunctionName("testDb", "sym_agg");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.INT, false,
                Type.INT, null,
                "init", "update", "merge",
                null, null, null, null);
        fn.setBinaryType(BinaryType.NATIVE);
        fn.setSymbolName("native_sym");

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.contains("\"SYMBOL\"=\"native_sym\""));
    }

    @Test
    void testAggregateFunction_withNullSymbolName() {
        FunctionName name = new FunctionName("testDb", "nosym_agg");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = new AggregateFunction(
                name, argTypes, Type.INT, false,
                Type.INT, null,
                "init", "update", "merge",
                null, null, null, null);
        fn.setBinaryType(BinaryType.NATIVE);
        // symbolName is null by default

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertFalse(sql.contains("SYMBOL"));
    }

    // ======================== End-to-end SQL structure ========================

    @Test
    void testScalarFunction_sqlEndsWithSemicolon() {
        FunctionName name = new FunctionName("testDb", "end_fn");
        Type[] argTypes = {Type.INT};
        ScalarFunction fn = ScalarFunction.createUdf(BinaryType.JAVA_UDF, name, argTypes,
                Type.INT, false, null, "sym", null, null);

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.endsWith(");"));
    }

    @Test
    void testAggregateFunction_sqlEndsWithSemicolon() {
        FunctionName name = new FunctionName("testDb", "end_agg");
        Type[] argTypes = {Type.INT};
        AggregateFunction fn = AggregateFunction.AggregateFunctionBuilder.createUdfBuilder()
                .name(name)
                .argsType(argTypes)
                .retType(Type.INT)
                .intermediateType(Type.INT)
                .hasVarArgs(false)
                .build();

        String sql = FunctionToSqlConverter.toSql(fn, false);

        Assertions.assertTrue(sql.endsWith(");"));
    }
}
