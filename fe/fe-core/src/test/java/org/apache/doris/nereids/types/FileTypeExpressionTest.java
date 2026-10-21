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

package org.apache.doris.nereids.types;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionVolatility;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.rules.analysis.CheckAnalysis;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.ImplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectList;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.MapAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.MapAggV2;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.MaxBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinBy;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeFile;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeFileOuter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayCompact;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayDistinct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMax;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayReverseSort;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySortBy;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdaf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdtf;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFunctionBinaryType;
import org.apache.doris.thrift.TResultSinkType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

public class FileTypeExpressionTest {
    private DataType type(String sql) {
        return DataType.convertFromString(sql);
    }

    private void assertAnalysisError(String expectedMessage, Executable executable, String... context) {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class, executable,
                String.join(" ", context));
        Assertions.assertTrue(error.getMessage().contains(expectedMessage),
                () -> String.join(" ", context) + ": expected " + expectedMessage + "; actual " + error.getMessage());
    }

    @Test
    public void testFlightRejectsOnlyFileResultColumns() {
        ConnectContext previous = ConnectContext.get();
        try {
            ConnectContext flight = MemoTestUtils.createConnectContext();
            flight.setResultSinkType(TResultSinkType.ARROW_FLIGHT_PROTOCOL);
            flight.setCurrentUserIdentity(UserIdentity.ROOT);
            flight.setRemoteIP("127.0.0.1");
            flight.setEnv(Env.getCurrentEnv());
            flight.setThreadLocalInfo();
            String file = "cast(named_struct('uri', 'urn:x', 'offset', null, 'size', null, "
                    + "'content_type', null, 'checksum', null, 'inline', null) as file)";
            for (String value : Arrays.asList("cast(null as file)", file, "array(" + file + ")",
                    "named_struct('f', " + file + ")", "map('key', " + file + ")")) {
                String sql = "select " + value;
                assertAnalysisError("Arrow Flight does not support FILE result columns",
                        () -> PlanChecker.from(flight).analyze(sql), sql);
                Assertions.assertDoesNotThrow(
                        () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql), sql);
                flight.setThreadLocalInfo();
            }
            for (String sql : Arrays.asList("select 1", "select named_struct('s', 'text')",
                    "select element_at(" + file + ", 'uri')",
                    "select to_json(" + file + ")")) {
                Assertions.assertDoesNotThrow(() -> PlanChecker.from(flight).analyze(sql), sql);
            }
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testFileChecksDoNotReconstructPersistedDecimalArithmeticTypes() {
        ConnectContext previous = ConnectContext.get();
        try {
            ConnectContext context = MemoTestUtils.createConnectContext();
            context.getSessionVariable().enableDecimal256 = false;
            Divide divide = new Divide(
                    new SlotReference("wide", DecimalV3Type.createDecimalV3TypeNotCheck256(52, 20)),
                    new SlotReference("divisor", DecimalV3Type.createDecimalV3Type(38, 10)));
            Alias alias = new Alias(divide, "quotient");
            LogicalOneRowRelation relation = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                    Collections.singletonList(alias));
            Assertions.assertThrows(NotSupportedException.class, divide::getDataType);
            Assertions.assertDoesNotThrow(divide::checkInputDataTypes);
            Assertions.assertDoesNotThrow(alias::checkInputDataTypes);
            Assertions.assertDoesNotThrow(() -> CheckAnalysis.checkFilePlan(relation));
            Assertions.assertFalse(context.getSessionVariable().isEnableDecimal256());
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testFileChecksDoNotComputeUncoercedNonFileResults() {
        for (String function : Arrays.asList("array_max", "array_min")) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + function + "(null)"));
        }
    }

    @Test
    public void testFunctionsKeepTheirNativeTypeRestrictions() {
        for (String expression : Arrays.asList("array_distinct(files)", "array_sort(files)")) {
            String expectedMessage;
            if (expression.startsWith("array_distinct(")) {
                expectedMessage = "array_distinct does not support element type";
            } else {
                expectedMessage = "array_sort does not support types: ARRAY<FILE>";
            }
            assertAnalysisError(expectedMessage,
                    () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression + " from (select 1 id, cast(null as file) f, "
                            + "cast(null as array<file>) files) t"), expression);
        }
        for (String expression : Arrays.asList("first_value(f) over(order by id)", "last_value(f) over(order by id)",
                "nth_value(f, 1) over(order by id)", "array_sortby(files, array(id))")) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression + " from (select 1 id, cast(null as file) f, "
                            + "cast(null as array<file>) files) t"), expression);
        }
    }

    @Test
    public void testArrayComparisonFunctionsRejectNestedFile() {
        for (String sqlType : Arrays.asList("ARRAY<FILE>", "ARRAY<ARRAY<FILE>>",
                "ARRAY<STRUCT<f:FILE>>", "ARRAY<MAP<STRING,FILE>>")) {
            Expression array = new SlotReference("files", type(sqlType));
            for (BoundFunction function : Arrays.asList(new ArrayReverseSort(array), new ArrayCompact(array))) {
                String expected = function.getName() + " does not support type";
                assertAnalysisError(expected, () -> TypeCoercionUtils.processBoundFunction(function), sqlType);
                String sql = "select " + function.getName() + "(files) from (select cast(null as "
                        + sqlType + ") files) t";
                assertAnalysisError(expected,
                        () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql), sql);
            }
        }
        for (String sqlType : Arrays.asList("ARRAY<INT>", "ARRAY<ARRAY<INT>>")) {
            Expression array = new SlotReference("values", type(sqlType));
            for (BoundFunction function : Arrays.asList(new ArrayReverseSort(array), new ArrayCompact(array))) {
                Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(function),
                        function.getName() + " " + sqlType);
            }
        }
    }

    @Test
    public void testHigherOrderFunctionsUseNativeSignatures() {
        String from = " from (select 1 id, cast(null as file) f, cast(null as array<file>) files) t";
        for (String expression : Arrays.asList("array_map(x -> x, files)",
                "array_map(x -> element_at(x,'size'), files)",
                "array_filter(x -> element_at(x,'size') > 0, files)",
                "array_map(x -> f, array(id))", "array_map((x,y) -> x, array(id), files)")) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression + from), expression);
        }
        for (String expression : Arrays.asList("array_map(x -> x + 1, array(id))",
                "array_map(x -> element_at(f,'size') + x, array(id))")) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression + from), expression);
        }
    }

    @Test
    public void testAliasFunctionsUseDeclaredSignatures() {
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            AliasUdf alias = new AliasUdf("file_alias", Collections.singletonList(type(sqlType)),
                    new NullLiteral(), Collections.singletonList("f"), Collections.emptyMap(), new NullLiteral());
            Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(alias), sqlType);
        }
        AliasUdf ordinary = new AliasUdf("ordinary_alias", Collections.singletonList(BigIntType.INSTANCE),
                new NullLiteral(), Collections.singletonList("n"), Collections.emptyMap(), new NullLiteral());
        Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(ordinary));
    }

    @Test
    public void testPublicFieldSelectors() {
        Expression file = new SlotReference("f", FileType.INSTANCE);
        Assertions.assertEquals(BigIntType.INSTANCE,
                TypeCoercionUtils.processBoundFunction(new ElementAt(file, new StringLiteral("SIZE"))).getDataType());
        Assertions.assertTrue(new ElementAt(file, new StringLiteral("uri")).nullable());
        Assertions.assertEquals(VarBinaryType.INSTANCE,
                TypeCoercionUtils.processBoundFunction(new ElementAt(file, new StringLiteral("INLINE"))).getDataType());
        Assertions.assertTrue(new ElementAt(file, new StringLiteral("inline")).nullable());
        for (Expression selector : Arrays.asList(new StringLiteral("path"),
                new IntegerLiteral(1), new SlotReference("s", StringType.INSTANCE))) {
            assertAnalysisError(selector instanceof StringLiteral
                    ? "the specified field name path was not found"
                    : "element_at over FILE requires a constant public field name",
                    () -> TypeCoercionUtils.processBoundFunction(new ElementAt(file, selector)));
        }
    }

    @Test
    public void testFileJsonCastsAreUnsupported() {
        for (boolean strict : Arrays.asList(true, false)) {
            for (String wrapper : Arrays.asList("%s", "ARRAY<%s>", "STRUCT<f:%s>", "MAP<STRING,%s>")) {
                DataType file = type(String.format(wrapper, "FILE"));
                DataType json = type(String.format(wrapper, "JSON"));
                Assertions.assertFalse(CheckCast.check(file, json, strict));
                Assertions.assertFalse(CheckCast.check(json, file, strict));
                Assertions.assertFalse(CheckCast.check(file, JsonType.INSTANCE, strict));
                Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, file, strict));
            }
        }
    }

    @Test
    public void testToJsonAcceptsFile() {
        for (String value : Arrays.asList("cast(null as file)",
                "cast(named_struct('uri', 'urn:x', 'offset', null, 'size', null, "
                        + "'content_type', null, 'checksum', null, 'inline', null) as file)")) {
            Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select to_json(" + value + ")").getPlan();
            Assertions.assertEquals(JsonType.INSTANCE, plan.getOutput().get(0).getDataType());
        }
    }

    @Test
    public void testFileCastUsesOrdinaryTypeChecksAndNullLiteralCoercion() {
        Assertions.assertFalse(Cast.castNullable(false, FileType.INSTANCE.publicStructType(), FileType.INSTANCE));
        Assertions.assertTrue(Cast.castNullable(true, FileType.INSTANCE.publicStructType(), FileType.INSTANCE));
        Assertions.assertTrue(Cast.castNullable(false, JsonType.INSTANCE, FileType.INSTANCE));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, FileType.INSTANCE, true));
        Expression converted = TypeCoercionUtils.castIfNotSameType(new NullLiteral(FileType.INSTANCE),
                JsonType.INSTANCE);
        Assertions.assertTrue(converted instanceof NullLiteral);
        Assertions.assertEquals(JsonType.INSTANCE, converted.getDataType());
        Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.castIfNotSameType(
                new SlotReference("s", FileType.INSTANCE.publicStructType()), FileType.INSTANCE));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, FileType.INSTANCE, false));
    }

    @Test
    public void testStructuredCastsAndNoStringParsing() {
        String metadata = "uri:STRING,offset:BIGINT,size:BIGINT,content_type:STRING,checksum:STRING";
        String fields = metadata + ",inline:VARBINARY";
        for (boolean strict : Arrays.asList(true, false)) {
            for (String source : Arrays.asList("STRUCT<" + fields + ">",
                    "STRUCT<inline:VARBINARY,checksum:VARCHAR(10),size:BIGINT,uri:CHAR(10),"
                            + "content_type:STRING,offset:BIGINT>")) {
                DataType value = type(source);
                Assertions.assertTrue(CheckCast.check(value, FileType.INSTANCE, strict), source);
                Assertions.assertTrue(CheckCast.check(FileType.INSTANCE, value, strict), source);
                Assertions.assertTrue(TypeCoercionUtils.implicitCast(value, FileType.INSTANCE).isPresent());
                Assertions.assertTrue(TypeCoercionUtils.implicitCast(FileType.INSTANCE, value).isPresent());
            }
            for (String source : Arrays.asList("STRING", "JSON", "VARIANT", "INT", "STRUCT<uri:STRING>",
                    "STRUCT<" + metadata + ">",
                    "STRUCT<" + metadata + ",inline:STRING>",
                    "STRUCT<" + fields + ",extra:STRING>",
                    "STRUCT<" + metadata + ",payload:VARBINARY>",
                    "STRUCT<uri:STRING,offset:INT,size:BIGINT,content_type:STRING,checksum:STRING,inline:VARBINARY>")) {
                Assertions.assertFalse(CheckCast.check(type(source), FileType.INSTANCE, strict), source);
                Assertions.assertFalse(CheckCast.check(FileType.INSTANCE, type(source), strict), source);
            }
            Assertions.assertTrue(CheckCast.check(type("ARRAY<STRUCT<" + fields + ">>"),
                    type("ARRAY<FILE>"), strict));
            Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, type("ARRAY<STRUCT<" + fields + ">>"), strict));
            Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, type("ARRAY<FILE>"), strict));
        }
        StructType nullableInline = new StructType(Arrays.asList(
                new StructField("uri", StringType.INSTANCE, false, ""),
                new StructField("offset", BigIntType.INSTANCE, true, ""),
                new StructField("size", BigIntType.INSTANCE, true, ""),
                new StructField("content_type", StringType.INSTANCE, true, ""),
                new StructField("checksum", StringType.INSTANCE, true, ""),
                new StructField("inline", NullType.INSTANCE, true, "")));
        Assertions.assertTrue(CheckCast.check(nullableInline, FileType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(nullableInline, FileType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(FileType.INSTANCE, nullableInline, true));
        StructType requiredInline = new StructType(FileType.INSTANCE.getFields().stream()
                .map(field -> field.getName().equals("inline")
                        ? new StructField("inline", VarBinaryType.INSTANCE, false, "") : field)
                .collect(java.util.stream.Collectors.toList()));
        Assertions.assertTrue(CheckCast.check(requiredInline, FileType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(FileType.INSTANCE, requiredInline, true));
        Assertions.assertFalse(CheckCast.check(FileType.INSTANCE, requiredInline, false));
        Assertions.assertTrue(TypeCoercionUtils.implicitCast(NullType.INSTANCE, FileType.INSTANCE).isPresent());
        Assertions.assertTrue(TypeCoercionUtils.implicitCast(FileType.INSTANCE, FileType.INSTANCE).isPresent());
    }

    @Test
    public void testNestedFileCastsRejectStringAndWholeVariantContainers() {
        for (boolean strict : Arrays.asList(true, false)) {
            for (String container : Arrays.asList("ARRAY<FILE>", "MAP<STRING,FILE>", "STRUCT<f:FILE>",
                    "ARRAY<STRUCT<f:FILE>>", "STRUCT<f:ARRAY<FILE>>", "MAP<STRING,ARRAY<FILE>>")) {
                DataType nested = type(container);
                for (String source : Arrays.asList("STRING", "VARCHAR(20)", "CHAR(20)", "VARIANT")) {
                    Assertions.assertFalse(CheckCast.check(type(source), nested, strict), source + " -> " + container);
                }
                Assertions.assertFalse(CheckCast.check(nested, VariantType.INSTANCE, strict),
                        container + " -> VARIANT");
                Assertions.assertTrue(CheckCast.check(NullType.INSTANCE, nested, strict), "NULL -> " + container);
                Assertions.assertTrue(CheckCast.check(nested, nested, strict), container);
            }
            for (String wrapper : Arrays.asList("ARRAY<%s>", "MAP<STRING,%s>", "STRUCT<f:%s>")) {
                for (String leaf : Arrays.asList("STRING", "VARIANT")) {
                    Assertions.assertFalse(CheckCast.check(type(String.format(wrapper, leaf)),
                            type(String.format(wrapper, "ARRAY<FILE>")), strict), wrapper + leaf);
                }
                Assertions.assertFalse(CheckCast.check(type(String.format(wrapper, "VARIANT")),
                        type(String.format(wrapper, "FILE")), strict), wrapper);
                Assertions.assertFalse(CheckCast.check(type(String.format(wrapper, "FILE")),
                        type(String.format(wrapper, "VARIANT")), strict), wrapper);
            }
            Assertions.assertTrue(CheckCast.check(VariantType.INSTANCE, type("ARRAY<INT>"), strict));
        }
    }

    @Test
    public void testConditionalFunctionsAcceptFileAndRejectMixedTypes() {
        Expression file = new SlotReference("f", FileType.INSTANCE);
        Expression structured = new SlotReference("s", type("STRUCT<uri:STRING>"));
        assertAnalysisError("Can not find the compatibility function signature:",
                () -> TypeCoercionUtils.processBoundFunction(new If(BooleanLiteral.TRUE, file, structured)));
        assertAnalysisError("Can not find the compatibility function signature:",
                () -> TypeCoercionUtils.processBoundFunction(new Coalesce(structured, file)));
        for (DataType valueType : Arrays.asList(FileType.INSTANCE, type("ARRAY<FILE>"),
                type("STRUCT<f:FILE>"), type("MAP<STRING,FILE>"))) {
            Expression value = new SlotReference("v", valueType);
            for (BoundFunction function : Arrays.asList(new Coalesce(new NullLiteral(), value),
                    new Coalesce(value, new NullLiteral(valueType)),
                    new Coalesce(new NullLiteral(valueType), new NullLiteral()),
                    new If(BooleanLiteral.TRUE, value, value),
                    new If(BooleanLiteral.TRUE, new NullLiteral(), value),
                    new If(BooleanLiteral.FALSE, new NullLiteral(valueType), new NullLiteral()),
                    new Nvl(value, new NullLiteral()), new Nvl(new NullLiteral(), value))) {
                Assertions.assertEquals(valueType,
                        TypeCoercionUtils.processBoundFunction(function).getDataType(), function.toSql());
            }
        }
    }

    @Test
    public void testSqlFileValuePropagationAndContainerConstruction() {
        String from = " from (select 1 id, cast(null as file) f, cast(null as array<file>) files, "
                + "cast(null as struct<f:file>) holder, cast(null as map<string,file>) lookup) t";
        for (String expression : Arrays.asList("array(f, null)", "struct(f)", "named_struct('asset', f)",
                "map('asset', f)", "array(holder, null)", "map('asset', files)",
                "reverse(files)", "array_slice(files, 1, 1)", "array_concat(files, files)",
                "map_values(lookup)", "map_keys(lookup)", "array_size(files)", "map_size(lookup)",
                "cardinality(files)", "cardinality(lookup)",
                "case when id > 0 then f else null end")) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression + from), expression);
        }
        for (String expression : Arrays.asList("map('asset', f) = map('asset', f)",
                "array_distinct(array(f))")) {
            String expectedMessage;
            if (expression.contains(" = ")) {
                expectedMessage = "comparison predicate could not contains complex type";
            } else {
                expectedMessage = "array_distinct does not support element type";
            }
            assertAnalysisError(expectedMessage,
                    () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression + from), expression);
        }
        String inline = "cast(named_struct('uri', null, 'offset', null, 'size', null, "
                + "'content_type', null, 'checksum', null, 'inline', X'00FF80') as file)";
        for (String expression : Arrays.asList("coalesce(cast(null as file), " + inline + ")",
                "if(true, " + inline + ", null)", "ifnull(null, " + inline + ")",
                "element_at(element_at(array(" + inline + ", null), 1), 'inline')",
                "element_at(element_at(named_struct('asset', " + inline + "), 'asset'), 'inline')",
                "element_at(element_at(map('asset', " + inline + "), 'asset'), 'inline')")) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression), expression);
        }
    }

    @Test
    public void testRecursiveComparisonGates() {
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            Expression value = new SlotReference("v", type(sql));
            assertAnalysisError(value.getDataType().isArrayType()
                    ? "data type " + sql + " could not used in ComparisonPredicate"
                    : "comparison predicate could not contains complex type",
                    () -> TypeCoercionUtils.processComparisonPredicate(new EqualTo(value, value)));
            assertAnalysisError("does not support FILE",
                    () -> TypeCoercionUtils.processInPredicate(
                            new InPredicate(value, Collections.singletonList(value))));
            assertAnalysisError("does not support FILE",
                    () -> ExpressionUtils.checkFileKey(value, "ORDER BY"));
        }
    }

    @Test
    public void testInPredicateRejectsOrdinaryStruct() {
        for (String sqlType : Arrays.asList("STRUCT<n:BIGINT>", "STRUCT<m:MAP<STRING,INT>>",
                "STRUCT<j:JSON>", "STRUCT<a:ARRAY<JSON>>")) {
            Expression value = new SlotReference("s", type(sqlType));
            InPredicate predicate = new InPredicate(value, Collections.singletonList(value));
            assertAnalysisError("in predicate could not contains complex type",
                    () -> TypeCoercionUtils.processInPredicate(predicate), sqlType);
        }
    }

    @Test
    public void testInPredicateRejectsFileInAnyArgument() {
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>",
                "STRUCT<a:ARRAY<FILE>>", "MAP<STRING,FILE>")) {
            Expression value = new SlotReference("f", type(sqlType));
            assertAnalysisError("IN does not support FILE",
                    () -> TypeCoercionUtils.processInPredicate(
                            new InPredicate(value, Collections.singletonList(value))), sqlType);
            assertAnalysisError("IN does not support FILE",
                    () -> TypeCoercionUtils.processInPredicate(
                            new InPredicate(new NullLiteral(), Collections.singletonList(value))), sqlType);
        }
    }

    @Test
    public void testElementAtExplodeAndReverseCastFields() {
        Expression file = new SlotReference("f", FileType.INSTANCE);
        for (StructField field : FileType.INSTANCE.getFields()) {
            Expression accessed = ExpressionAnalyzer.analyzeFunction(null, null,
                    new ElementAt(file, new StringLiteral(field.getName().toUpperCase(Locale.ROOT))));
            Assertions.assertTrue(accessed instanceof ElementAt);
            Assertions.assertEquals(field.getName().toUpperCase(Locale.ROOT),
                    ((StringLiteral) accessed.child(1)).getStringValue());
            Assertions.assertEquals(field.getDataType(), accessed.getDataType());
            Assertions.assertTrue(accessed.nullable());
        }
        ElementAt inline = new ElementAt(new SlotReference("required", FileType.INSTANCE, false),
                new StringLiteral("inline"));
        Assertions.assertEquals(VarBinaryType.INSTANCE, inline.getDataType());
        Assertions.assertTrue(inline.nullable());
        for (BoundFunction explode : Arrays.asList(new ExplodeFile(file), new ExplodeFileOuter(file))) {
            Assertions.assertEquals(FileType.INSTANCE.publicStructType(),
                    TypeCoercionUtils.processBoundFunction(explode).getDataType());
        }
        Cast struct = new Cast(file, FileType.INSTANCE.publicStructType(), true);
        Expression accessed = TypeCoercionUtils.processBoundFunction(new ElementAt(struct, new StringLiteral("INLINE")));
        Assertions.assertTrue(accessed instanceof ElementAt);
        Assertions.assertSame(struct, accessed.child(0));
        Assertions.assertEquals(VarBinaryType.INSTANCE, accessed.getDataType());
        Assertions.assertEquals("INLINE", ((StringLiteral) accessed.child(1)).getStringValue());
    }

    @Test
    public void testFunctionParameterAndAssignmentBoundaries() {
        DataType struct = type("STRUCT<uri:STRING>");
        for (DataType source : Arrays.asList(struct, JsonType.INSTANCE)) {
            Assertions.assertFalse(ImplicitlyCastableSignature.isImplicitlyCastable(FileType.INSTANCE, source));
            Assertions.assertFalse(ExplicitlyCastableSignature.isExplicitlyCastable(FileType.INSTANCE, source));
        }
        Assertions.assertEquals(ExplicitlyCastableSignature.isExplicitlyCastable(
                StringType.INSTANCE, FileType.INSTANCE.publicStructType()),
                ExplicitlyCastableSignature.isExplicitlyCastable(StringType.INSTANCE, FileType.INSTANCE));
        Assertions.assertFalse(ImplicitlyCastableSignature.isImplicitlyCastable(
                FileType.INSTANCE, StringType.INSTANCE));
        Assertions.assertTrue(ImplicitlyCastableSignature.isImplicitlyCastable(
                type("STRUCT<f:STRING>"), type("STRUCT<f:FILE>")));
        Assertions.assertTrue(ExplicitlyCastableSignature.isExplicitlyCastable(
                type("STRUCT<f:STRING>"), type("STRUCT<f:FILE>")));
        Assertions.assertTrue(ImplicitlyCastableSignature.isImplicitlyCastable(
                type("STRUCT<f:FILE>"), type("STRUCT<f:STRUCT<uri:STRING>>")));
        assertAnalysisError("can not cast from origin type FILE to target type=TEXT",
                () -> TypeCoercionUtils.castIfNotSameType(
                        new SlotReference("f", FileType.INSTANCE), StringType.INSTANCE));
    }

    @Test
    public void testGenericAggregatesUseNativeSignatures() {
        Expression key = new IntegerLiteral(1);
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            Expression file = new SlotReference("f", type(sqlType));
            for (BoundFunction function : Arrays.asList(new Count(file), new AnyValue(file), new ArrayAgg(file),
                    new CollectList(file), new MinBy(file, key), new MaxBy(file, key), new MapAggV2(key, file))) {
                Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(function),
                        function.getName() + " " + sqlType);
            }
            assertAnalysisError("COUNT DISTINCT could not process type",
                    () -> new Count(true, file).checkLegalityAfterRewrite(), sqlType);
            assertAnalysisError("array_distinct does not support element type",
                    () -> TypeCoercionUtils.processBoundFunction(
                            new ArrayDistinct(new SlotReference("a", ArrayType.of(file.getDataType())))), sqlType);
        }
        Expression file = new SlotReference("f", FileType.INSTANCE);
        assertAnalysisError("column must use with specific function",
                () -> TypeCoercionUtils.processBoundFunction(new Min(file)));
        assertAnalysisError("column must use with specific function",
                () -> TypeCoercionUtils.processBoundFunction(new MinBy(key, file)));
    }

    @Test
    public void testFileComparatorFunctionsRejectNestedKeys() {
        Expression key = new IntegerLiteral(1);
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            Expression file = new SlotReference("f", type(sqlType));
            for (BoundFunction function : Arrays.asList(new Min(file), new Max(file),
                    new MinBy(key, file), new MaxBy(key, file))) {
                assertAnalysisError("column must use with specific function",
                        () -> TypeCoercionUtils.processBoundFunction(function), function.getName() + " " + sqlType);
            }
            Expression files = new SlotReference("files", ArrayType.of(file.getDataType()));
            assertAnalysisError("array_sortby key does not support FILE",
                    () -> TypeCoercionUtils.processBoundFunction(new ArraySortBy(
                            new SlotReference("values", ArrayType.of(IntegerType.INSTANCE)), files)), sqlType);
            for (BoundFunction function : Arrays.asList(new MapAgg(file, key), new MapAggV2(file, key))) {
                assertAnalysisError("MAP_AGG key does not support FILE",
                        () -> TypeCoercionUtils.processBoundFunction(function), sqlType);
            }
            assertAnalysisError("array_min does not support complex types",
                    () -> TypeCoercionUtils.processBoundFunction(new ArrayMin(files)), sqlType);
            assertAnalysisError("array_max does not support complex types",
                    () -> TypeCoercionUtils.processBoundFunction(new ArrayMax(files)), sqlType);
        }
    }

    @Test
    public void testGenericAggregateSqlAdmission() {
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            String input = " from (select cast(null as " + sqlType + ") f) t";
            for (String aggregate : Arrays.asList("count(f)", "count(*)", "any_value(f)", "array_agg(f)",
                    "collect_list(f)", "group_array(f)", "min_by(f, 1)", "max_by(f, 1)", "map_agg(1, f)")) {
                Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                        .analyze("select " + aggregate + input), aggregate + " " + sqlType);
            }
        }
    }

    @Test
    public void testPythonFunctionsUseDeclaredSignaturesAtBinding() {
        for (DataType fileType : Arrays.asList(FileType.INSTANCE, type("ARRAY<FILE>"),
                type("STRUCT<f:FILE>"), type("MAP<STRING,FILE>"))) {
            for (String kind : Arrays.asList("scalar", "aggregate", "table")) {
                Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(
                        pythonFunction(kind, fileType, IntegerType.INSTANCE, IntegerType.INSTANCE)));
                Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(
                        (BoundFunction) pythonFunction(kind, fileType, IntegerType.INSTANCE, IntegerType.INSTANCE)
                                .withChildren(Collections.singletonList(new NullLiteral()))));
                Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(
                        pythonFunction(kind, IntegerType.INSTANCE, fileType, IntegerType.INSTANCE)));
            }
            Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(
                    pythonFunction("aggregate", IntegerType.INSTANCE, IntegerType.INSTANCE, fileType)));
        }
    }

    @Test
    public void testPythonFunctionsStillAcceptOrdinaryTypesAtBinding() {
        for (DataType type : Arrays.asList(IntegerType.INSTANCE, StringType.INSTANCE, type("ARRAY<INT>"),
                type("STRUCT<v:INT>"), type("MAP<STRING,INT>"))) {
            for (String kind : Arrays.asList("scalar", "aggregate", "table")) {
                Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(
                        pythonFunction(kind, type, type, type)));
            }
            Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(
                    pythonFunction("aggregate", type, type, null)));
        }
    }

    private BoundFunction pythonFunction(String kind, DataType argumentType, DataType returnType,
            DataType intermediateType) {
        Expression argument = new SlotReference("v", argumentType);
        FunctionSignature signature = FunctionSignature.ret(
                kind.equals("table") ? ArrayType.of(returnType) : returnType).args(argumentType);
        if (kind.equals("aggregate")) {
            return new PythonUdaf("py_agg", 1, "db1", TFunctionBinaryType.PYTHON_UDF,
                    signature, intermediateType, Function.NullableMode.ALWAYS_NULLABLE,
                    FunctionVolatility.IMMUTABLE, Udf.createVolatileIdentity(FunctionVolatility.IMMUTABLE),
                    null, "Agg", null, null, null, null, null, null, null, false, "", false, 360,
                    "3.10.2", "", argument);
        }
        if (kind.equals("table")) {
            return new PythonUdtf("py_table", 1, "db1", TFunctionBinaryType.PYTHON_UDF,
                    signature, Function.NullableMode.ALWAYS_NULLABLE, FunctionVolatility.IMMUTABLE,
                    Udf.createVolatileIdentity(FunctionVolatility.IMMUTABLE),
                    null, "evaluate", null, null, "", false, 360, "3.10.2", "", argument);
        }
        return new PythonUdf("py_scalar", 1, "db1", TFunctionBinaryType.PYTHON_UDF,
                signature, Function.NullableMode.ALWAYS_NULLABLE, FunctionVolatility.IMMUTABLE,
                Udf.createVolatileIdentity(FunctionVolatility.IMMUTABLE),
                null, "evaluate", null, null, "", false, 360, "3.10.2", "", argument);
    }

    @Test
    public void testSqlValueTransportAndExplodeAliases() {
        for (String sql : Arrays.asList(
                "select element_at(cast(null as file), 'uri')",
                "select element_at(cast(null as file), 'inline')",
                "select element_at(cast(null as file), 'INLINE')",
                "select element_at(cast(null as file), 'SIZE')",
                "select cast(null as file) union all select cast(named_struct('uri', 'urn:x', 'offset', cast(0 "
                        + "as bigint), 'size', cast(0 as bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null) as file)",
                "select cast(named_struct('uri', 'urn:x', 'offset', cast(0 as bigint), 'size', cast(0 as "
                        + "bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null) as file) union all "
                        + "select cast(named_struct('uri', 'urn:y', 'offset', null, 'size', null, "
                        + "'content_type', null, 'checksum', null, 'inline', null) as file) "
                        + "union all select cast(null as file)",
                "select cast(null as array<file>) union all "
                        + "select cast(array(named_struct('uri', 'urn:x', 'offset', cast(0 as bigint), 'size', "
                        + "cast(0 as bigint), 'content_type', '', 'checksum', '', 'inline', null)) as array<file>)",
                "select count(f), count(*) "
                        + "from (select cast(null as file) f) t",
                "select any_value(distinct f) from (select cast(null as file) f) t",
                "select element_at(f, 'uri'), count(*) from (select cast(null as file) f) t "
                        + "group by element_at(f, 'uri')",
                "select u, o, s, c, h, b from (select cast(null as file) f) t "
                        + "lateral view explode_file(f) e as u, o, s, c, h, b",
                "select u, o, s, c, h, b from (select cast(null as file) f) t "
                        + "lateral view explode_file_outer(f) e as u, o, s, c, h, b")) {
            Assertions.assertDoesNotThrow(
                    () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql), sql);
        }
    }

    @Test
    public void testInlineAndExplodeSqlOutputSchema() {
        Plan getters = PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select element_at(f, 'inline'), element_at(f,'INLINE') "
                        + "from (select cast(null as file) f) t").getPlan();
        Assertions.assertEquals(2, getters.getOutput().size());
        getters.getOutput().forEach(output -> {
            Assertions.assertEquals(VarBinaryType.INSTANCE, output.getDataType());
            Assertions.assertTrue(output.nullable());
        });
        for (String function : Arrays.asList("explode_file", "explode_file_outer")) {
            Plan expanded = PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                    "select u, o, s, c, h, b from (select cast(null as file) f) t lateral view "
                            + function + "(f) e as u, o, s, c, h, b").getPlan();
            Assertions.assertEquals(6, expanded.getOutput().size());
            Assertions.assertEquals(VarBinaryType.INSTANCE, expanded.getOutput().get(5).getDataType());
            Assertions.assertTrue(expanded.getOutput().stream().allMatch(Expression::nullable));
        }
    }

    @Test
    public void testUnionUsesExplicitFileTargets() {
        for (String sql : Arrays.asList(
                "select cast(named_struct('uri', 'urn:x', 'offset', cast(0 as bigint), 'size', cast(0 as "
                        + "bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null) as file) "
                        + "union all select cast(null as file)",
                "select cast(null as file) union all select cast(named_struct('uri', 'urn:x', 'offset', cast(0 "
                        + "as bigint), 'size', cast(0 as bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null) as file) "
                        + "union all select cast(named_struct('uri', 'urn:z', 'offset', cast(0 as bigint), "
                        + "'size', cast(0 as bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null) as file)")) {
            Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).getPlan();
            Assertions.assertEquals(FileType.INSTANCE, plan.getOutput().get(0).getDataType());
        }
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select 1 union all select 2.5 union all select null union all select 3"));
        assertAnalysisError("Can not find compatible type", () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select cast(null as file) union all select named_struct('uri', 'urn:x', 'offset', "
                        + "cast(0 as bigint), 'size', cast(0 as bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null)"));
    }

    @Test
    public void testSqlOperatorRejections() {
        for (String sql : Arrays.asList(
                "select cast(null as file) = cast(null as file)",
                "select cast(null as struct<f:file>) in (cast(null as struct<f:file>))",
                "select f from (select cast(null as file) f) t order by f",
                "select f from (select cast(null as file) f) t group by f",
                "select cast(null as file) union select cast(null as file)",
                "select cast(null as file) intersect select cast(null as file)",
                "select min(cast(null as file))",
                "select element_at(cast(null as file), 'missing')",
                "select u from (select cast(null as file) f) t lateral view explode_file(f) e as u",
                "select u from (select cast(null as file) f) t lateral view explode_file(f) e as u, o, s, c, h",
                "select u from (select cast(null as file) f) t "
                        + "lateral view explode_file_outer(f) e as u, o, s, c, h")) {
            String expectedMessage = "does not support FILE";
            if (sql.contains("lateral view")) {
                expectedMessage = "EXPLODE_FILE requires exactly six column aliases";
            } else if (sql.contains("missing")) {
                expectedMessage = "the specified field name missing was not found";
            } else if (sql.contains(" = ")) {
                expectedMessage = "comparison predicate could not contains complex type";
            } else if (sql.startsWith("select min(")) {
                expectedMessage = "column must use with specific function";
            }
            assertAnalysisError(expectedMessage,
                    () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql), sql);
        }
    }

    @Test
    public void testScalarWindowFrameKeepsExistingAnalysis() {
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select sum(v) over(order by v rows between unbounded preceding and current row) "
                        + "from (select 1 v) t"));
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select last_value(f) over(order by id rows between unbounded preceding and current row) "
                        + "from (select 1 id, cast(null as file) f) t"));
    }
}
