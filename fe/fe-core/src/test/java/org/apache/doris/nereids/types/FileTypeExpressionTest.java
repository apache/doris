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

import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionVolatility;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.rules.analysis.CheckAnalysis;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
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
import org.apache.doris.nereids.trees.expressions.functions.agg.MapAggV2;
import org.apache.doris.nereids.trees.expressions.functions.agg.MaxBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinBy;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeFile;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeFileOuter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayDistinct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FileDataSize;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdaf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdtf;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FileLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.proto.Types.PGenericType;
import org.apache.doris.proto.Types.PGenericType.TypeId;
import org.apache.doris.proto.Types.PValues;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNodeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

public class FileTypeExpressionTest {
    private DataType type(String sql) {
        return DataType.convertFromString(sql);
    }

    @Test
    public void testGenericFunctionsRejectFileArguments() {
        for (String expression : Arrays.asList(
                "lag(f) over(order by id)", "lead(f) over(order by id)",
                "first_value(f) over(order by id)", "last_value(f) over(order by id)",
                "nth_value(f, 1) over(order by id)",
                "array_sortby(files, array(id))", "array(f)", "coalesce(f, f)",
                "if(id > 0, f, f)", "named_struct('asset', f)", "map('asset', f)")) {
            Assertions.assertThrows(Exception.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression + " from (select 1 id, cast(null as file) f, "
                            + "cast(null as array<file>) files) t"), expression);
        }
    }

    @Test
    public void testHigherOrderFunctionsRejectFileInputsAndResults() {
        String from = " from (select 1 id, cast(null as file) f, cast(null as array<file>) files) t";
        for (String expression : Arrays.asList("array_map(x -> x, files)",
                "array_map(x -> element_at(x,'size'), files)",
                "array_filter(x -> element_at(x,'size') > 0, files)",
                "array_map(x -> f, array(id))", "array_map((x,y) -> x, array(id), files)")) {
            Assertions.assertThrows(Exception.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression + from), expression);
        }
        for (String expression : Arrays.asList("array_map(x -> x + 1, array(id))",
                "array_map(x -> element_at(f,'size') + x, array(id))")) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select " + expression + from), expression);
        }
    }

    @Test
    public void testAliasFileDeclarationsRejectNullArguments() {
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            AliasUdf alias = new AliasUdf("file_alias", Collections.singletonList(type(sqlType)),
                    new NullLiteral(), Collections.singletonList("f"), Collections.emptyMap(), new NullLiteral());
            Assertions.assertThrows(Exception.class, () -> TypeCoercionUtils.processBoundFunction(alias), sqlType);
        }
        AliasUdf ordinary = new AliasUdf("ordinary_alias", Collections.singletonList(BigIntType.INSTANCE),
                new NullLiteral(), Collections.singletonList("n"), Collections.emptyMap(), new NullLiteral());
        Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(ordinary));
    }

    @Test
    public void testInternalStatisticsSizeSignatureAndAdmission() {
        for (boolean nullable : Arrays.asList(false, true)) {
            FileDataSize function = new FileDataSize(new SlotReference("f", FileType.INSTANCE, nullable));
            Assertions.assertEquals(BigIntType.INSTANCE, function.getDataType());
            Assertions.assertEquals(nullable, function.nullable());
            Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.checkFileFunction(function));
            Assertions.assertEquals(FileType.INSTANCE, function.getSignature().getArgType(0));
        }
        Assertions.assertThrows(Exception.class, () -> TypeCoercionUtils.processBoundFunction(
                new FileDataSize(new StringLiteral("{}"))));
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select coalesce(sum(__file_data_size(f)), 0), count(*) - count(__file_data_size(f)) "
                        + "from (select cast(null as file) as f) t"));
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
            Assertions.assertThrows(Exception.class,
                    () -> TypeCoercionUtils.processBoundFunction(new ElementAt(file, selector)));
        }
    }

    @Test
    public void testStructuredCastsAndNoStringParsing() {
        String metadata = "uri:STRING,offset:BIGINT,size:BIGINT,content_type:STRING,checksum:STRING";
        String fields = metadata + ",inline:VARBINARY";
        for (boolean strict : Arrays.asList(true, false)) {
            for (String source : Arrays.asList("STRUCT<" + fields + ">", "JSON", "VARIANT",
                    "STRUCT<inline:VARBINARY,checksum:VARCHAR(10),size:BIGINT,uri:CHAR(10),"
                            + "content_type:STRING,offset:BIGINT>")) {
                DataType value = type(source);
                Assertions.assertTrue(CheckCast.check(value, FileType.INSTANCE, strict), source);
                Assertions.assertTrue(CheckCast.check(FileType.INSTANCE, value, strict), source);
                Assertions.assertFalse(TypeCoercionUtils.implicitCast(value, FileType.INSTANCE).isPresent());
                Assertions.assertFalse(TypeCoercionUtils.implicitCast(FileType.INSTANCE, value).isPresent());
            }
            for (String source : Arrays.asList("STRING", "INT", "STRUCT<uri:STRING>",
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
    public void testConditionalFunctionsRejectFile() {
        Expression file = new SlotReference("f", FileType.INSTANCE);
        Expression structured = new SlotReference("s", type("STRUCT<uri:STRING>"));
        Assertions.assertThrows(Exception.class,
                () -> TypeCoercionUtils.processBoundFunction(new If(BooleanLiteral.TRUE, file, structured)));
        Assertions.assertThrows(Exception.class,
                () -> TypeCoercionUtils.processBoundFunction(new Coalesce(structured, file)));
        Assertions.assertThrows(Exception.class,
                () -> TypeCoercionUtils.processBoundFunction(new Coalesce(new NullLiteral(), file)));
        Assertions.assertThrows(Exception.class,
                () -> TypeCoercionUtils.processBoundFunction(new If(BooleanLiteral.TRUE, file, file)));
    }

    @Test
    public void testWindowsRejectNestedFileValuesAndDefaults() {
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            Expression value = new SlotReference("f", type(sqlType));
            for (BoundFunction function : Arrays.asList(new Lag(value), new Lead(value),
                    new Lag(new NullLiteral(), new IntegerLiteral(1), value),
                    new Lead(new NullLiteral(), new IntegerLiteral(1), value))) {
                Assertions.assertThrows(Exception.class, () -> TypeCoercionUtils.processBoundFunction(function));
            }
        }
    }

    @Test
    public void testRecursiveComparisonGates() {
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            Expression value = new SlotReference("v", type(sql));
            Assertions.assertThrows(Exception.class,
                    () -> TypeCoercionUtils.processComparisonPredicate(new EqualTo(value, value)));
            Assertions.assertThrows(Exception.class,
                    () -> TypeCoercionUtils.processInPredicate(
                            new InPredicate(value, Collections.singletonList(value))));
            Assertions.assertThrows(Exception.class,
                    () -> Expression.checkFileKey(value, "ORDER BY"));
        }
    }

    @Test
    public void testElementAtExplodeAndReverseCastFieldRewrite() {
        Expression file = new SlotReference("f", FileType.INSTANCE);
        for (StructField field : FileType.INSTANCE.getFields()) {
            Expression accessed = ExpressionAnalyzer.analyzeFunction(null, null,
                    new ElementAt(file, new StringLiteral(field.getName().toUpperCase(Locale.ROOT))));
            Assertions.assertTrue(accessed instanceof ElementAt);
            Assertions.assertEquals(field.getName(), ((StringLiteral) accessed.child(1)).getStringValue());
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
        Expression accessed = TypeCoercionUtils.processBoundFunction(new ElementAt(
                new Cast(file, FileType.INSTANCE.publicStructType(), true), new StringLiteral("INLINE")));
        Assertions.assertTrue(accessed instanceof ElementAt);
        Assertions.assertSame(file, accessed.child(0));
        Assertions.assertEquals(VarBinaryType.INSTANCE, accessed.getDataType());
        Assertions.assertEquals("inline", ((StringLiteral) accessed.child(1)).getStringValue());
    }

    @Test
    public void testFunctionParameterAndAssignmentBoundaries() {
        DataType struct = type("STRUCT<uri:STRING>");
        for (DataType source : Arrays.asList(struct, JsonType.INSTANCE)) {
            Assertions.assertFalse(ImplicitlyCastableSignature.isImplicitlyCastable(FileType.INSTANCE, source));
            Assertions.assertFalse(ExplicitlyCastableSignature.isExplicitlyCastable(FileType.INSTANCE, source));
        }
        Assertions.assertFalse(ExplicitlyCastableSignature.isExplicitlyCastable(
                StringType.INSTANCE, FileType.INSTANCE));
        Assertions.assertFalse(ImplicitlyCastableSignature.isImplicitlyCastable(
                FileType.INSTANCE, StringType.INSTANCE));
        Assertions.assertFalse(ImplicitlyCastableSignature.isImplicitlyCastable(
                type("STRUCT<f:STRING>"), type("STRUCT<f:FILE>")));
        Assertions.assertFalse(ExplicitlyCastableSignature.isExplicitlyCastable(
                type("STRUCT<f:STRING>"), type("STRUCT<f:FILE>")));
        Assertions.assertFalse(ImplicitlyCastableSignature.isImplicitlyCastable(
                type("STRUCT<f:FILE>"), type("STRUCT<f:STRUCT<uri:STRING>>")));
        Assertions.assertThrows(Exception.class, () -> TypeCoercionUtils.castIfNotSameType(
                new SlotReference("f", FileType.INSTANCE), StringType.INSTANCE));
    }

    @Test
    public void testOnlyCountAcceptsFileInBuiltinAggregates() {
        Expression key = new IntegerLiteral(1);
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            Expression file = new SlotReference("f", type(sqlType));
            Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(new Count(file)), sqlType);
            for (BoundFunction rejected : Arrays.asList(new AnyValue(file), new ArrayAgg(file),
                    new CollectList(file), new MinBy(file, key), new MaxBy(file, key), new MapAggV2(key, file),
                    new Count(true, file), new ArrayAgg(true, file), new Min(file), new MinBy(key, file),
                    new MapAggV2(file, key),
                    new ArrayDistinct(new SlotReference("a", ArrayType.of(file.getDataType()))))) {
                Assertions.assertThrows(Exception.class,
                        () -> TypeCoercionUtils.processBoundFunction(rejected), rejected.getName() + " " + sqlType);
            }
        }
        for (BoundFunction ordinary : Arrays.asList(new AnyValue(key), new ArrayAgg(key), new CollectList(key),
                new MinBy(key, key), new MaxBy(key, key), new MapAggV2(key, key))) {
            Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(ordinary), ordinary.getName());
        }
    }

    @Test
    public void testFileAggregateSqlAdmissionAndStateCombinators() {
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            String input = " from (select cast(null as " + sqlType + ") f) t";
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select count(f), count(*)" + input));
            for (String aggregate : Arrays.asList("any_value(f)", "array_agg(f)", "collect_list(f)",
                    "group_array(f)", "min_by(f, 1)", "max_by(f, 1)", "map_agg(1, f)",
                    "map_agg_v1(1, f)", "map_agg_v2(1, f)",
                    "any_value_state(f)", "count_state(f)")) {
                Assertions.assertThrows(Exception.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                        .analyze("select " + aggregate + input), aggregate + " " + sqlType);
            }
        }
    }

    @Test
    public void testPythonFunctionsRejectFileAtBinding() {
        for (DataType fileType : Arrays.asList(FileType.INSTANCE, type("ARRAY<FILE>"),
                type("STRUCT<f:FILE>"), type("MAP<STRING,FILE>"))) {
            for (String kind : Arrays.asList("scalar", "aggregate", "table")) {
                Assertions.assertThrows(Exception.class, () -> TypeCoercionUtils.processBoundFunction(
                        pythonFunction(kind, fileType, IntegerType.INSTANCE, IntegerType.INSTANCE)));
                Assertions.assertThrows(Exception.class, () -> TypeCoercionUtils.processBoundFunction(
                        (BoundFunction) pythonFunction(kind, fileType, IntegerType.INSTANCE, IntegerType.INSTANCE)
                                .withChildren(Collections.singletonList(new NullLiteral()))));
                Assertions.assertThrows(Exception.class, () -> TypeCoercionUtils.processBoundFunction(
                        pythonFunction(kind, IntegerType.INSTANCE, fileType, IntegerType.INSTANCE)));
            }
            Assertions.assertThrows(Exception.class, () -> TypeCoercionUtils.processBoundFunction(
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
            return new PythonUdaf("py_agg", 1, "db1", Function.BinaryType.PYTHON_UDF,
                    signature, intermediateType, Function.NullableMode.ALWAYS_NULLABLE,
                    FunctionVolatility.IMMUTABLE, Udf.createVolatileIdentity(FunctionVolatility.IMMUTABLE),
                    null, "Agg", null, null, null, null, null, null, null, false, "", false, 360,
                    "3.10.2", "", argument);
        }
        if (kind.equals("table")) {
            return new PythonUdtf("py_table", 1, "db1", Function.BinaryType.PYTHON_UDF,
                    signature, Function.NullableMode.ALWAYS_NULLABLE, FunctionVolatility.IMMUTABLE,
                    Udf.createVolatileIdentity(FunctionVolatility.IMMUTABLE),
                    null, "evaluate", null, null, "", false, 360, "3.10.2", "", argument);
        }
        return new PythonUdf("py_scalar", 1, "db1", Function.BinaryType.PYTHON_UDF,
                signature, Function.NullableMode.ALWAYS_NULLABLE, FunctionVolatility.IMMUTABLE,
                Udf.createVolatileIdentity(FunctionVolatility.IMMUTABLE),
                null, "evaluate", null, null, "", false, 360, "3.10.2", "", argument);
    }

    @Test
    public void testRemovedFileGettersAreUnregistered() {
        for (String field : Arrays.asList("uri", "offset", "size", "content_type", "checksum", "inline")) {
            Assertions.assertThrows(Exception.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select fl_get_" + field + "(cast(null as file))"));
        }
    }

    @Test
    public void testIndependentLiteralAndSixChildWire() {
        FileLiteral literal = new FileLiteral(Arrays.asList(new StringLiteral("urn:x"),
                new NullLiteral(BigIntType.INSTANCE), new NullLiteral(BigIntType.INSTANCE),
                new NullLiteral(VarcharType.createVarcharType(1024)),
                new NullLiteral(VarcharType.createVarcharType(1024)), new NullLiteral(VarBinaryType.INSTANCE)));
        Assertions.assertEquals(Literal.class, literal.getClass().getSuperclass());
        TExpr wire = ExprToThriftVisitor.treeToThrift(literal.toLegacyLiteral());
        Assertions.assertEquals(TExprNodeType.FILE_LITERAL, wire.getNodes().get(0).getNodeType());
        Assertions.assertEquals(6, wire.getNodes().get(0).getNumChildren());
        Assertions.assertEquals(7, wire.getNodesSize());
        Assertions.assertTrue(literal.getStringValue().contains("\"inline\":null"));
        Assertions.assertTrue(literal.toSql().contains("'inline'"));
        Assertions.assertTrue(literal.toLegacyLiteral().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)
                .contains("'inline'"));
        FileLiteral binary = new FileLiteral(Arrays.asList(new StringLiteral("urn:x"),
                new NullLiteral(BigIntType.INSTANCE), new NullLiteral(BigIntType.INSTANCE),
                new NullLiteral(VarcharType.createVarcharType(1024)),
                new NullLiteral(VarcharType.createVarcharType(1024)),
                new VarBinaryLiteral(new byte[] {0, (byte) 0xff, (byte) 0x80})));
        Assertions.assertTrue(binary.getStringValue().contains("\"inline\":\"AP+A\""));
        Assertions.assertTrue(binary.toSql().contains("X'00FF80'"));
        Assertions.assertTrue(binary.toLegacyLiteral().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)
                .contains("X'00FF80'"));
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select " + binary.toSql()));
        FileLiteral empty = new FileLiteral(Arrays.asList(new StringLiteral("urn:empty"),
                new NullLiteral(BigIntType.INSTANCE), new NullLiteral(BigIntType.INSTANCE),
                new NullLiteral(VarcharType.createVarcharType(1024)),
                new NullLiteral(VarcharType.createVarcharType(1024)), new VarBinaryLiteral(new byte[0])));
        Assertions.assertTrue(empty.getStringValue().contains("\"inline\":\"\""));
        Assertions.assertTrue(empty.toSql().contains("X''"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> literal.toLegacyLiteral().compareLiteral(literal.toLegacyLiteral()));
        PValues.Builder values = PValues.newBuilder().setType(PGenericType.newBuilder().setId(TypeId.FILE));
        values.addChildElement(PValues.newBuilder().setType(PGenericType.newBuilder().setId(TypeId.STRING))
                .addStringValue("urn:x"));
        for (int i = 1; i < 6; i++) {
            PValues.Builder child = PValues.newBuilder().setHasNull(true).addNullMap(true)
                    .setType(PGenericType.newBuilder().setId(i <= 2 ? TypeId.INT64
                            : i < 5 ? TypeId.STRING : TypeId.VARBINARY));
            if (i <= 2) {
                child.addInt64Value(0);
            } else if (i < 5) {
                child.addStringValue("");
            } else {
                child.addBytesValue(com.google.protobuf.ByteString.EMPTY);
            }
            values.addChildElement(child);
        }
        Assertions.assertEquals(Collections.singletonList(literal),
                FoldConstantRuleOnBE.getResultExpression(FileType.INSTANCE, values.build()));
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
                        + "select cast(cast('{\"uri\":\"urn:y\"}' as json) as file) "
                        + "union all select cast(null as file)",
                "select cast(null as array<file>) union all "
                        + "select cast(array(named_struct('uri', 'urn:x', 'offset', cast(0 as bigint), 'size', "
                        + "cast(0 as bigint), 'content_type', '', 'checksum', '', 'inline', null)) as array<file>)",
                "select count(f), count(*) "
                        + "from (select cast(null as file) f) t",
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
                        + "union all select cast(cast('{}' as json) as file) union all select cast(null as file)",
                "select cast(null as file) union all select cast(named_struct('uri', 'urn:x', 'offset', cast(0 "
                        + "as bigint), 'size', cast(0 as bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null) as file) "
                        + "union all select cast(cast('{}' as json) as file) "
                        + "union all select cast(named_struct('uri', 'urn:z', 'offset', cast(0 as bigint), "
                        + "'size', cast(0 as bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null) as file)")) {
            Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).getPlan();
            Assertions.assertEquals(FileType.INSTANCE, plan.getOutput().get(0).getDataType());
        }
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select 1 union all select 2.5 union all select null union all select 3"));
        Assertions.assertThrows(Exception.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select cast(null as file) union all select named_struct('uri', 'urn:x', 'offset', "
                        + "cast(0 as bigint), 'size', cast(0 as bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null)"));
    }

    @Test
    public void testSqlOperatorRejections() {
        for (String sql : Arrays.asList(
                "select cast(null as file) = cast(null as file)",
                "select cast(null as struct<f:file>) in (cast(null as struct<f:file>))",
                "select count(distinct f) from (select cast(null as file) f) t",
                "select any_value(distinct f) from (select cast(null as file) f) t",
                "select f from (select cast(null as file) f) t order by f",
                "select f from (select cast(null as file) f) t group by f",
                "select distinct cast(null as file)",
                "select cast(null as file) union select cast(null as file)",
                "select cast(null as file) intersect select cast(null as file)",
                "select crc32_internal(cast(null as file))",
                "select min(cast(null as file))",
                "select element_at(cast(null as file), 'missing')",
                "select u from (select cast(null as file) f) t lateral view explode_file(f) e as u",
                "select u from (select cast(null as file) f) t lateral view explode_file(f) e as u, o, s, c, h",
                "select u from (select cast(null as file) f) t "
                        + "lateral view explode_file_outer(f) e as u, o, s, c, h")) {
            Assertions.assertThrows(Exception.class,
                    () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql), sql);
        }
    }

    @Test
    public void testQueryVersionGate() {
        LogicalOneRowRelation relation = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                Collections.singletonList(new Alias(new NullLiteral(FileType.INSTANCE), "f")));
        int version = Config.be_exec_version;
        try {
            Config.be_exec_version = Config.FILE_MIN_BE_EXEC_VERSION - 1;
            Assertions.assertThrows(Exception.class, () -> CheckAnalysis.checkFilePlan(relation));
            Expression getter = ExpressionAnalyzer.analyzeFunction(null, null,
                    new ElementAt(new SlotReference("f", FileType.INSTANCE), new StringLiteral("uri")));
            Assertions.assertThrows(Exception.class, getter::checkInputDataTypes);
        } finally {
            Config.be_exec_version = version;
        }
        Assertions.assertDoesNotThrow(() -> CheckAnalysis.checkFilePlan(relation));
    }

    @Test
    public void testScalarWindowFrameKeepsExistingAnalysis() {
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select sum(v) over(order by v rows between unbounded preceding and current row) "
                        + "from (select 1 v) t"));
        Assertions.assertThrows(Exception.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select last_value(f) over(order by id rows between unbounded preceding and current row) "
                        + "from (select 1 id, cast(null as file) f) t"));
    }
}
