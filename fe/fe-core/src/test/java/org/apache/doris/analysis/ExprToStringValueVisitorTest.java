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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.foundation.format.FormatOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class ExprToStringValueVisitorTest {

    private static final ExprToStringValueVisitor V = ExprToStringValueVisitor.INSTANCE;

    // ======================== BoolLiteral ========================

    @Test
    public void testBoolLiteralQueryTopLevel() {
        BoolLiteral t = new BoolLiteral(true);
        BoolLiteral f = new BoolLiteral(false);
        FormatOptions opts = FormatOptions.getDefault();
        // level=0: uses getStringValue() which returns "1"/"0"
        Assertions.assertEquals("1", V.visitBoolLiteral(t, StringValueContext.forQuery(opts)));
        Assertions.assertEquals("0", V.visitBoolLiteral(f, StringValueContext.forQuery(opts)));
    }

    @Test
    public void testBoolLiteralNestedDefaultFormat() {
        BoolLiteral t = new BoolLiteral(true);
        BoolLiteral f = new BoolLiteral(false);
        FormatOptions opts = FormatOptions.getDefault();
        // Default: isBoolValueNum=true, so nested returns "1"/"0"
        opts.level = 1;
        Assertions.assertEquals("1", V.visitBoolLiteral(t, StringValueContext.forQuery(opts)));
        Assertions.assertEquals("0", V.visitBoolLiteral(f, StringValueContext.forQuery(opts)));
    }

    @Test
    public void testBoolLiteralNestedHiveFormat() {
        BoolLiteral t = new BoolLiteral(true);
        BoolLiteral f = new BoolLiteral(false);
        FormatOptions opts = FormatOptions.getForHive();
        // Hive: isBoolValueNum=false, nested returns "true"/"false"
        opts.level = 1;
        Assertions.assertEquals("true", V.visitBoolLiteral(t, StringValueContext.forQuery(opts)));
        Assertions.assertEquals("false", V.visitBoolLiteral(f, StringValueContext.forQuery(opts)));
    }

    // ======================== NullLiteral ========================

    @Test
    public void testNullLiteralQueryTopLevel() {
        NullLiteral n = new NullLiteral();
        Assertions.assertNull(V.visitNullLiteral(n, StringValueContext.forQuery(FormatOptions.getDefault())));
    }

    @Test
    public void testNullLiteralStreamLoad() {
        NullLiteral n = new NullLiteral();
        Assertions.assertEquals(FeConstants.null_string,
                V.visitNullLiteral(n, StringValueContext.forStreamLoad(FormatOptions.getDefault())));
    }

    @Test
    public void testNullLiteralInComplexTypeDefault() {
        NullLiteral n = new NullLiteral();
        // Default nullFormat = "null"
        Assertions.assertEquals("null",
                V.visitNullLiteral(n, StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType()));
    }

    @Test
    public void testNullLiteralInComplexTypePresto() {
        NullLiteral n = new NullLiteral();
        // Presto nullFormat = "NULL"
        Assertions.assertEquals("NULL",
                V.visitNullLiteral(n, StringValueContext.forQuery(FormatOptions.getForPresto()).asComplexType()));
    }

    @Test
    public void testNullLiteralInComplexTypeStreamLoad() {
        NullLiteral n = new NullLiteral();
        // Inside a complex type during stream load, inComplexType takes precedence over forStreamLoad.
        // Old behavior: ArrayLiteral.getStringValueForStreamLoad -> getStringValueForQuery
        //   -> child.getStringValueInComplexTypeForQuery -> getNullFormat() = "null"
        Assertions.assertEquals("null",
                V.visitNullLiteral(n,
                        StringValueContext.forStreamLoad(FormatOptions.getDefault()).asComplexType()));
    }

    // ======================== DecimalLiteral ========================

    @Test
    public void testDecimalLiteralPlainString() throws Exception {
        DecimalLiteral d = DecimalLiteralUtils.create("123.45", false);
        Assertions.assertEquals("123.45", V.visitDecimalLiteral(d, StringValueContext.forQuery(FormatOptions.getDefault())));
    }

    @Test
    public void testDecimalLiteralNoScientificNotation() throws Exception {
        DecimalLiteral d = DecimalLiteralUtils.create("100000000000000000.123", false);
        // toPlainString avoids scientific notation
        Assertions.assertFalse(V.visitDecimalLiteral(d,
                StringValueContext.forQuery(FormatOptions.getDefault())).contains("E"));
    }

    // ======================== StringLiteral ========================

    @Test
    public void testStringLiteralQueryTopLevel() {
        StringLiteral s = new StringLiteral("hello");
        Assertions.assertEquals("hello",
                V.visitStringLiteral(s, StringValueContext.forQuery(FormatOptions.getDefault())));
    }

    @Test
    public void testStringLiteralInComplexType() {
        StringLiteral s = new StringLiteral("hello");
        // Default wrapper = "
        Assertions.assertEquals("\"hello\"",
                V.visitStringLiteral(s, StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType()));
    }

    @Test
    public void testStringLiteralInComplexTypePresto() {
        StringLiteral s = new StringLiteral("hello");
        // Presto wrapper = "" (empty)
        Assertions.assertEquals("hello",
                V.visitStringLiteral(s, StringValueContext.forQuery(FormatOptions.getForPresto()).asComplexType()));
    }

    // ======================== IPv4Literal ========================

    @Test
    public void testIPv4LiteralQuery() throws Exception {
        IPv4Literal ip = new IPv4Literal("192.168.1.1");
        Assertions.assertEquals("192.168.1.1",
                V.visitIPv4Literal(ip, StringValueContext.forQuery(FormatOptions.getDefault())));
    }

    @Test
    public void testIPv4LiteralInComplexType() throws Exception {
        IPv4Literal ip = new IPv4Literal("10.0.0.1");
        Assertions.assertEquals("\"10.0.0.1\"",
                V.visitIPv4Literal(ip, StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType()));
    }

    // ======================== IPv6Literal ========================

    @Test
    public void testIPv6LiteralQuery() throws Exception {
        IPv6Literal ip = new IPv6Literal("::1");
        Assertions.assertEquals("::1",
                V.visitIPv6Literal(ip, StringValueContext.forQuery(FormatOptions.getDefault())));
    }

    @Test
    public void testIPv6LiteralInComplexType() throws Exception {
        IPv6Literal ip = new IPv6Literal("fe80::1");
        Assertions.assertEquals("\"fe80::1\"",
                V.visitIPv6Literal(ip, StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType()));
    }

    // ======================== VarBinaryLiteral ========================

    @Test
    public void testVarBinaryLiteralQuery() throws Exception {
        VarBinaryLiteral vb = new VarBinaryLiteral("hello".getBytes());
        Assertions.assertEquals("hello",
                V.visitVarBinaryLiteral(vb, StringValueContext.forQuery(FormatOptions.getDefault())));
    }

    @Test
    public void testVarBinaryLiteralInComplexType() throws Exception {
        VarBinaryLiteral vb = new VarBinaryLiteral("test".getBytes());
        Assertions.assertEquals("\"test\"",
                V.visitVarBinaryLiteral(vb, StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType()));
    }

    // ======================== PlaceHolderExpr ========================

    @Test
    public void testPlaceHolderExprInComplexType() {
        PlaceHolderExpr ph = new PlaceHolderExpr();
        // PlaceHolderExpr.getStringValue() returns ""
        Assertions.assertEquals("\"\"",
                V.visitPlaceHolderExpr(ph, StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType()));
    }

    // ======================== JsonLiteral ========================

    @Test
    public void testJsonLiteralQuery() throws Exception {
        JsonLiteral j = new JsonLiteral("{\"a\":1}");
        Assertions.assertEquals("{\"a\":1}",
                V.visitJsonLiteral(j, StringValueContext.forQuery(FormatOptions.getDefault())));
    }

    @Test
    public void testJsonLiteralInComplexTypeReturnsNull() throws Exception {
        JsonLiteral j = new JsonLiteral("{\"a\":1}");
        Assertions.assertNull(
                V.visitJsonLiteral(j, StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType()));
    }

    // ======================== MaxLiteral ========================

    @Test
    public void testMaxLiteralQuery() {
        Assertions.assertNull(
                V.visitMaxLiteral(MaxLiteral.MAX_VALUE, StringValueContext.forQuery(FormatOptions.getDefault())));
    }

    @Test
    public void testMaxLiteralInComplexTypeReturnsNull() {
        Assertions.assertNull(
                V.visitMaxLiteral(MaxLiteral.MAX_VALUE,
                        StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType()));
    }

    // ======================== FloatLiteral ========================

    @Test
    public void testFloatLiteralDouble() {
        FloatLiteral f = new FloatLiteral(3.14, Type.DOUBLE);
        String result = V.visitFloatLiteral(f, StringValueContext.forQuery(FormatOptions.getDefault()));
        // FractionalFormat should produce a numeric string
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("3.14"));
    }

    @Test
    public void testFloatLiteralFloat() {
        FloatLiteral f = new FloatLiteral(2.5, Type.FLOAT);
        String result = V.visitFloatLiteral(f, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("2.5"));
    }

    @Test
    public void testFloatLiteralPositiveInfinity() {
        FloatLiteral f = new FloatLiteral(Double.POSITIVE_INFINITY, Type.DOUBLE);
        String result = V.visitFloatLiteral(f, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals("Infinity", result);
    }

    @Test
    public void testFloatLiteralNegativeInfinity() {
        FloatLiteral f = new FloatLiteral(Double.NEGATIVE_INFINITY, Type.DOUBLE);
        String result = V.visitFloatLiteral(f, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals("-Infinity", result);
    }

    @Test
    public void testFloatLiteralNaN() {
        FloatLiteral f = new FloatLiteral(Double.NaN, Type.DOUBLE);
        String result = V.visitFloatLiteral(f, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals("NaN", result);
    }

    @Test
    public void testFloatLiteralFloatTypePositiveInfinity() {
        // FLOAT infinity gets promoted to DOUBLE infinity
        FloatLiteral f = new FloatLiteral((double) Float.POSITIVE_INFINITY, Type.FLOAT);
        String result = V.visitFloatLiteral(f, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals("Infinity", result);
    }

    // ======================== DateLiteral ========================

    @Test
    public void testDateLiteralDateType() throws Exception {
        DateLiteral d = new DateLiteral(2024, 1, 15, Type.DATEV2);
        Assertions.assertEquals("2024-01-15",
                V.visitDateLiteral(d, StringValueContext.forQuery(FormatOptions.getDefault())));
    }

    @Test
    public void testDateLiteralDatetimeType() throws Exception {
        DateLiteral d = new DateLiteral(2024, 1, 15, 10, 30, 0,
                ScalarType.createDatetimeV2Type(0));
        String result = V.visitDateLiteral(d, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals("2024-01-15 10:30:00", result);
    }

    @Test
    public void testDateLiteralInComplexType() throws Exception {
        DateLiteral d = new DateLiteral(2024, 1, 15, Type.DATEV2);
        Assertions.assertEquals("\"2024-01-15\"",
                V.visitDateLiteral(d, StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType()));
    }

    // ======================== CastExpr ========================

    @Test
    public void testCastExprQueryTopLevel() {
        StringLiteral child = new StringLiteral("42");
        CastExpr cast = new CastExpr(Type.BIGINT, child, false);
        // Top-level query: returns getStringValue() of cast (empty string for CastExpr)
        String result = V.visitCastExpr(cast, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals(cast.getStringValue(), result);
    }

    @Test
    public void testCastExprInComplexType() {
        StringLiteral child = new StringLiteral("42");
        CastExpr cast = new CastExpr(Type.BIGINT, child, false);
        // In complex type: delegates to child
        String result = V.visitCastExpr(cast, StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType());
        Assertions.assertEquals("\"42\"", result);
    }

    @Test
    public void testCastExprStreamLoad() {
        StringLiteral child = new StringLiteral("42");
        CastExpr cast = new CastExpr(Type.BIGINT, child, false);
        // Stream load: delegates to child
        String result = V.visitCastExpr(cast, StringValueContext.forStreamLoad(FormatOptions.getDefault()));
        Assertions.assertEquals("42", result);
    }

    // ======================== ArrayLiteral ========================

    @Test
    public void testArrayLiteralDefault() {
        ArrayLiteral arr = new ArrayLiteral(new ArrayType(Type.INT),
                new IntLiteral(1), new IntLiteral(2), new IntLiteral(3));
        String result = V.visitArrayLiteral(arr, StringValueContext.forQuery(FormatOptions.getDefault()));
        // Default collectionDelim = ", "
        Assertions.assertEquals("[1, 2, 3]", result);
    }

    @Test
    public void testArrayLiteralHive() {
        ArrayLiteral arr = new ArrayLiteral(new ArrayType(Type.INT),
                new IntLiteral(10), new IntLiteral(20));
        String result = V.visitArrayLiteral(arr, StringValueContext.forQuery(FormatOptions.getForHive()));
        // Hive collectionDelim = ","
        Assertions.assertEquals("[10,20]", result);
    }

    @Test
    public void testArrayLiteralWithStrings() {
        ArrayLiteral arr = new ArrayLiteral(new ArrayType(Type.VARCHAR),
                new StringLiteral("abc"), new StringLiteral("def"));
        String result = V.visitArrayLiteral(arr, StringValueContext.forQuery(FormatOptions.getDefault()));
        // Strings get wrapped with nestedStringWrapper in complex context
        Assertions.assertEquals("[\"abc\", \"def\"]", result);
    }

    @Test
    public void testArrayLiteralWithNulls() {
        ArrayLiteral arr = new ArrayLiteral(new ArrayType(Type.INT),
                new IntLiteral(1), new NullLiteral());
        String result = V.visitArrayLiteral(arr, StringValueContext.forQuery(FormatOptions.getDefault()));
        // NullLiteral in complex type returns nullFormat = "null"
        Assertions.assertEquals("[1, null]", result);
    }

    @Test
    public void testArrayLiteralEmpty() {
        ArrayLiteral arr = new ArrayLiteral(new ArrayType(Type.INT));
        String result = V.visitArrayLiteral(arr, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals("[]", result);
    }

    // ======================== MapLiteral ========================

    @Test
    public void testMapLiteralDefault() {
        MapLiteral map = new MapLiteral(new MapType(Type.VARCHAR, Type.INT),
                Arrays.asList(new StringLiteral("a"), new StringLiteral("b")),
                Arrays.asList(new IntLiteral(1), new IntLiteral(2)));
        String result = V.visitMapLiteral(map, StringValueContext.forQuery(FormatOptions.getDefault()));
        // Default mapKeyDelim = ":", collectionDelim = ", ", strings wrapped
        Assertions.assertEquals("{\"a\":1, \"b\":2}", result);
    }

    @Test
    public void testMapLiteralPresto() {
        MapLiteral map = new MapLiteral(new MapType(Type.VARCHAR, Type.INT),
                Arrays.asList(new StringLiteral("x")),
                Arrays.asList(new IntLiteral(42)));
        String result = V.visitMapLiteral(map, StringValueContext.forQuery(FormatOptions.getForPresto()));
        // Presto: mapKeyDelim = "=", wrapper = "" (empty)
        Assertions.assertEquals("{x=42}", result);
    }

    // ======================== StructLiteral ========================

    @Test
    public void testStructLiteralQuery() throws Exception {
        StructType structType = new StructType(
                new StructField("name", Type.VARCHAR),
                new StructField("age", Type.INT));
        StructLiteral s = new StructLiteral(structType,
                new StringLiteral("Alice"), new IntLiteral(30));
        FormatOptions opts = FormatOptions.getDefault();
        String result = V.visitStructLiteral(s, StringValueContext.forQuery(opts));
        // Query mode: includes field names, mapKeyDelim=":"
        Assertions.assertEquals("{\"name\":\"Alice\", \"age\":30}", result);
    }

    @Test
    public void testStructLiteralStreamLoad() throws Exception {
        StructType structType = new StructType(
                new StructField("name", Type.VARCHAR),
                new StructField("age", Type.INT));
        StructLiteral s = new StructLiteral(structType,
                new StringLiteral("Bob"), new IntLiteral(25));
        FormatOptions opts = FormatOptions.getDefault();
        String result = V.visitStructLiteral(s, StringValueContext.forStreamLoad(opts));
        // Stream load mode: NO field names, children use query+complex context
        Assertions.assertEquals("{\"Bob\", 25}", result);
    }

    // ======================== Default visitor (unoverridden Expr) ========================

    @Test
    public void testDefaultVisitFallsBackToGetStringValue() {
        IntLiteral i = new IntLiteral(42);
        // IntLiteral has no specific visit method, falls through to default visit()
        String result = V.visit(i, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals("42", result);
    }

    // ======================== Context behavior ========================

    @Test
    public void testAsComplexTypeIdempotent() {
        StringValueContext ctx = StringValueContext.forQuery(FormatOptions.getDefault()).asComplexType();
        StringValueContext ctx2 = ctx.asComplexType();
        // asComplexType on already-complex context returns same instance
        Assertions.assertSame(ctx, ctx2);
    }

    @Test
    public void testContextModeFlags() {
        StringValueContext query = StringValueContext.forQuery(FormatOptions.getDefault());
        Assertions.assertFalse(query.isForStreamLoad());
        Assertions.assertFalse(query.isInComplexType());

        StringValueContext stream = StringValueContext.forStreamLoad(FormatOptions.getDefault());
        Assertions.assertTrue(stream.isForStreamLoad());
        Assertions.assertFalse(stream.isInComplexType());

        StringValueContext complex = query.asComplexType();
        Assertions.assertFalse(complex.isForStreamLoad());
        Assertions.assertTrue(complex.isInComplexType());
    }

    @Test
    public void testAsQueryComplexType() {
        StringValueContext streamCtx = StringValueContext.forStreamLoad(FormatOptions.getDefault());
        StringValueContext queryComplex = streamCtx.asQueryComplexType();
        // asQueryComplexType: forces query mode + complex type
        Assertions.assertFalse(queryComplex.isForStreamLoad());
        Assertions.assertTrue(queryComplex.isInComplexType());
    }

    // ======================== Nested collections ========================

    @Test
    public void testNestedArrayInArray() {
        ArrayLiteral inner = new ArrayLiteral(new ArrayType(Type.INT),
                new IntLiteral(1), new IntLiteral(2));
        ArrayLiteral outer = new ArrayLiteral(new ArrayType(new ArrayType(Type.INT)), inner);
        String result = V.visitArrayLiteral(outer, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals("[[1, 2]]", result);
    }

    @Test
    public void testArrayWithMapValues() {
        MapLiteral map = new MapLiteral(new MapType(Type.VARCHAR, Type.INT),
                Arrays.asList(new StringLiteral("k")),
                Arrays.asList(new IntLiteral(1)));
        ArrayLiteral arr = new ArrayLiteral(new ArrayType(new MapType(Type.VARCHAR, Type.INT)), map);
        String result = V.visitArrayLiteral(arr, StringValueContext.forQuery(FormatOptions.getDefault()));
        Assertions.assertEquals("[{\"k\":1}]", result);
    }

    // ======================== Level tracking ========================

    @Test
    public void testLevelResetAfterArray() {
        FormatOptions opts = FormatOptions.getDefault();
        Assertions.assertEquals(0, opts.level);
        ArrayLiteral arr = new ArrayLiteral(new ArrayType(Type.INT), new IntLiteral(1));
        V.visitArrayLiteral(arr, StringValueContext.forQuery(opts));
        // level should be restored to 0 after visit
        Assertions.assertEquals(0, opts.level);
    }

    @Test
    public void testLevelResetAfterMap() {
        FormatOptions opts = FormatOptions.getDefault();
        MapLiteral map = new MapLiteral(new MapType(Type.VARCHAR, Type.INT),
                Arrays.asList(new StringLiteral("k")),
                Arrays.asList(new IntLiteral(1)));
        V.visitMapLiteral(map, StringValueContext.forQuery(opts));
        Assertions.assertEquals(0, opts.level);
    }

    @Test
    public void testLevelResetAfterStruct() throws Exception {
        FormatOptions opts = FormatOptions.getDefault();
        StructType st = new StructType(new StructField("f", Type.INT));
        StructLiteral s = new StructLiteral(st, new IntLiteral(1));
        V.visitStructLiteral(s, StringValueContext.forQuery(opts));
        Assertions.assertEquals(0, opts.level);
    }

}
