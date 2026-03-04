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
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.info.TableNameInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests verifying toSql() output for all 39 concrete Expr subclasses.
 * These tests validate current behavior prior to visitor-pattern refactoring.
 */
public class ExprToSqlTest {

    // -----------------------------------------------------------------------
    // Literals
    // -----------------------------------------------------------------------

    @Test
    public void testBoolLiteralTrue() {
        BoolLiteral expr = new BoolLiteral(true);
        Assertions.assertEquals("TRUE", expr.toSql());
    }

    @Test
    public void testBoolLiteralFalse() {
        BoolLiteral expr = new BoolLiteral(false);
        Assertions.assertEquals("FALSE", expr.toSql());
    }

    @Test
    public void testStringLiteralSimple() {
        StringLiteral expr = new StringLiteral("hello");
        Assertions.assertEquals("'hello'", expr.toSql());
    }

    @Test
    public void testStringLiteralWithSingleQuote() {
        // Single quotes inside the value should be escaped as ''
        StringLiteral expr = new StringLiteral("it's");
        Assertions.assertEquals("'it''s'", expr.toSql());
    }

    @Test
    public void testIntLiteral() {
        IntLiteral expr = new IntLiteral(42L);
        Assertions.assertEquals("42", expr.toSql());
    }

    @Test
    public void testFloatLiteralDouble() {
        FloatLiteral expr = new FloatLiteral(3.14, Type.DOUBLE);
        Assertions.assertEquals("3.14", expr.toSql());
    }

    @Test
    public void testFloatLiteralDoubleWholeNumber() {
        // Trailing zeros are stripped: 1.0 → "1"
        FloatLiteral expr = new FloatLiteral(1.0, Type.DOUBLE);
        Assertions.assertEquals("1", expr.toSql());
    }

    @Test
    public void testFloatLiteralFloat() {
        // FLOAT type uses maxFractionDigits=7; 1.5f fits exactly
        FloatLiteral expr = new FloatLiteral(1.5, Type.FLOAT);
        Assertions.assertEquals("1.5", expr.toSql());
    }

    @Test
    public void testFloatLiteralFloatPrecisionArtifact() {
        // 3.14 cannot be represented exactly as a 32-bit float, exposing a precision artifact
        FloatLiteral expr = new FloatLiteral((double) 3.14f, Type.FLOAT);
        Assertions.assertTrue(expr.toSql().startsWith("3.1400"));
    }

    @Test
    public void testDecimalLiteral() {
        DecimalLiteral expr = new DecimalLiteral(new java.math.BigDecimal("1.5"));
        Assertions.assertEquals("1.5", expr.toSql());
    }

    @Test
    public void testLargeIntLiteral() {
        LargeIntLiteral expr = new LargeIntLiteral(java.math.BigInteger.valueOf(12345678901234L));
        Assertions.assertEquals("12345678901234", expr.toSql());
    }

    @Test
    public void testNullLiteral() {
        NullLiteral expr = new NullLiteral();
        Assertions.assertEquals("NULL", expr.toSql());
    }

    @Test
    public void testMaxLiteral() {
        MaxLiteral expr = MaxLiteral.MAX_VALUE;
        Assertions.assertEquals("MAXVALUE", expr.toSql());
    }

    @Test
    public void testJsonLiteral() throws AnalysisException {
        JsonLiteral expr = new JsonLiteral("{\"k\":1}");
        Assertions.assertEquals("'{\"k\":1}'", expr.toSql());
    }

    @Test
    public void testIPv4Literal() throws AnalysisException {
        IPv4Literal expr = new IPv4Literal("192.168.1.1");
        Assertions.assertEquals("\"192.168.1.1\"", expr.toSql());
    }

    @Test
    public void testIPv6Literal() throws AnalysisException {
        IPv6Literal expr = new IPv6Literal("::1");
        // IPv6Literal.toSqlImpl() returns '"' + value + '"'
        Assertions.assertEquals("\"::1\"", expr.toSql());
    }

    @Test
    public void testTimeV2LiteralPositive() {
        // 01:02:03.000000, scale 6, not negative
        TimeV2Literal expr = new TimeV2Literal(1, 2, 3, 0, 6, false);
        Assertions.assertEquals("\"01:02:03.000000\"", expr.toSql());
    }

    @Test
    public void testTimeV2LiteralNegative() {
        TimeV2Literal expr = new TimeV2Literal(10, 20, 30, 0, 0, true);
        Assertions.assertEquals("\"-10:20:30\"", expr.toSql());
    }

    @Test
    public void testVarBinaryLiteral() throws AnalysisException {
        byte[] bytes = new byte[]{0x68, 0x65, 0x6c, 0x6c, 0x6f};
        VarBinaryLiteral expr = new VarBinaryLiteral(bytes);
        Assertions.assertEquals("X'68656C6C6F'", expr.toSql());
    }

    @Test
    public void testDateLiteral() throws AnalysisException {
        DateLiteral expr = new DateLiteral("2024-01-15", Type.DATEV2);
        Assertions.assertEquals("'2024-01-15'", expr.toSql());
    }

    @Test
    public void testArrayLiteral() {
        ArrayType arrayType = new ArrayType(Type.INT);
        ArrayLiteral expr = new ArrayLiteral(arrayType,
                new IntLiteral(1L), new IntLiteral(2L), new IntLiteral(3L));
        Assertions.assertEquals("[1, 2, 3]", expr.toSql());
    }

    @Test
    public void testArrayLiteralEmpty() {
        ArrayLiteral expr = new ArrayLiteral();
        Assertions.assertEquals("[]", expr.toSql());
    }

    @Test
    public void testMapLiteral() {
        MapType mapType = new MapType(Type.VARCHAR, Type.INT);
        List<LiteralExpr> keys = Arrays.asList(new StringLiteral("a"), new StringLiteral("b"));
        List<LiteralExpr> values = Arrays.asList(new IntLiteral(1L), new IntLiteral(2L));
        MapLiteral expr = new MapLiteral(mapType, keys, values);
        Assertions.assertEquals("MAP{'a':1, 'b':2}", expr.toSql());
    }

    @Test
    public void testStructLiteral() throws AnalysisException {
        StructType structType = new StructType(
                new StructField("f1", Type.INT),
                new StructField("f2", Type.VARCHAR));
        StructLiteral expr = new StructLiteral(structType, new IntLiteral(10L), new StringLiteral("x"));
        Assertions.assertEquals("STRUCT(10, 'x')", expr.toSql());
    }

    @Test
    public void testPlaceHolderExprNull() {
        // null lExpr case → "?"
        PlaceHolderExpr expr = new PlaceHolderExpr();
        Assertions.assertEquals("?", expr.toSql());
    }

    @Test
    public void testPlaceHolderExprWithLiteral() throws AnalysisException {
        PlaceHolderExpr expr = PlaceHolderExpr.create("42", Type.INT);
        Assertions.assertEquals("_placeholder_(42)", expr.toSql());
    }

    // -----------------------------------------------------------------------
    // References / identifiers
    // -----------------------------------------------------------------------

    @Test
    public void testColumnRefExpr() {
        ColumnRefExpr expr = new ColumnRefExpr(false);
        expr.setName("my_col");
        Assertions.assertEquals("my_col", expr.toSql());
    }

    @Test
    public void testInformationFunction() {
        InformationFunction expr = new InformationFunction("DATABASE");
        Assertions.assertEquals("DATABASE()", expr.toSql());
    }

    @Test
    public void testEncryptKeyRefNoDb() {
        // parts = ["mykey"] → db=null, keyName="mykey"
        EncryptKeyName keyName = new EncryptKeyName(Collections.singletonList("mykey"));
        EncryptKeyRef expr = new EncryptKeyRef(keyName);
        Assertions.assertEquals("KEY mykey", expr.toSql());
    }

    @Test
    public void testEncryptKeyRefWithDb() {
        // parts = ["mydb", "mykey"]
        EncryptKeyName keyName = new EncryptKeyName(Arrays.asList("mydb", "mykey"));
        EncryptKeyRef expr = new EncryptKeyRef(keyName);
        Assertions.assertEquals("KEY mydb.mykey", expr.toSql());
    }

    @Test
    public void testSlotRefNoTable() {
        // No table, no desc → falls back to label
        SlotRef expr = new SlotRef(null, "col1");
        Assertions.assertEquals("`col1`", expr.toSql());
    }

    @Test
    public void testSlotRefWithTable() {
        SlotRef expr = new SlotRef(new TableNameInfo("mytbl"), "col1");
        // TableNameInfo("mytbl").toSql() = "`mytbl`"
        Assertions.assertEquals("`mytbl`.`col1`", expr.toSql());
    }

    // -----------------------------------------------------------------------
    // Predicates
    // -----------------------------------------------------------------------

    @Test
    public void testBinaryPredicateEq() {
        Expr e1 = new SlotRef(null, "a");
        Expr e2 = new IntLiteral(1L);
        BinaryPredicate expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, e1, e2);
        Assertions.assertEquals("(`a` = 1)", expr.toSql());
    }

    @Test
    public void testBinaryPredicateNe() {
        Expr e1 = new SlotRef(null, "x");
        Expr e2 = new IntLiteral(0L);
        BinaryPredicate expr = new BinaryPredicate(BinaryPredicate.Operator.NE, e1, e2);
        Assertions.assertEquals("(`x` != 0)", expr.toSql());
    }

    @Test
    public void testIsNullPredicate() {
        Expr e1 = new SlotRef(null, "col");
        IsNullPredicate expr = new IsNullPredicate(e1, false);
        Assertions.assertEquals("`col` IS NULL", expr.toSql());
    }

    @Test
    public void testIsNotNullPredicate() {
        Expr e1 = new SlotRef(null, "col");
        IsNullPredicate expr = new IsNullPredicate(e1, true);
        Assertions.assertEquals("`col` IS NOT NULL", expr.toSql());
    }

    @Test
    public void testCompoundPredicateAnd() {
        Expr e1 = new BoolLiteral(true);
        Expr e2 = new BoolLiteral(false);
        CompoundPredicate expr = new CompoundPredicate(CompoundPredicate.Operator.AND, e1, e2);
        Assertions.assertEquals("(TRUE AND FALSE)", expr.toSql());
    }

    @Test
    public void testCompoundPredicateOr() {
        Expr e1 = new BoolLiteral(true);
        Expr e2 = new BoolLiteral(false);
        CompoundPredicate expr = new CompoundPredicate(CompoundPredicate.Operator.OR, e1, e2);
        Assertions.assertEquals("(TRUE OR FALSE)", expr.toSql());
    }

    @Test
    public void testCompoundPredicateNotDefault() {
        // NOT with default SQL: "NOT child"
        Expr e1 = new BoolLiteral(true);
        CompoundPredicate expr = new CompoundPredicate(CompoundPredicate.Operator.NOT, e1, null);
        Assertions.assertEquals("NOT TRUE", expr.toSql());
    }

    @Test
    public void testCompoundPredicateNotExternal() {
        // NOT with external SQL: "(NOT child)"
        Expr e1 = new BoolLiteral(true);
        CompoundPredicate expr = new CompoundPredicate(CompoundPredicate.Operator.NOT, e1, null);
        String sql = expr.toSql(true, true, null, null);
        Assertions.assertEquals("(NOT TRUE)", sql);
    }

    @Test
    public void testInPredicate() {
        Expr e1 = new SlotRef(null, "col");
        List<Expr> inList = Arrays.asList(new IntLiteral(1L), new IntLiteral(2L));
        InPredicate expr = new InPredicate(e1, inList, false);
        Assertions.assertEquals("`col` IN (1, 2)", expr.toSql());
    }

    @Test
    public void testNotInPredicate() {
        Expr e1 = new SlotRef(null, "col");
        List<Expr> inList = Arrays.asList(new IntLiteral(1L), new IntLiteral(2L));
        InPredicate expr = new InPredicate(e1, inList, true);
        Assertions.assertEquals("`col` NOT IN (1, 2)", expr.toSql());
    }

    @Test
    public void testLikePredicate() {
        Expr e1 = new SlotRef(null, "name");
        Expr e2 = new StringLiteral("foo%");
        LikePredicate expr = new LikePredicate(LikePredicate.Operator.LIKE, e1, e2);
        Assertions.assertEquals("`name` LIKE 'foo%'", expr.toSql());
    }

    @Test
    public void testRegexpPredicate() {
        Expr e1 = new SlotRef(null, "name");
        Expr e2 = new StringLiteral("^foo");
        LikePredicate expr = new LikePredicate(LikePredicate.Operator.REGEXP, e1, e2);
        Assertions.assertEquals("`name` REGEXP '^foo'", expr.toSql());
    }

    @Test
    public void testMatchPredicate() {
        Expr e1 = new SlotRef(null, "content");
        Expr e2 = new StringLiteral("word");
        MatchPredicate expr = new MatchPredicate(
                MatchPredicate.Operator.MATCH_ANY, e1, e2, Type.BOOLEAN,
                NullableMode.ALWAYS_NULLABLE, null, false);
        Assertions.assertEquals("`content` MATCH_ANY 'word'", expr.toSql());
    }

    @Test
    public void testBetweenPredicate() throws Exception {
        // BetweenPredicate has no public constructor; use reflection.
        Expr col = new SlotRef(null, "age");
        Expr low = new IntLiteral(18L);
        Expr high = new IntLiteral(65L);

        java.lang.reflect.Constructor<BetweenPredicate> ctor =
                BetweenPredicate.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        BetweenPredicate expr = ctor.newInstance();

        java.lang.reflect.Field inbField = BetweenPredicate.class.getDeclaredField("isNotBetween");
        inbField.setAccessible(true);
        inbField.set(expr, false);

        expr.addChild(col);
        expr.addChild(low);
        expr.addChild(high);

        Assertions.assertEquals("`age` BETWEEN 18 AND 65", expr.toSql());
    }

    @Test
    public void testNotBetweenPredicate() throws Exception {
        Expr col = new SlotRef(null, "age");
        Expr low = new IntLiteral(18L);
        Expr high = new IntLiteral(65L);

        java.lang.reflect.Constructor<BetweenPredicate> ctor =
                BetweenPredicate.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        BetweenPredicate expr = ctor.newInstance();

        java.lang.reflect.Field inbField = BetweenPredicate.class.getDeclaredField("isNotBetween");
        inbField.setAccessible(true);
        inbField.set(expr, true);

        expr.addChild(col);
        expr.addChild(low);
        expr.addChild(high);

        Assertions.assertEquals("`age` NOT BETWEEN 18 AND 65", expr.toSql());
    }

    // -----------------------------------------------------------------------
    // Arithmetic / Cast
    // -----------------------------------------------------------------------

    @Test
    public void testArithmeticExprAdd() {
        Expr e1 = new IntLiteral(1L);
        Expr e2 = new IntLiteral(2L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, e1, e2, Type.BIGINT,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(1 + 2)", expr.toSql());
    }

    @Test
    public void testArithmeticExprBitNot() {
        // unary operator (BITNOT): "~ child"
        Expr e1 = new IntLiteral(5L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.BITNOT, e1, null, Type.BIGINT,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("~ 5", expr.toSql());
    }

    @Test
    public void testCastExprDefault() {
        CastExpr expr = new CastExpr(Type.BIGINT, new IntLiteral(1L), false);
        Assertions.assertEquals("CAST(1 AS bigint)", expr.toSql());
    }

    @Test
    public void testCastExprExternal() {
        // External SQL strips the cast and returns just the child
        CastExpr expr = new CastExpr(Type.BIGINT, new IntLiteral(1L), false);
        String sql = expr.toSql(false, true, null, null);
        Assertions.assertEquals("1", sql);
    }

    @Test
    public void testTryCastExprDefault() {
        TryCastExpr expr = new TryCastExpr(Type.INT, new IntLiteral(1L), false, false);
        Assertions.assertEquals("TRY_CAST(1 AS int)", expr.toSql());
    }

    @Test
    public void testTryCastExprExternal() {
        TryCastExpr expr = new TryCastExpr(Type.INT, new IntLiteral(1L), false, false);
        String sql = expr.toSql(false, true, null, null);
        Assertions.assertEquals("1", sql);
    }

    // -----------------------------------------------------------------------
    // CASE / Variable / Functions
    // -----------------------------------------------------------------------

    @Test
    public void testCaseExprSimple() {
        // CASE WHEN TRUE THEN 1 END
        Expr whenExpr = new BoolLiteral(true);
        Expr thenExpr = new IntLiteral(1L);
        CaseWhenClause clause = new CaseWhenClause(whenExpr, thenExpr);
        CaseExpr expr = new CaseExpr(Collections.singletonList(clause), null, false);
        Assertions.assertEquals("CASE WHEN TRUE THEN 1 END", expr.toSql());
    }

    @Test
    public void testCaseExprWithElse() {
        // CASE WHEN TRUE THEN 1 ELSE 0 END
        Expr whenExpr = new BoolLiteral(true);
        Expr thenExpr = new IntLiteral(1L);
        Expr elseExpr = new IntLiteral(0L);
        CaseWhenClause clause = new CaseWhenClause(whenExpr, thenExpr);
        CaseExpr expr = new CaseExpr(Collections.singletonList(clause), elseExpr, false);
        Assertions.assertEquals("CASE WHEN TRUE THEN 1 ELSE 0 END", expr.toSql());
    }

    @Test
    public void testVariableExprUser() {
        VariableExpr expr = new VariableExpr("myvar", SetType.USER);
        Assertions.assertEquals("@myvar", expr.toSql());
    }

    @Test
    public void testVariableExprSession() {
        VariableExpr expr = new VariableExpr("version", SetType.SESSION);
        Assertions.assertEquals("@@version", expr.toSql());
    }

    @Test
    public void testVariableExprGlobal() {
        VariableExpr expr = new VariableExpr("timeout", SetType.GLOBAL);
        Assertions.assertEquals("@@GLOBAL.timeout", expr.toSql());
    }

    @Test
    public void testFunctionCallExpr() {
        FunctionCallExpr expr = new FunctionCallExpr("upper",
                Arrays.asList(new StringLiteral("hello")), false);
        Assertions.assertEquals("upper('hello')", expr.toSql());
    }

    @Test
    public void testFunctionCallExprNoArgs() {
        FunctionCallExpr expr = new FunctionCallExpr("now",
                Collections.emptyList(), false);
        Assertions.assertEquals("now()", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprFuncStyle() {
        // date_add(col, INTERVAL 1 YEAR)
        Expr e1 = new SlotRef(null, "dt");
        Expr e2 = new IntLiteral(1L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "date_add", ArithmeticExpr.Operator.ADD,
                e1, e2, "YEAR", Type.DATETIMEV2, false);
        Assertions.assertEquals("date_add(`dt`, INTERVAL 1 YEAR)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprTimestampdiff() {
        // TIMESTAMPDIFF(YEAR, e2, e1)
        Expr e1 = new SlotRef(null, "end_dt");
        Expr e2 = new SlotRef(null, "start_dt");
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "TIMESTAMPDIFF", ArithmeticExpr.Operator.ADD,
                e1, e2, "YEAR", Type.DATETIMEV2, false);
        Assertions.assertEquals("TIMESTAMPDIFF(YEAR, `start_dt`, `end_dt`)", expr.toSql());
    }

    @Test
    public void testLambdaFunctionExprOneArg() {
        // x -> x + 1
        Expr slot = new SlotRef(null, "x");
        Expr body = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, slot, new IntLiteral(1L),
                Type.BIGINT, NullableMode.DEPEND_ON_ARGUMENT, false);
        LambdaFunctionExpr expr = new LambdaFunctionExpr(
                body, Collections.singletonList("x"),
                Collections.singletonList(slot), false);
        Assertions.assertEquals("x -> (`x` + 1)", expr.toSql());
    }

    @Test
    public void testLambdaFunctionExprTwoArgs() {
        // (x,y) -> x + y
        Expr slotX = new SlotRef(null, "x");
        Expr slotY = new SlotRef(null, "y");
        Expr body = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, slotX, slotY,
                Type.BIGINT, NullableMode.DEPEND_ON_ARGUMENT, false);
        LambdaFunctionExpr expr = new LambdaFunctionExpr(
                body, Arrays.asList("x", "y"),
                Arrays.asList(slotX, slotY), false);
        Assertions.assertEquals("(x,y) -> (`x` + `y`)", expr.toSql());
    }

    // -----------------------------------------------------------------------
    // ArithmeticExpr – additional operators
    // -----------------------------------------------------------------------

    @Test
    public void testArithmeticExprSubtract() {
        Expr e1 = new IntLiteral(10L);
        Expr e2 = new IntLiteral(3L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.SUBTRACT, e1, e2, Type.BIGINT,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(10 - 3)", expr.toSql());
    }

    @Test
    public void testArithmeticExprMultiply() {
        Expr e1 = new IntLiteral(4L);
        Expr e2 = new IntLiteral(5L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.MULTIPLY, e1, e2, Type.BIGINT,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(4 * 5)", expr.toSql());
    }

    @Test
    public void testArithmeticExprDivide() {
        Expr e1 = new IntLiteral(10L);
        Expr e2 = new IntLiteral(2L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.DIVIDE, e1, e2, Type.DOUBLE,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(10 / 2)", expr.toSql());
    }

    @Test
    public void testArithmeticExprMod() {
        Expr e1 = new IntLiteral(7L);
        Expr e2 = new IntLiteral(3L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.MOD, e1, e2, Type.BIGINT,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(7 % 3)", expr.toSql());
    }

    @Test
    public void testArithmeticExprIntDivide() {
        Expr e1 = new IntLiteral(7L);
        Expr e2 = new IntLiteral(3L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.INT_DIVIDE, e1, e2, Type.BIGINT,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(7 DIV 3)", expr.toSql());
    }

    @Test
    public void testArithmeticExprBitAnd() {
        Expr e1 = new IntLiteral(12L);
        Expr e2 = new IntLiteral(10L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.BITAND, e1, e2, Type.BIGINT,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(12 & 10)", expr.toSql());
    }

    @Test
    public void testArithmeticExprBitOr() {
        Expr e1 = new IntLiteral(12L);
        Expr e2 = new IntLiteral(10L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.BITOR, e1, e2, Type.BIGINT,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(12 | 10)", expr.toSql());
    }

    @Test
    public void testArithmeticExprBitXor() {
        Expr e1 = new IntLiteral(12L);
        Expr e2 = new IntLiteral(10L);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.BITXOR, e1, e2, Type.BIGINT,
                NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(12 ^ 10)", expr.toSql());
    }

    @Test
    public void testArithmeticExprNestedBinary() {
        // (1 + 2) * 3
        Expr add = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, new IntLiteral(1L), new IntLiteral(2L),
                Type.BIGINT, NullableMode.DEPEND_ON_ARGUMENT, false);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.MULTIPLY, add, new IntLiteral(3L),
                Type.BIGINT, NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("((1 + 2) * 3)", expr.toSql());
    }

    // -----------------------------------------------------------------------
    // CaseExpr – additional cases
    // -----------------------------------------------------------------------

    @Test
    public void testCaseExprMultipleWhenClauses() {
        // CASE WHEN col > 10 THEN 'big' WHEN col > 5 THEN 'mid' ELSE 'small' END
        Expr col = new SlotRef(null, "col");
        Expr when1 = new BinaryPredicate(BinaryPredicate.Operator.GT, col, new IntLiteral(10L));
        Expr when2 = new BinaryPredicate(BinaryPredicate.Operator.GT, col, new IntLiteral(5L));
        List<CaseWhenClause> clauses = Arrays.asList(
                new CaseWhenClause(when1, new StringLiteral("big")),
                new CaseWhenClause(when2, new StringLiteral("mid")));
        CaseExpr expr = new CaseExpr(clauses, new StringLiteral("small"), false);
        Assertions.assertEquals("CASE WHEN (`col` > 10) THEN 'big' WHEN (`col` > 5) THEN 'mid' ELSE 'small' END",
                expr.toSql());
    }

    @Test
    public void testCaseExprNoElse() {
        // CASE WHEN TRUE THEN 1 WHEN FALSE THEN 2 END  (no else)
        List<CaseWhenClause> clauses = Arrays.asList(
                new CaseWhenClause(new BoolLiteral(true), new IntLiteral(1L)),
                new CaseWhenClause(new BoolLiteral(false), new IntLiteral(2L)));
        CaseExpr expr = new CaseExpr(clauses, null, false);
        Assertions.assertEquals("CASE WHEN TRUE THEN 1 WHEN FALSE THEN 2 END", expr.toSql());
    }

    @Test
    public void testCaseExprNullElse() {
        // CASE WHEN col IS NULL THEN 'null_val' ELSE col END
        Expr col = new SlotRef(null, "val");
        Expr when1 = new IsNullPredicate(col, false);
        CaseExpr expr = new CaseExpr(
                Collections.singletonList(new CaseWhenClause(when1, new StringLiteral("null_val"))),
                col, false);
        Assertions.assertEquals("CASE WHEN `val` IS NULL THEN 'null_val' ELSE `val` END", expr.toSql());
    }

    @Test
    public void testCaseExprNestedInArithmetic() {
        // (CASE WHEN TRUE THEN 1 ELSE 0 END) + 5
        CaseExpr caseExpr = new CaseExpr(
                Collections.singletonList(new CaseWhenClause(new BoolLiteral(true), new IntLiteral(1L))),
                new IntLiteral(0L), false);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, caseExpr, new IntLiteral(5L),
                Type.BIGINT, NullableMode.DEPEND_ON_ARGUMENT, false);
        Assertions.assertEquals("(CASE WHEN TRUE THEN 1 ELSE 0 END + 5)", expr.toSql());
    }

    // -----------------------------------------------------------------------
    // FunctionCallExpr – additional cases
    // -----------------------------------------------------------------------

    @Test
    public void testFunctionCallExprMultipleArgs() {
        FunctionCallExpr expr = new FunctionCallExpr("concat",
                Arrays.asList(new StringLiteral("a"), new StringLiteral("b"), new StringLiteral("c")), false);
        Assertions.assertEquals("concat('a', 'b', 'c')", expr.toSql());
    }

    @Test
    public void testFunctionCallExprDistinct() throws Exception {
        // count(DISTINCT col) — set fnParams via reflection since no public (FunctionName, FunctionParams) ctor
        FunctionCallExpr expr = new FunctionCallExpr("count",
                Collections.singletonList(new SlotRef(null, "col")), false);
        FunctionParams distinctParams = new FunctionParams(true,
                Collections.singletonList(new SlotRef(null, "col")));
        java.lang.reflect.Field fnParamsField = FunctionCallExpr.class.getDeclaredField("fnParams");
        fnParamsField.setAccessible(true);
        fnParamsField.set(expr, distinctParams);
        Assertions.assertEquals("count(DISTINCT `col`)", expr.toSql());
    }

    @Test
    public void testFunctionCallExprStar() throws Exception {
        // count(*) — set fnParams via reflection
        FunctionCallExpr expr = new FunctionCallExpr("count", Collections.emptyList(), false);
        FunctionParams starParams = FunctionParams.createStarParam();
        java.lang.reflect.Field fnParamsField = FunctionCallExpr.class.getDeclaredField("fnParams");
        fnParamsField.setAccessible(true);
        fnParamsField.set(expr, starParams);
        Assertions.assertEquals("count(*)", expr.toSql());
    }

    @Test
    public void testFunctionCallExprNestedFunctions() {
        // length(upper('hello'))
        FunctionCallExpr inner = new FunctionCallExpr("upper",
                Collections.singletonList(new StringLiteral("hello")), false);
        FunctionCallExpr expr = new FunctionCallExpr("length",
                Collections.singletonList(inner), false);
        Assertions.assertEquals("length(upper('hello'))", expr.toSql());
    }

    @Test
    public void testFunctionCallExprWithSlotRef() {
        // coalesce(col, 0)
        FunctionCallExpr expr = new FunctionCallExpr("coalesce",
                Arrays.asList(new SlotRef(null, "col"), new IntLiteral(0L)), false);
        Assertions.assertEquals("coalesce(`col`, 0)", expr.toSql());
    }

    @Test
    public void testFunctionCallExprIfNull() {
        // ifnull(a, b)
        FunctionCallExpr expr = new FunctionCallExpr("ifnull",
                Arrays.asList(new SlotRef(null, "a"), new StringLiteral("default")), false);
        Assertions.assertEquals("ifnull(`a`, 'default')", expr.toSql());
    }

    // -----------------------------------------------------------------------
    // TimestampArithmeticExpr – additional cases
    // -----------------------------------------------------------------------

    @Test
    public void testTimestampArithmeticExprDateSub() {
        // date_sub(col, INTERVAL 7 DAY)
        Expr e1 = new SlotRef(null, "dt");
        Expr e2 = new IntLiteral(7L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "date_sub", ArithmeticExpr.Operator.SUBTRACT,
                e1, e2, "DAY", Type.DATETIMEV2, false);
        Assertions.assertEquals("date_sub(`dt`, INTERVAL 7 DAY)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprDaysAdd() {
        // days_add(col, INTERVAL 30 DAY)
        Expr e1 = new SlotRef(null, "created_at");
        Expr e2 = new IntLiteral(30L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "days_add", ArithmeticExpr.Operator.ADD,
                e1, e2, "DAY", Type.DATETIMEV2, false);
        Assertions.assertEquals("days_add(`created_at`, INTERVAL 30 DAY)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprMonthsAdd() {
        // months_add(col, INTERVAL 3 MONTH)
        Expr e1 = new SlotRef(null, "dt");
        Expr e2 = new IntLiteral(3L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "months_add", ArithmeticExpr.Operator.ADD,
                e1, e2, "MONTH", Type.DATETIMEV2, false);
        Assertions.assertEquals("months_add(`dt`, INTERVAL 3 MONTH)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprHoursAdd() {
        // hours_add(col, INTERVAL 2 HOUR)
        Expr e1 = new SlotRef(null, "ts");
        Expr e2 = new IntLiteral(2L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "hours_add", ArithmeticExpr.Operator.ADD,
                e1, e2, "HOUR", Type.DATETIMEV2, false);
        Assertions.assertEquals("hours_add(`ts`, INTERVAL 2 HOUR)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprMinutesAdd() {
        // minutes_add(col, INTERVAL 15 MINUTE)
        Expr e1 = new SlotRef(null, "ts");
        Expr e2 = new IntLiteral(15L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "minutes_add", ArithmeticExpr.Operator.ADD,
                e1, e2, "MINUTE", Type.DATETIMEV2, false);
        Assertions.assertEquals("minutes_add(`ts`, INTERVAL 15 MINUTE)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprSecondsAdd() {
        // seconds_add(col, INTERVAL 30 SECOND)
        Expr e1 = new SlotRef(null, "ts");
        Expr e2 = new IntLiteral(30L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "seconds_add", ArithmeticExpr.Operator.ADD,
                e1, e2, "SECOND", Type.DATETIMEV2, false);
        Assertions.assertEquals("seconds_add(`ts`, INTERVAL 30 SECOND)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprTimestampadd() {
        // TIMESTAMPADD(MONTH, 1, col)
        Expr e1 = new SlotRef(null, "dt");
        Expr e2 = new IntLiteral(1L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "TIMESTAMPADD", ArithmeticExpr.Operator.ADD,
                e1, e2, "MONTH", Type.DATETIMEV2, false);
        // TIMESTAMPADD(unit, interval, timestamp) — same layout as TIMESTAMPDIFF
        Assertions.assertEquals("TIMESTAMPADD(MONTH, 1, `dt`)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprTimestampdiffDay() {
        // TIMESTAMPDIFF(DAY, start, end)
        Expr e1 = new SlotRef(null, "end_date");
        Expr e2 = new SlotRef(null, "start_date");
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "TIMESTAMPDIFF", ArithmeticExpr.Operator.ADD,
                e1, e2, "DAY", Type.DATETIMEV2, false);
        Assertions.assertEquals("TIMESTAMPDIFF(DAY, `start_date`, `end_date`)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprWeekAdd() {
        // weeks_add(col, INTERVAL 2 WEEK)
        Expr e1 = new SlotRef(null, "dt");
        Expr e2 = new IntLiteral(2L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "weeks_add", ArithmeticExpr.Operator.ADD,
                e1, e2, "WEEK", Type.DATETIMEV2, false);
        Assertions.assertEquals("weeks_add(`dt`, INTERVAL 2 WEEK)", expr.toSql());
    }

    @Test
    public void testTimestampArithmeticExprWithLiteral() {
        // date_add('2024-01-01', INTERVAL 1 YEAR)
        Expr e1;
        try {
            e1 = new DateLiteral("2024-01-01", Type.DATEV2);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
        Expr e2 = new IntLiteral(1L);
        TimestampArithmeticExpr expr = new TimestampArithmeticExpr(
                "date_add", ArithmeticExpr.Operator.ADD,
                e1, e2, "YEAR", Type.DATETIMEV2, false);
        Assertions.assertEquals("date_add('2024-01-01', INTERVAL 1 YEAR)", expr.toSql());
    }

    // -----------------------------------------------------------------------
    // TimeV2Literal – different scales
    // -----------------------------------------------------------------------

    @Test
    public void testTimeV2LiteralScale0() {
        // scale=0 → no fractional part: "01:02:03"
        TimeV2Literal expr = new TimeV2Literal(1, 2, 3, 0, 0, false);
        Assertions.assertEquals("\"01:02:03\"", expr.toSql());
    }

    @Test
    public void testTimeV2LiteralScale1() {
        // scale=1 → 1 digit: "01:02:03.1"
        // microsecond=100000 → scaled: 100000 / 10^(6-1) = 100000/100000 = 1
        TimeV2Literal expr = new TimeV2Literal(1, 2, 3, 100000, 1, false);
        Assertions.assertEquals("\"01:02:03.1\"", expr.toSql());
    }

    @Test
    public void testTimeV2LiteralScale3() {
        // scale=3 → 3 digits: "01:02:03.123"
        // microsecond=123000 → scaled: 123000 / 10^(6-3) = 123000/1000 = 123
        TimeV2Literal expr = new TimeV2Literal(1, 2, 3, 123000, 3, false);
        Assertions.assertEquals("\"01:02:03.123\"", expr.toSql());
    }

    @Test
    public void testTimeV2LiteralScale6() {
        // scale=6 → 6 digits: "01:02:03.123456"
        TimeV2Literal expr = new TimeV2Literal(1, 2, 3, 123456, 6, false);
        Assertions.assertEquals("\"01:02:03.123456\"", expr.toSql());
    }

    @Test
    public void testTimeV2LiteralNegativeWithScale() {
        // negative, scale=3: "-10:20:30.500"
        TimeV2Literal expr = new TimeV2Literal(10, 20, 30, 500000, 3, true);
        Assertions.assertEquals("\"-10:20:30.500\"", expr.toSql());
    }

    @Test
    public void testTimeV2LiteralLargeHour() {
        // hour > 99, scale=0: "838:59:59"
        TimeV2Literal expr = new TimeV2Literal(838, 59, 59, 0, 0, false);
        Assertions.assertEquals("\"838:59:59\"", expr.toSql());
    }

    @Test
    public void testTimeV2LiteralZero() {
        // 00:00:00, scale=0
        TimeV2Literal expr = new TimeV2Literal(0, 0, 0, 0, 0, false);
        Assertions.assertEquals("\"00:00:00\"", expr.toSql());
    }

    // -----------------------------------------------------------------------
    // DateLiteral – different types and scales
    // -----------------------------------------------------------------------

    @Test
    public void testDateLiteralDateV2() throws AnalysisException {
        // DATE type: 'YYYY-MM-DD'
        DateLiteral expr = new DateLiteral("2024-03-15", Type.DATEV2);
        Assertions.assertEquals("'2024-03-15'", expr.toSql());
    }

    @Test
    public void testDateLiteralDatetime() throws AnalysisException {
        // DATETIME (v1) type: 'YYYY-MM-DD HH:MM:SS'
        DateLiteral expr = new DateLiteral("2024-03-15 10:30:00", Type.DATETIME);
        Assertions.assertEquals("'2024-03-15 10:30:00'", expr.toSql());
    }

    @Test
    public void testDateLiteralDatetimeV2Scale0() throws AnalysisException {
        // DATETIMEV2 scale=0: 'YYYY-MM-DD HH:MM:SS'
        DateLiteral expr = new DateLiteral("2024-03-15 10:30:00", ScalarType.createDatetimeV2Type(0));
        Assertions.assertEquals("'2024-03-15 10:30:00'", expr.toSql());
    }

    @Test
    public void testDateLiteralDatetimeV2Scale3() throws AnalysisException {
        // DATETIMEV2 scale=3: 'YYYY-MM-DD HH:MM:SS.mmm'
        DateLiteral expr = new DateLiteral("2024-03-15 10:30:00.123", ScalarType.createDatetimeV2Type(3));
        Assertions.assertEquals("'2024-03-15 10:30:00.123'", expr.toSql());
    }

    @Test
    public void testDateLiteralDatetimeV2Scale6() throws AnalysisException {
        // DATETIMEV2 scale=6: 'YYYY-MM-DD HH:MM:SS.mmmmmm'
        DateLiteral expr = new DateLiteral("2024-03-15 10:30:00.123456", ScalarType.createDatetimeV2Type(6));
        Assertions.assertEquals("'2024-03-15 10:30:00.123456'", expr.toSql());
    }

    @Test
    public void testDateLiteralDateV2FromLongConstructor() {
        // Long-based constructor: year=2023, month=12, day=31
        DateLiteral expr = new DateLiteral(2023L, 12L, 31L, Type.DATEV2);
        Assertions.assertEquals("'2023-12-31'", expr.toSql());
    }

    @Test
    public void testDateLiteralDatetimeFromLongConstructor() {
        // Long-based constructor with time: 2023-06-01 08:00:00
        DateLiteral expr = new DateLiteral(2023L, 6L, 1L, 8L, 0L, 0L, Type.DATETIME);
        Assertions.assertEquals("'2023-06-01 08:00:00'", expr.toSql());
    }

    @Test
    public void testDateLiteralDatetimeV2Scale1() throws AnalysisException {
        // DATETIMEV2 scale=1: 'YYYY-MM-DD HH:MM:SS.m'
        DateLiteral expr = new DateLiteral("2024-01-01 00:00:00.5", ScalarType.createDatetimeV2Type(1));
        Assertions.assertEquals("'2024-01-01 00:00:00.5'", expr.toSql());
    }
}
