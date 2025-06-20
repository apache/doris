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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class ExprSerDeTest {

    private static final SlotRef SLOT_1 = new SlotRef(new TableName("t1"), "c1");
    private static final SlotRef SLOT_2 = new SlotRef(new TableName("t1"), "c2");
    private static final SlotRef SLOT_3 = new SlotRef(new TableName("t1"), "c3");

    private void testSerDe(Expr original) {
        String json = GsonUtils.GSON.toJson(original);
        Expr deserialized = GsonUtils.GSON.fromJson(json, Expr.class);
        Assertions.assertEquals(original, deserialized);
    }

    @Test
    void testFunctionCallExpr() {
        FunctionCallExpr original = new FunctionCallExpr("func", Lists.newArrayList(SLOT_1));
        original.setIsAnalyticFnCall(true);
        original.setTableFnCall(true);
        testSerDe(original);
    }

    @Test
    void testLambdaFunctionCallExpr() {
        LambdaFunctionCallExpr original = new LambdaFunctionCallExpr("func", Lists.newArrayList(SLOT_1));
        testSerDe(original);
    }

    @Test
    void testCastExpr() {
        CastExpr original = new CastExpr(TypeDef.create(PrimitiveType.BIGINT), SLOT_1);
        original.setImplicit(false);
        testSerDe(original);
    }

    @Test
    void testTimestampArithmeticExpr() {
        TimestampArithmeticExpr original = new TimestampArithmeticExpr("func", SLOT_1, SLOT_2, "timeUnit");
        testSerDe(original);
    }

    @Test
    void testIsNullPredicate() {
        IsNullPredicate original = new IsNullPredicate(SLOT_1, false);
        testSerDe(original);
    }

    @Test
    void testBetweenPredicate() {
        BetweenPredicate original = new BetweenPredicate(SLOT_1, SLOT_2, SLOT_3, false);
        testSerDe(original);
    }

    @Test
    void testBinaryPredicate() {
        BinaryPredicate original = new BinaryPredicate(Operator.EQ, SLOT_1, SLOT_2);
        testSerDe(original);
    }

    @Test
    void testLikePredicate() {
        LikePredicate original = new LikePredicate(LikePredicate.Operator.LIKE, SLOT_1, SLOT_2);
        testSerDe(original);
    }

    @Test
    void testMatchPredicate() {
        MatchPredicate original = new MatchPredicate(MatchPredicate.Operator.MATCH_ALL, SLOT_1, SLOT_2);
        testSerDe(original);
    }

    @Test
    void testInPredicate() {
        InPredicate original = new InPredicate(SLOT_1, Lists.newArrayList(SLOT_2, SLOT_3), false);
        testSerDe(original);
    }

    @Test
    void testCompoundPredicate() {
        CompoundPredicate original = new CompoundPredicate(CompoundPredicate.Operator.NOT, SLOT_1, null);
        testSerDe(original);
    }

    @Test
    void testBoolLiteral() {
        BoolLiteral original = new BoolLiteral(true);
        testSerDe(original);
    }

    @Test
    void testMaxLiteral() {
        MaxLiteral original = MaxLiteral.MAX_VALUE;
        testSerDe(original);
    }

    @Test
    void testStringLiteral() {
        StringLiteral original = new StringLiteral("abc");
        testSerDe(original);
    }

    @Test
    void testIntLiteral() {
        IntLiteral original = new IntLiteral(1);
        testSerDe(original);
    }

    @Test
    void testLargeIntLiteral() {
        LargeIntLiteral original = new LargeIntLiteral(new BigInteger("1"));
        testSerDe(original);
    }

    @Test
    void testDecimalLiteral() {
        DecimalLiteral original = new DecimalLiteral(new BigDecimal("1.23"));
        testSerDe(original);
    }

    @Test
    void testFloatLiteral() {
        FloatLiteral original = new FloatLiteral(1.23);
        testSerDe(original);
    }

    @Test
    void testNullLiteral() {
        NullLiteral original = new NullLiteral();
        testSerDe(original);
    }

    @Test
    void testMapLiteral() {
        MapLiteral original = new MapLiteral(new MapType(ScalarType.INT, ScalarType.SMALLINT),
                Lists.newArrayList(new IntLiteral(1)), Lists.newArrayList(new IntLiteral(1)));
        testSerDe(original);
    }

    @Test
    void testDateLiteral() throws AnalysisException {
        DateLiteral original = new DateLiteral("2020-02-02", Type.DATEV2);
        testSerDe(original);
    }

    @Test
    void testIPv6Literal() throws AnalysisException {
        IPv6Literal original = new IPv6Literal("::");
        testSerDe(original);
    }

    @Test
    void testIPv4Literal() {
        IPv4Literal original = new IPv4Literal(0);
        testSerDe(original);
    }

    @Test
    void testJsonLiteral() throws AnalysisException {
        JsonLiteral original = new JsonLiteral("[1, 2, 3]");
        String json = GsonUtils.GSON.toJson(original);
        Expr deserialized = GsonUtils.GSON.fromJson(json, Expr.class);
        Assertions.assertInstanceOf(JsonLiteral.class, deserialized);
        JsonLiteral jsonLiteral = (JsonLiteral) deserialized;
        Assertions.assertEquals(original.getValue(), jsonLiteral.getValue());
    }

    @Test
    void testArrayLiteral() {
        ArrayLiteral original = new ArrayLiteral(new ArrayType(ScalarType.INT), new IntLiteral(1), new IntLiteral(2));
        testSerDe(original);
    }

    @Test
    void testStructLiteral() throws AnalysisException {
        StructLiteral original = new StructLiteral(new IntLiteral(1), new IntLiteral(2));
        testSerDe(original);
    }

    @Test
    void testCaseExpr() {
        CaseExpr original = new CaseExpr(SLOT_1, Lists.newArrayList(new CaseWhenClause(SLOT_2, SLOT_2)), SLOT_3);
        testSerDe(original);
    }

    @Test
    void testLambdaFunctionExpr() {
        LambdaFunctionExpr original = new LambdaFunctionExpr(SLOT_1, "func", Lists.newArrayList(SLOT_2));
        testSerDe(original);
    }

    @Test
    void testEncryptKeyRef() {
        EncryptKeyRef original = new EncryptKeyRef(new EncryptKeyName("db", "key"));
        testSerDe(original);
    }

    @Test
    void testArithmeticExpr() {
        ArithmeticExpr original = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, SLOT_1, SLOT_2);
        testSerDe(original);
    }

    @Test
    void testSlotRef() {
        testSerDe(SLOT_1);
    }

    @Test
    void testInformationFunction() {
        InformationFunction original = new InformationFunction("func");
        testSerDe(original);
    }
}
