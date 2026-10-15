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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.analysis.ExprId;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParse;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.UuidLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.thrift.TExpr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FoldConstantRuleOnBETest extends ExpressionRewriteTestHelper {
    private static final UuidLiteral UUID = new UuidLiteral("00112233-4455-6677-8899-aabbccddeeff");

    @Test
    void testJsonToFileUsesGenericJsonFoldingRestriction() {
        Expression expression = new Cast(new JsonbParse(new StringLiteral(
                "{\"uri\":\"s3://bucket/object\",\"offset\":null,\"size\":null,"
                        + "\"content_type\":null,\"checksum\":null,\"inline\":null}")), FileType.INSTANCE);
        Assertions.assertTrue(expression.isConstant());
        Assertions.assertTrue(collectConstants(expression).isEmpty());
    }

    @Test
    void testFileCastsRemainForRuntimeEvaluation() {
        StructLiteral fileFields = new StructLiteral(ImmutableList.of(new StringLiteral("urn:x"),
                new NullLiteral(BigIntType.INSTANCE), new NullLiteral(BigIntType.INSTANCE),
                new NullLiteral(StringType.INSTANCE), new NullLiteral(StringType.INSTANCE),
                new NullLiteral(VarBinaryType.INSTANCE)), FileType.INSTANCE.publicStructType());
        List<Expression> expressions = ImmutableList.of(
                new Cast(fileFields, FileType.INSTANCE),
                new Cast(new ArrayLiteral(ImmutableList.of(fileFields)), ArrayType.of(FileType.INSTANCE)),
                new Cast(new MapLiteral(ImmutableMap.of(new StringLiteral("f"), fileFields)),
                        MapType.of(StringType.INSTANCE, FileType.INSTANCE)),
                new Cast(new StructLiteral(ImmutableList.of(fileFields)),
                        new StructType(ImmutableList.of(new StructField("f", FileType.INSTANCE, true, "")))));
        for (Expression expression : expressions) {
            Assertions.assertTrue(expression.isConstant());
            Assertions.assertTrue(CheckCast.check(expression.child(0).getDataType(), expression.getDataType(), false));
            Assertions.assertThrows(AnalysisException.class,
                    () -> expression.child(0).checkedCastTo(expression.getDataType()));
            Assertions.assertSame(expression, FoldConstantRuleOnFE.VISITOR_INSTANCE.rewrite(expression, context));
            Assertions.assertTrue(collectConstants(expression).isEmpty(), expression.toSql());
            Assertions.assertTrue(collectConstants(new IsNull(expression)).isEmpty(), expression.toSql());
        }
        Expression supported = new EqualTo(new Add(new IntegerLiteral(1), new IntegerLiteral(2)),
                new IntegerLiteral(3));
        Assertions.assertEquals(ImmutableList.of(supported),
                collectConstants(new And(new IsNull(expressions.get(0)), supported)));
    }

    @Test
    void testFileNullCastsUseNativeLiteralFolding() {
        for (DataType type : ImmutableList.of(FileType.INSTANCE, ArrayType.of(FileType.INSTANCE),
                MapType.of(StringType.INSTANCE, FileType.INSTANCE),
                new StructType(ImmutableList.of(new StructField("f", FileType.INSTANCE, true, ""))))) {
            Assertions.assertEquals(new NullLiteral(type), FoldConstantRuleOnFE.VISITOR_INSTANCE.rewrite(
                    new Cast(new NullLiteral(), type), context));
        }
        ArrayType fileArray = ArrayType.of(FileType.INSTANCE);
        Assertions.assertEquals(new ArrayLiteral(ImmutableList.of(new NullLiteral(FileType.INSTANCE)), fileArray),
                FoldConstantRuleOnFE.VISITOR_INSTANCE.rewrite(new Cast(
                        new ArrayLiteral(ImmutableList.of(new NullLiteral())), fileArray), context));
    }

    @Test
    void testNonConstantDivisionDoesNotRecomputePersistedDecimalType() {
        context.cascadesContext.getConnectContext().getSessionVariable().enableDecimal256 = false;
        Expression divide = new Divide(
                new SlotReference("wide", DecimalV3Type.createDecimalV3TypeNotCheck256(52, 20)),
                new SlotReference("divisor", DecimalV3Type.createDecimalV3Type(38, 10)));
        Assertions.assertFalse(divide.isConstant());
        Assertions.assertTrue(collectConstants(divide).isEmpty());
    }

    @Test
    void testSkipUuidElementAt() {
        ArrayLiteral values = new ArrayLiteral(ImmutableList.of(UUID, new NullLiteral(UuidType.INSTANCE)));
        for (int index : new int[] {1, 2, 3}) {
            Expression expression = new ElementAt(values, new IntegerLiteral(index));
            Assertions.assertTrue(expression.getDataType().isUuidType());
            // Non-null, null element and out-of-bounds results all require runtime evaluation.
            Expression folded = FoldConstantRuleOnFE.VISITOR_INSTANCE.rewrite(expression, context);
            Assertions.assertFalse(folded.isLiteral());
            Assertions.assertTrue(collectConstants(folded).isEmpty());
        }
    }

    @Test
    void testSkipNestedUuidLiterals() {
        List<Literal> values = ImmutableList.of(
                new ArrayLiteral(ImmutableList.of(UUID)),
                new MapLiteral(ImmutableMap.of(UUID, new IntegerLiteral(1))),
                new MapLiteral(ImmutableMap.of(new IntegerLiteral(1), UUID)),
                new StructLiteral(ImmutableList.of(UUID)),
                new StructLiteral(ImmutableList.of(new ArrayLiteral(ImmutableList.of(UUID)))));
        for (Literal value : values) {
            // Complex literal contents are not expression-tree children.
            Expression expression = new ElementAt(new ArrayLiteral(ImmutableList.of(value)), new IntegerLiteral(1));
            Assertions.assertTrue(expression.isConstant());
            Assertions.assertTrue(collectConstants(expression).isEmpty(), expression.toSql());
        }
    }

    @Test
    void testKeepSupportedSibling() {
        Expression uuidResult = new ElementAt(new ArrayLiteral(ImmutableList.of(UUID)), new IntegerLiteral(2));
        Expression supported = new EqualTo(new Add(new IntegerLiteral(1), new IntegerLiteral(2)),
                new IntegerLiteral(3));
        Assertions.assertEquals(ImmutableList.of(supported),
                collectConstants(new And(new IsNull(uuidResult), supported)));
    }

    @Test
    void testSkipCastFromVarBinary() {
        // FE leaves these casts for BE because no FE string literal carries 0xFF. BE folding would build such a
        // literal from its result anyway, decoding 0xFF as U+FFFD, so they wait for BE to evaluate them.
        VarBinaryLiteral notUtf8 = new VarBinaryLiteral(new byte[] {(byte) 0xFF});
        List<Expression> casts = ImmutableList.of(new Cast(notUtf8, StringType.INSTANCE),
                new Cast(new ArrayLiteral(ImmutableList.of(notUtf8)), ArrayType.of(StringType.INSTANCE)));
        for (Expression cast : casts) {
            Assertions.assertEquals(cast, FoldConstantRuleOnFE.VISITOR_INSTANCE.rewrite(cast, context));
            Assertions.assertTrue(collectConstants(cast).isEmpty(), cast.toSql());
        }
        Expression filter = new EqualTo(casts.get(0), new StringLiteral(String.valueOf((char) 0xFFFD)));
        Expression supported = new EqualTo(new Add(new IntegerLiteral(1), new IntegerLiteral(2)),
                new IntegerLiteral(3));
        Assertions.assertEquals(ImmutableList.of(supported), collectConstants(new And(filter, supported)));
    }

    private List<Expression> collectConstants(Expression expression) {
        Map<String, Expression> constants = new HashMap<>();
        Map<String, TExpr> serialized = new HashMap<>();
        Deencapsulation.invoke(FoldConstantRuleOnBE.class, "collectConst", expression,
                constants, serialized, ExprId.createGenerator());
        Assertions.assertEquals(constants.size(), serialized.size());
        return new ArrayList<>(constants.values());
    }
}
