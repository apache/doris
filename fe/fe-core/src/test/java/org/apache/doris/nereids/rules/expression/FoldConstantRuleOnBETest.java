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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.proto.Types.PGenericType;
import org.apache.doris.proto.Types.PGenericType.TypeId;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.proto.Types.PStructField;
import org.apache.doris.proto.Types.PTypeNode;
import org.apache.doris.proto.Types.PValues;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
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
    void testNonConstantDivisionDoesNotRecomputePersistedDecimalType() {
        context.cascadesContext.getConnectContext().getSessionVariable().enableDecimal256 = false;
        Expression divide = new Divide(
                new SlotReference("wide", DecimalV3Type.createDecimalV3TypeNotCheck256(52, 20)),
                new SlotReference("divisor", DecimalV3Type.createDecimalV3Type(38, 10)));
        Assertions.assertFalse(divide.isConstant());
        Assertions.assertTrue(collectConstants(divide).isEmpty());
    }

    @Test
    void testDecodeVarbinaryAndStructFromProtobuf() {
        byte[] bytes = {0, (byte) 0xff, (byte) 0x80};
        PValues binary = PValues.newBuilder().setType(PGenericType.newBuilder().setId(TypeId.VARBINARY))
                .addBytesValue(ByteString.copyFrom(bytes)).addBytesValue(ByteString.EMPTY)
                .addBytesValue(ByteString.EMPTY).setHasNull(true)
                .addNullMap(false).addNullMap(true).addNullMap(false).build();
        List<Literal> expected = ImmutableList.of(new VarBinaryLiteral(bytes),
                new NullLiteral(VarBinaryType.INSTANCE), new VarBinaryLiteral(new byte[0]));
        Assertions.assertEquals(expected,
                FoldConstantRuleOnBE.getResultExpression(VarBinaryType.INSTANCE, binary));
        StructType type = new StructType(ImmutableList.of(
                new StructField("payload", VarBinaryType.INSTANCE, true, "")));
        PValues structure = PValues.newBuilder().setType(PGenericType.newBuilder().setId(TypeId.STRUCT))
                .addChildElement(binary).build();
        List<Literal> decoded = FoldConstantRuleOnBE.getResultExpression(type, structure);
        Assertions.assertEquals(expected.size(), decoded.size());
        for (int row = 0; row < expected.size(); row++) {
            Assertions.assertEquals(new StructLiteral(ImmutableList.of(expected.get(row)), type), decoded.get(row));
        }
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

    @Test
    void testReconstructFileTypeFromBeMetadata() {
        PTypeNode.Builder root = PTypeNode.newBuilder().setType(TTypeNodeType.FILE.getValue());
        List<PTypeNode> nodes = new ArrayList<>();
        nodes.add(null);
        for (org.apache.doris.catalog.StructField field : Type.FILE.getFields()) {
            root.addStructFields(PStructField.newBuilder().setName(field.getName()).setContainsNull(true));
            nodes.add(PTypeNode.newBuilder().setType(TTypeNodeType.SCALAR.getValue())
                    .setScalarType(PScalarType.newBuilder().setType(field.getType().getPrimitiveType().toThrift()
                            .getValue()).setLen(field.getType().getLength())).build());
        }
        nodes.set(0, root.build());
        // Exercise the protobuf type reconstruction that precedes value decoding.
        Pair<DataType, Integer> decoded = Deencapsulation.invoke(FoldConstantRuleOnBE.class,
                "convertToNereidsType", nodes, 0);
        Assertions.assertSame(FileType.INSTANCE, decoded.key());
        Assertions.assertEquals(7, decoded.value());
        nodes.add(0, PTypeNode.newBuilder().setType(TTypeNodeType.SCALAR.getValue())
                .setScalarType(PScalarType.newBuilder().setType(
                        org.apache.doris.thrift.TPrimitiveType.ARRAY.getValue())).build());
        Pair<DataType, Integer> nested = Deencapsulation.invoke(FoldConstantRuleOnBE.class,
                "convertToNereidsType", nodes, 0);
        Assertions.assertEquals(org.apache.doris.nereids.types.ArrayType.of(FileType.INSTANCE), nested.key());
        Assertions.assertEquals(8, nested.value());
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
