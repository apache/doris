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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParse;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
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
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FoldConstantRuleOnBETest extends ExpressionRewriteTestHelper {

    @Test
    void testJsonToFileUsesGenericJsonFoldingRestriction() {
        Expression expression = new Cast(new JsonbParse(new StringLiteral(
                "{\"uri\":\"s3://bucket/object\",\"offset\":null,\"size\":null,"
                        + "\"content_type\":null,\"checksum\":null,\"inline\":null}")), FileType.INSTANCE);
        Assertions.assertTrue(expression.isConstant());
        Assertions.assertTrue(collectConstants(expression).isEmpty());
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
