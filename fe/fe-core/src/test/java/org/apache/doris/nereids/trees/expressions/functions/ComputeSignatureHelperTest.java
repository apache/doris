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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ComputeSignatureHelperTest {

    /////////////////////////////////////////
    // implementFollowToArgumentReturnType
    /////////////////////////////////////////

    @Test
    void testNoImplementFollowToArgumentReturnType() {
        FunctionSignature signature = FunctionSignature.ret(DoubleType.INSTANCE).args(IntegerType.INSTANCE);
        signature = ComputeSignatureHelper.implementFollowToArgumentReturnType(signature, Collections.emptyList());
        Assertions.assertTrue(signature.returnType instanceof DoubleType);
    }

    @Test
    void testArrayImplementFollowToArgumentReturnType() {
        FunctionSignature signature = FunctionSignature.ret(ArrayType.of(new FollowToArgumentType(0)))
                .args(IntegerType.INSTANCE);
        signature = ComputeSignatureHelper.implementFollowToArgumentReturnType(signature, Collections.emptyList());
        Assertions.assertTrue(signature.returnType instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.returnType).getItemType() instanceof IntegerType);
    }

    @Test
    void testMapImplementFollowToArgumentReturnType() {
        FunctionSignature signature = FunctionSignature.ret(MapType.of(
                new FollowToArgumentType(0), new FollowToArgumentType(1)))
                .args(IntegerType.INSTANCE, DoubleType.INSTANCE);
        signature = ComputeSignatureHelper.implementFollowToArgumentReturnType(signature, Collections.emptyList());
        Assertions.assertTrue(signature.returnType instanceof MapType);
        Assertions.assertTrue(((MapType) signature.returnType).getKeyType() instanceof IntegerType);
        Assertions.assertTrue(((MapType) signature.returnType).getValueType() instanceof DoubleType);
    }

    /////////////////////////////////////////
    // implementAnyDataTypeWithOutIndex
    /////////////////////////////////////////

    @Test
    void testNoImplementAnyDataTypeWithOutIndex() {
        FunctionSignature signature = FunctionSignature.ret(DoubleType.INSTANCE).args(IntegerType.INSTANCE);
        signature = ComputeSignatureHelper.implementAnyDataTypeWithOutIndex(signature, Collections.emptyList());
        Assertions.assertTrue(signature.returnType instanceof DoubleType);
    }

    @Test
    void testArraySigWithNullArgImplementAnyDataTypeWithOutIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(ArrayType.of(AnyDataType.INSTANCE_WITHOUT_INDEX));
        List<Expression> arguments = Lists.newArrayList(new NullLiteral());
        signature = ComputeSignatureHelper.implementAnyDataTypeWithOutIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.getArgType(0)).getItemType() instanceof NullType);
    }

    @Test
    void testMapSigWithNullArgImplementAnyDataTypeWithOutIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(MapType.of(AnyDataType.INSTANCE_WITHOUT_INDEX, AnyDataType.INSTANCE_WITHOUT_INDEX));
        List<Expression> arguments = Lists.newArrayList(new NullLiteral());
        signature = ComputeSignatureHelper.implementAnyDataTypeWithOutIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof MapType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getKeyType() instanceof NullType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getValueType() instanceof NullType);
    }

    @Test
    void testArrayImplementAnyDataTypeWithOutIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(ArrayType.of(AnyDataType.INSTANCE_WITHOUT_INDEX));
        List<Expression> arguments = Lists.newArrayList(new ArrayLiteral(Lists.newArrayList(new IntegerLiteral(0))));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithOutIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.getArgType(0)).getItemType() instanceof IntegerType);
    }

    @Test
    void testMapImplementAnyDataTypeWithOutIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(MapType.of(AnyDataType.INSTANCE_WITHOUT_INDEX, AnyDataType.INSTANCE_WITHOUT_INDEX));
        List<Expression> arguments = Lists.newArrayList(new MapLiteral(Lists.newArrayList(new IntegerLiteral(0)),
                Lists.newArrayList(new BigIntLiteral(0))));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithOutIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof MapType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getKeyType() instanceof IntegerType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getValueType() instanceof BigIntType);
    }

    /////////////////////////////////////////
    // implementAnyDataTypeWithIndex
    /////////////////////////////////////////

    @Test
    void testNoImplementAnyDataTypeWithIndex() {
        FunctionSignature signature = FunctionSignature.ret(DoubleType.INSTANCE).args(IntegerType.INSTANCE);
        signature = ComputeSignatureHelper.implementAnyDataTypeWithIndex(signature, Collections.emptyList());
        Assertions.assertTrue(signature.returnType instanceof DoubleType);
    }

    @Test
    void testArraySigWithNullArgImplementAnyDataTypeWithIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(ArrayType.of(new AnyDataType(0)), new AnyDataType(0));
        List<Expression> arguments = Lists.newArrayList(
                new NullLiteral(),
                new BigIntLiteral(0));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.getArgType(0)).getItemType() instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(1) instanceof BigIntType);
    }

    @Test
    void testMapSigWithNullArgImplementAnyDataTypeWithIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(MapType.of(new AnyDataType(0), new AnyDataType(1)),
                        new AnyDataType(0), new AnyDataType(1));
        List<Expression> arguments = Lists.newArrayList(
                new NullLiteral(), new BigIntLiteral(0), new IntegerLiteral(0));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof MapType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getKeyType() instanceof BigIntType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getValueType() instanceof IntegerType);
        Assertions.assertTrue(signature.getArgType(1) instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(2) instanceof IntegerType);
    }

    @Test
    void testArrayImplementAnyDataTypeWithIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(ArrayType.of(new AnyDataType(0)), new AnyDataType(0));
        List<Expression> arguments = Lists.newArrayList(
                new ArrayLiteral(Lists.newArrayList(new IntegerLiteral(0))),
                new BigIntLiteral(0));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.getArgType(0)).getItemType() instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(1) instanceof BigIntType);
    }

    @Test
    void testMapImplementAnyDataTypeWithIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(MapType.of(new AnyDataType(0), new AnyDataType(1)),
                        new AnyDataType(0), new AnyDataType(1));
        List<Expression> arguments = Lists.newArrayList(
                new MapLiteral(Lists.newArrayList(new IntegerLiteral(0)), Lists.newArrayList(new BigIntLiteral(0))),
                new BigIntLiteral(0), new IntegerLiteral(0));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof MapType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getKeyType() instanceof BigIntType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getValueType() instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(1) instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(2) instanceof BigIntType);
    }

    @Test
    void testArraySigWithNullArgWithFollowToAnyImplementAnyDataTypeWithIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(ArrayType.of(new AnyDataType(0)), new AnyDataType(0),
                        ArrayType.of(new FollowToAnyDataType(0)));
        List<Expression> arguments = Lists.newArrayList(
                new NullLiteral(),
                new NullLiteral(),
                new ArrayLiteral(Lists.newArrayList(new SmallIntLiteral((byte) 0))));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.getArgType(0)).getItemType() instanceof SmallIntType);
        Assertions.assertTrue(signature.getArgType(1) instanceof SmallIntType);
        Assertions.assertTrue(signature.getArgType(2) instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.getArgType(2)).getItemType() instanceof SmallIntType);
    }

    @Test
    void testMapSigWithNullArgWithFollowToAnyImplementAnyDataTypeWithIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(MapType.of(new AnyDataType(0), new AnyDataType(1)),
                        new AnyDataType(0), new AnyDataType(1),
                        MapType.of(new FollowToAnyDataType(0), new FollowToAnyDataType(1)));
        List<Expression> arguments = Lists.newArrayList(
                new NullLiteral(), new NullLiteral(), new NullLiteral(),
                new MapLiteral(Lists.newArrayList(new BigIntLiteral(0)),
                        Lists.newArrayList(new IntegerLiteral(0))));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof MapType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getKeyType() instanceof BigIntType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getValueType() instanceof IntegerType);
        Assertions.assertTrue(signature.getArgType(1) instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(2) instanceof IntegerType);
        Assertions.assertTrue(signature.getArgType(3) instanceof MapType);
        Assertions.assertTrue(((MapType) signature.getArgType(3)).getKeyType() instanceof BigIntType);
        Assertions.assertTrue(((MapType) signature.getArgType(3)).getValueType() instanceof IntegerType);
    }

    @Test
    void testArrayWithFollowToAnyImplementAnyDataTypeWithIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(ArrayType.of(new AnyDataType(0)), new AnyDataType(0),
                        ArrayType.of(new FollowToAnyDataType(0)));
        List<Expression> arguments = Lists.newArrayList(
                new ArrayLiteral(Lists.newArrayList(new IntegerLiteral(0))),
                new BigIntLiteral(0),
                new ArrayLiteral(Lists.newArrayList(new IntegerLiteral(0))));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.getArgType(0)).getItemType() instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(1) instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(2) instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.getArgType(2)).getItemType() instanceof BigIntType);
    }

    @Test
    void testMapWithFollowToAnyImplementAnyDataTypeWithIndex() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(MapType.of(new AnyDataType(0), new AnyDataType(1)),
                        new AnyDataType(0), new AnyDataType(1),
                        MapType.of(new FollowToAnyDataType(0), new FollowToAnyDataType(1)));
        List<Expression> arguments = Lists.newArrayList(
                new MapLiteral(Lists.newArrayList(new IntegerLiteral(0)), Lists.newArrayList(new BigIntLiteral(0))),
                new BigIntLiteral(0), new IntegerLiteral(0),
                new MapLiteral(Lists.newArrayList(new IntegerLiteral(0)), Lists.newArrayList(new BigIntLiteral(0))));
        signature = ComputeSignatureHelper.implementAnyDataTypeWithIndex(signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof MapType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getKeyType() instanceof BigIntType);
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getValueType() instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(1) instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(2) instanceof BigIntType);
        Assertions.assertTrue(signature.getArgType(3) instanceof MapType);
        Assertions.assertTrue(((MapType) signature.getArgType(3)).getKeyType() instanceof BigIntType);
        Assertions.assertTrue(((MapType) signature.getArgType(3)).getValueType() instanceof BigIntType);
    }

    @Test
    void testNoNormalizeDecimalV2() {
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE).args();
        signature = ComputeSignatureHelper.normalizeDecimalV2(signature, Collections.emptyList());
        Assertions.assertEquals(IntegerType.INSTANCE, signature.returnType);
    }

    @Test
    void testNormalizeDecimalV2() {
        FunctionSignature signature = FunctionSignature.ret(DecimalV2Type.createDecimalV2Type(15, 3)).args();
        signature = ComputeSignatureHelper.normalizeDecimalV2(signature, Collections.emptyList());
        Assertions.assertEquals(DecimalV2Type.SYSTEM_DEFAULT, signature.returnType);
    }

    @Test
    void testArrayDecimalV3ComputePrecision() {
        FunctionSignature signature = FunctionSignature.ret(BooleanType.INSTANCE)
                .args(ArrayType.of(DecimalV3Type.WILDCARD),
                        ArrayType.of(DecimalV3Type.WILDCARD),
                        DecimalV3Type.WILDCARD,
                        IntegerType.INSTANCE,
                        ArrayType.of(IntegerType.INSTANCE));
        List<Expression> arguments = Lists.newArrayList(
                new ArrayLiteral(Lists.newArrayList(new DecimalV3Literal(new BigDecimal("1.1234")))),
                new NullLiteral(),
                new DecimalV3Literal(new BigDecimal("123.123")),
                new IntegerLiteral(0),
                new ArrayLiteral(Lists.newArrayList(new IntegerLiteral(0))));
        signature = ComputeSignatureHelper.computePrecision(new FakeComputeSignature(), signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(7, 4),
                ((ArrayType) signature.getArgType(0)).getItemType());
        Assertions.assertTrue(signature.getArgType(1) instanceof ArrayType);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(7, 4),
                ((ArrayType) signature.getArgType(1)).getItemType());
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(7, 4),
                signature.getArgType(2));
        Assertions.assertTrue(signature.getArgType(4) instanceof ArrayType);
        Assertions.assertEquals(IntegerType.INSTANCE,
                ((ArrayType) signature.getArgType(4)).getItemType());
    }

    @Test
    void testMapDecimalV3ComputePrecision() {
        FunctionSignature signature = FunctionSignature.ret(BooleanType.INSTANCE)
                .args(MapType.of(DecimalV3Type.WILDCARD, DecimalV3Type.WILDCARD),
                        MapType.of(DecimalV3Type.WILDCARD, DecimalV3Type.WILDCARD),
                        DecimalV3Type.WILDCARD);
        List<Expression> arguments = Lists.newArrayList(
                new MapLiteral(Lists.newArrayList(new DecimalV3Literal(new BigDecimal("1.1234"))),
                        Lists.newArrayList(new DecimalV3Literal(new BigDecimal("12.12345")))),
                new NullLiteral(),
                new DecimalV3Literal(new BigDecimal("123.123")));
        signature = ComputeSignatureHelper.computePrecision(new FakeComputeSignature(), signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof MapType);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(8, 5),
                ((MapType) signature.getArgType(0)).getKeyType());
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(8, 5),
                ((MapType) signature.getArgType(0)).getValueType());
        Assertions.assertTrue(signature.getArgType(1) instanceof MapType);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(8, 5),
                ((MapType) signature.getArgType(1)).getKeyType());
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(8, 5),
                ((MapType) signature.getArgType(1)).getValueType());
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(8, 5),
                signature.getArgType(2));
    }

    @Test
    void testArrayDateTimeV2ComputePrecision() {
        FunctionSignature signature = FunctionSignature.ret(BooleanType.INSTANCE)
                .args(ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT),
                        ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT),
                        DateTimeV2Type.SYSTEM_DEFAULT,
                        IntegerType.INSTANCE,
                        ArrayType.of(IntegerType.INSTANCE));
        List<Expression> arguments = Lists.newArrayList(
                new ArrayLiteral(Lists.newArrayList(new DateTimeV2Literal("2020-02-02 00:00:00.123"))),
                new NullLiteral(),
                new DateTimeV2Literal("2020-02-02 00:00:00.12"),
                new IntegerLiteral(0),
                new ArrayLiteral(Lists.newArrayList(new IntegerLiteral(0))));
        signature = ComputeSignatureHelper.computePrecision(new FakeComputeSignature(), signature, arguments);
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        Assertions.assertEquals(DateTimeV2Type.of(3),
                ((ArrayType) signature.getArgType(0)).getItemType());
        Assertions.assertTrue(signature.getArgType(1) instanceof ArrayType);
        Assertions.assertEquals(DateTimeV2Type.of(3),
                ((ArrayType) signature.getArgType(1)).getItemType());
        Assertions.assertEquals(DateTimeV2Type.of(3),
                signature.getArgType(2));
        Assertions.assertTrue(signature.getArgType(4) instanceof ArrayType);
        Assertions.assertEquals(IntegerType.INSTANCE,
                ((ArrayType) signature.getArgType(4)).getItemType());
    }

    @Test
    void testMapDateTimeV2ComputePrecision() {
        FunctionSignature signature = FunctionSignature.ret(BooleanType.INSTANCE)
                .args(MapType.of(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT),
                        MapType.of(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT),
                        DateTimeV2Type.SYSTEM_DEFAULT);
        List<Expression> arguments = Lists.newArrayList(
                new MapLiteral(Lists.newArrayList(new DateTimeV2Literal("2020-02-02 00:00:00.123")),
                        Lists.newArrayList(new DateTimeV2Literal("2020-02-02 00:00:00.12"))),
                new NullLiteral(),
                new DateTimeV2Literal("2020-02-02 00:00:00.1234"));
        signature = ComputeSignatureHelper.computePrecision(new FakeComputeSignature(), signature, arguments);
        Assertions.assertInstanceOf(MapType.class, signature.getArgType(0));
        Assertions.assertEquals(DateTimeV2Type.of(4),
                ((MapType) signature.getArgType(0)).getKeyType());
        Assertions.assertEquals(DateTimeV2Type.of(4),
                ((MapType) signature.getArgType(0)).getValueType());
        Assertions.assertInstanceOf(MapType.class, signature.getArgType(1));
        Assertions.assertEquals(DateTimeV2Type.of(4),
                ((MapType) signature.getArgType(1)).getKeyType());
        Assertions.assertEquals(DateTimeV2Type.of(4),
                ((MapType) signature.getArgType(1)).getValueType());
        Assertions.assertEquals(DateTimeV2Type.of(4),
                signature.getArgType(2));
    }

    private static class FakeComputeSignature implements ComputeSignature {
        @Override
        public List<Expression> children() {
            return null;
        }

        @Override
        public Expression child(int index) {
            return null;
        }

        @Override
        public int arity() {
            return 0;
        }

        @Override
        public <T> Optional<T> getMutableState(String key) {
            return Optional.empty();
        }

        @Override
        public void setMutableState(String key, Object value) {

        }

        @Override
        public Expression withChildren(List<Expression> children) {
            return null;
        }

        @Override
        public List<FunctionSignature> getSignatures() {
            return null;
        }

        @Override
        public FunctionSignature getSignature() {
            return null;
        }

        @Override
        public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
            return null;
        }

        @Override
        public boolean nullable() {
            return false;
        }
    }
}
