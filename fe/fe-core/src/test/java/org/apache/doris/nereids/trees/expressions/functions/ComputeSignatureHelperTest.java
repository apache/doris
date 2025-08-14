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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.VariantType;
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

    @Test
    void testTimeV2PrecisionPromotion() {
        FunctionSignature signature = FunctionSignature.ret(BooleanType.INSTANCE).args(TimeV2Type.INSTANCE,
                        TimeV2Type.INSTANCE, TimeV2Type.INSTANCE);
        List<Expression> arguments = Lists.newArrayList(new TimeV2Literal("12:34:56.12"),
                        new TimeV2Literal("12:34:56.123"), new TimeV2Literal("12:34:56.1"));
        signature = ComputeSignatureHelper.computePrecision(new FakeComputeSignature(), signature, arguments);

        // All arguments should be promoted to the highest precision (3)
        Assertions.assertEquals(TimeV2Type.of(3), signature.getArgType(0));
        Assertions.assertEquals(TimeV2Type.of(3), signature.getArgType(1));
        Assertions.assertEquals(TimeV2Type.of(3), signature.getArgType(2));
    }

    @Test
    void testMixedDateTimeV2AndTimeV2PrecisionPromotion() {
        FunctionSignature signature = FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT).args(
                        DateTimeV2Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        List<Expression> arguments = Lists.newArrayList(new DateTimeV2Literal("2020-02-02 00:00:00.12"),
                        new TimeV2Literal("12:34:56.123"), new DateTimeV2Literal("2020-02-02 00:00:00.1"));
        signature = ComputeSignatureHelper.computePrecision(new FakeComputeSignature(), signature, arguments);

        // All arguments should be promoted to the highest precision (3)
        Assertions.assertEquals(DateTimeV2Type.of(3), signature.getArgType(0));
        Assertions.assertEquals(TimeV2Type.of(3), signature.getArgType(1));
        Assertions.assertEquals(DateTimeV2Type.of(3), signature.getArgType(2));
        // Return type should also be promoted to precision 3
        Assertions.assertEquals(DateTimeV2Type.of(3), signature.returnType);
    }

    @Test
    void testNestedTimeV2PrecisionPromotion() {
        FunctionSignature signature = FunctionSignature.ret(ArrayType.of(TimeV2Type.INSTANCE)).args(
                        ArrayType.of(TimeV2Type.INSTANCE),
                        MapType.of(IntegerType.INSTANCE, TimeV2Type.INSTANCE), TimeV2Type.INSTANCE);
        List<Expression> arguments = Lists.newArrayList(
                        new ArrayLiteral(Lists.newArrayList(new TimeV2Literal("12:34:56.12"))),
                        new MapLiteral(Lists.newArrayList(new IntegerLiteral(1)),
                                        Lists.newArrayList(new TimeV2Literal("12:34:56.1234"))),
                        new TimeV2Literal("12:34:56.123"));
        signature = ComputeSignatureHelper.computePrecision(new FakeComputeSignature(), signature, arguments);

        // Check array argument (precision should be 4 from the map value)
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        Assertions.assertEquals(TimeV2Type.of(4), ((ArrayType) signature.getArgType(0)).getItemType());

        // Check map argument
        Assertions.assertTrue(signature.getArgType(1) instanceof MapType);
        Assertions.assertEquals(IntegerType.INSTANCE, ((MapType) signature.getArgType(1)).getKeyType());
        Assertions.assertEquals(TimeV2Type.of(4), ((MapType) signature.getArgType(1)).getValueType());

        // Check scalar argument
        Assertions.assertEquals(TimeV2Type.of(4), signature.getArgType(2));

        // Check return type
        Assertions.assertTrue(signature.returnType instanceof ArrayType);
        Assertions.assertEquals(TimeV2Type.of(4), ((ArrayType) signature.returnType).getItemType());
    }

    @Test
    void testComplexNestedMixedTimePrecisionPromotion() {
        // Create a complex nested structure with both DateTimeV2 and TimeV2 types
        FunctionSignature signature = FunctionSignature
                        .ret(MapType.of(DateTimeV2Type.SYSTEM_DEFAULT, ArrayType.of(TimeV2Type.INSTANCE)))
                        .args(MapType.of(DateTimeV2Type.SYSTEM_DEFAULT, ArrayType.of(TimeV2Type.INSTANCE)),
                                        ArrayType.of(MapType.of(TimeV2Type.INSTANCE,
                                                        DateTimeV2Type.SYSTEM_DEFAULT)),
                                        DateTimeV2Type.SYSTEM_DEFAULT);

        // Create complex arguments with different precisions
        List<Expression> arguments = Lists.newArrayList(
                        // Map(DateTimeV2(2) -> Array(TimeV2(1)))
                        new MapLiteral(Lists.newArrayList(new DateTimeV2Literal("2020-02-02 00:00:00.12")),
                                        Lists.newArrayList(new ArrayLiteral(
                                                        Lists.newArrayList(new TimeV2Literal("12:34:56.1"))))),
                        // Array(Map(TimeV2(3) -> DateTimeV2(0)))
                        new ArrayLiteral(Lists.newArrayList(new MapLiteral(
                                        Lists.newArrayList(new TimeV2Literal("12:34:56.123")),
                                        Lists.newArrayList(new DateTimeV2Literal("2020-02-02 00:00:00"))))),
                        // DateTimeV2(4)
                        new DateTimeV2Literal("2020-02-02 00:00:00.1234"));

        signature = ComputeSignatureHelper.computePrecision(new FakeComputeSignature(), signature, arguments);

        // All time types should be promoted to precision 4

        // Check first argument: Map(DateTimeV2 -> Array(TimeV2))
        Assertions.assertTrue(signature.getArgType(0) instanceof MapType);
        Assertions.assertEquals(DateTimeV2Type.of(4), ((MapType) signature.getArgType(0)).getKeyType());
        Assertions.assertTrue(((MapType) signature.getArgType(0)).getValueType() instanceof ArrayType);
        Assertions.assertEquals(TimeV2Type.of(4),
                        ((ArrayType) ((MapType) signature.getArgType(0)).getValueType()).getItemType());

        // Check second argument: Array(Map(TimeV2 -> DateTimeV2))
        Assertions.assertTrue(signature.getArgType(1) instanceof ArrayType);
        Assertions.assertTrue(((ArrayType) signature.getArgType(1)).getItemType() instanceof MapType);
        Assertions.assertEquals(TimeV2Type.of(4),
                        ((MapType) ((ArrayType) signature.getArgType(1)).getItemType()).getKeyType());
        Assertions.assertEquals(DateTimeV2Type.of(4),
                        ((MapType) ((ArrayType) signature.getArgType(1)).getItemType()).getValueType());

        // Check third argument: DateTimeV2
        Assertions.assertEquals(DateTimeV2Type.of(4), signature.getArgType(2));

        // Check return type: Map(DateTimeV2 -> Array(TimeV2))
        Assertions.assertTrue(signature.returnType instanceof MapType);
        Assertions.assertEquals(DateTimeV2Type.of(4), ((MapType) signature.returnType).getKeyType());
        Assertions.assertTrue(((MapType) signature.returnType).getValueType() instanceof ArrayType);
        Assertions.assertEquals(TimeV2Type.of(4),
                        ((ArrayType) ((MapType) signature.returnType).getValueType()).getItemType());
    }

    @Test
    void testNoDynamicComputeVariantArgs() {
        FunctionSignature signature = FunctionSignature.ret(DoubleType.INSTANCE).args(IntegerType.INSTANCE);
        signature = ComputeSignatureHelper.dynamicComputeVariantArgs(signature, Collections.emptyList());
        Assertions.assertTrue(signature.returnType instanceof DoubleType);
    }

    @Test
    void testDynamicComputeVariantArgsSingleVariant() {
        VariantType variantType = new VariantType(100);
        FunctionSignature signature = FunctionSignature.ret(VariantType.INSTANCE)
                .args(VariantType.INSTANCE, IntegerType.INSTANCE);

        List<Expression> arguments = Lists.newArrayList(
                new MockVariantExpression(variantType),
                new IntegerLiteral(42));

        signature = ComputeSignatureHelper.dynamicComputeVariantArgs(signature, arguments);

        Assertions.assertTrue(signature.returnType instanceof VariantType);
        Assertions.assertEquals(100, ((VariantType) signature.returnType).getVariantMaxSubcolumnsCount());

        Assertions.assertTrue(signature.getArgType(0) instanceof VariantType);
        Assertions.assertEquals(100, ((VariantType) signature.getArgType(0)).getVariantMaxSubcolumnsCount());

        Assertions.assertTrue(signature.getArgType(1) instanceof IntegerType);
    }

    @Test
    void testDynamicComputeVariantArgsMultipleVariants() {
        VariantType variantType1 = new VariantType(150);
        VariantType variantType2 = new VariantType(250);
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(VariantType.INSTANCE, VariantType.INSTANCE);

        List<Expression> arguments = Lists.newArrayList(
                new MockVariantExpression(variantType1),
                new MockVariantExpression(variantType2));

        signature = ComputeSignatureHelper.dynamicComputeVariantArgs(signature, arguments);

        Assertions.assertTrue(signature.getArgType(0) instanceof VariantType);
        Assertions.assertEquals(150, ((VariantType) signature.getArgType(0)).getVariantMaxSubcolumnsCount());
        Assertions.assertTrue(signature.getArgType(1) instanceof VariantType);
        Assertions.assertEquals(250, ((VariantType) signature.getArgType(1)).getVariantMaxSubcolumnsCount());
        Assertions.assertTrue(signature.returnType instanceof IntegerType);
    }

    @Test
    void testDynamicComputeVariantArgsMixedTypesWithSingleVariant() {
        VariantType variantType = new VariantType(75);
        FunctionSignature signature = FunctionSignature.ret(BooleanType.INSTANCE)
                .args(VariantType.INSTANCE, IntegerType.INSTANCE, DoubleType.INSTANCE);

        List<Expression> arguments = Lists.newArrayList(
                new MockVariantExpression(variantType),
                new IntegerLiteral(10),
                new DoubleLiteral(3.14));

        signature = ComputeSignatureHelper.dynamicComputeVariantArgs(signature, arguments);

        Assertions.assertTrue(signature.getArgType(0) instanceof VariantType);
        Assertions.assertEquals(75, ((VariantType) signature.getArgType(0)).getVariantMaxSubcolumnsCount());

        Assertions.assertTrue(signature.getArgType(1) instanceof IntegerType);
        Assertions.assertTrue(signature.getArgType(2) instanceof DoubleType);

        Assertions.assertTrue(signature.returnType instanceof BooleanType);
    }

    @Test
    void testDynamicComputeVariantArgsWithNullLiteral() {
        FunctionSignature signature = FunctionSignature.ret(BooleanType.INSTANCE)
                .args(VariantType.INSTANCE, IntegerType.INSTANCE);

        List<Expression> arguments = Lists.newArrayList(
                new NullLiteral(),
                new IntegerLiteral(10));

        signature = ComputeSignatureHelper.dynamicComputeVariantArgs(signature, arguments);

        Assertions.assertTrue(signature.getArgType(0) instanceof VariantType);
        Assertions.assertEquals(0, ((VariantType) signature.getArgType(0)).getVariantMaxSubcolumnsCount());
        Assertions.assertTrue(signature.getArgType(1) instanceof IntegerType);
    }

    @Test
    void testDynamicComputeVariantArgsNoVariantReturnType() {
        VariantType variantType = new VariantType(300);
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE)
                .args(VariantType.INSTANCE);

        List<Expression> arguments = Lists.newArrayList(
                new MockVariantExpression(variantType));

        signature = ComputeSignatureHelper.dynamicComputeVariantArgs(signature, arguments);

        Assertions.assertTrue(signature.returnType instanceof IntegerType);

        Assertions.assertTrue(signature.getArgType(0) instanceof VariantType);
        Assertions.assertEquals(300, ((VariantType) signature.getArgType(0)).getVariantMaxSubcolumnsCount());
    }

    @Test
    void testDynamicComputeVariantArgsWithVarArgsThrowsException() {
        VariantType variantType1 = new VariantType(150);
        VariantType variantType2 = new VariantType(250);
        FunctionSignature signature = FunctionSignature.ret(VariantType.INSTANCE)
                .args(VariantType.INSTANCE, VariantType.INSTANCE);

        List<Expression> arguments = Lists.newArrayList(
                new MockVariantExpression(variantType1),
                new MockVariantExpression(variantType2));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            ComputeSignatureHelper.dynamicComputeVariantArgs(signature, arguments);
        });

        Assertions.assertEquals("variant type is not supported in multiple arguments", exception.getMessage());
    }

    @Test
    void testDynamicComputeVariantArgsWithComputeSignature() {
        VariantType variantType = new VariantType(200);
        FunctionSignature signature = FunctionSignature.ret(VariantType.INSTANCE)
                .args(VariantType.INSTANCE);

        List<Expression> arguments = Lists.newArrayList(
                new MockVariantExpression(variantType));

        signature = ComputeSignatureHelper.dynamicComputeVariantArgs(signature, arguments);

        Assertions.assertTrue(signature.returnType instanceof VariantType);
        Assertions.assertEquals(200, ((VariantType) signature.returnType).getVariantMaxSubcolumnsCount());
        Assertions.assertTrue(signature.getArgType(0) instanceof VariantType);
        Assertions.assertEquals(200, ((VariantType) signature.getArgType(0)).getVariantMaxSubcolumnsCount());
    }

    /**
     * Mock Expression class for testing VariantType
     */
    private static class MockVariantExpression extends Expression {
        private final VariantType variantType;

        public MockVariantExpression(VariantType variantType) {
            super(Collections.emptyList());
            this.variantType = variantType;
        }

        @Override
        public DataType getDataType() {
            return variantType;
        }

        @Override
        public boolean nullable() {
            return true;
        }

        @Override
        public Expression withChildren(List<Expression> children) {
            return this;
        }

        @Override
        public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
            return visitor.visit(this, context);
        }

        @Override
        public int arity() {
            return 0;
        }

        @Override
        public Expression child(int index) {
            throw new IndexOutOfBoundsException("MockVariantExpression has no children");
        }

        @Override
        public List<Expression> children() {
            return Collections.emptyList();
        }
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
