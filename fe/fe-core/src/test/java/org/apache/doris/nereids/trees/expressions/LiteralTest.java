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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.proto.Types.PGenericType;
import org.apache.doris.proto.Types.PGenericType.TypeId;
import org.apache.doris.proto.Types.PValues;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

class LiteralTest {

    @Test
    public void testEqual() {
        IntegerLiteral one = new IntegerLiteral(1);
        IntegerLiteral anotherOne = new IntegerLiteral(1);
        IntegerLiteral two = new IntegerLiteral(2);
        Assertions.assertNotEquals(one, two);
        Assertions.assertEquals(one, anotherOne);
        StringLiteral str1 = new StringLiteral("hello");
        Assertions.assertNotEquals(str1, one);
        Assertions.assertTrue(Literal.of("world") instanceof StringLiteral);
        Assertions.assertTrue(Literal.of(null) instanceof NullLiteral);
        Assertions.assertTrue(Literal.of(1) instanceof IntegerLiteral);
        Assertions.assertTrue(Literal.of(false) instanceof BooleanLiteral);
    }

    @Test
    public void testGetResultExpressionTimeStampNs() {
        assertTimeStampNsResult(Long.MIN_VALUE, "1677-09-21 00:12:43.145224192");
        assertTimeStampNsResult(123456800L, "1970-01-01 00:00:00.123456800");
        assertTimeStampNsResult(-10L, "1969-12-31 23:59:59.999999990");
        assertTimeStampNsResult(-1L, "1969-12-31 23:59:59.999999999");
        assertTimeStampNsResult(Long.MAX_VALUE, "2262-04-11 23:47:16.854775807");
    }

    @Test
    public void testGetResultExpressionNullableTimeStampNs() {
        PValues values = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.INT64).build())
                .addInt64Value(-1L)
                .addInt64Value(0L)
                .setHasNull(true)
                .addNullMap(false)
                .addNullMap(true)
                .build();

        List<Literal> result = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE
                .getResultExpression(TimeStampNsType.INSTANCE, values);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("1969-12-31 23:59:59.999999999", result.get(0).getStringValue());
        Assertions.assertInstanceOf(NullLiteral.class, result.get(1));
    }

    @Test
    public void testGetResultExpressionNestedTimeStampNs() {
        TimeStampNsType nanoType = TimeStampNsType.INSTANCE;
        PGenericType int64Type = PGenericType.newBuilder().setId(TypeId.INT64).build();
        PValues nanoValues = PValues.newBuilder()
                .setType(int64Type)
                .addInt64Value(-1L)
                .addInt64Value(123456789L)
                .build();
        PValues arrayValues = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.LIST).build())
                .addChildElement(nanoValues)
                .addChildOffset(2)
                .build();

        List<Literal> result = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE
                .getResultExpression(ArrayType.of(nanoType), arrayValues);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0) instanceof ArrayLiteral);
        List<Literal> items = ((ArrayLiteral) result.get(0)).getValue();
        Assertions.assertEquals("1969-12-31 23:59:59.999999999",
                ((TimeStampNsLiteral) items.get(0)).getStringValue());
        Assertions.assertEquals("1970-01-01 00:00:00.123456789",
                ((TimeStampNsLiteral) items.get(1)).getStringValue());
    }

    @Test
    public void testGetResultExpressionDateTimeV2AlongsideTimeStampNs() {
        long packedDateTime = (2024L << 46) | (1L << 42) | (2L << 37)
                | (3L << 32) | (4L << 26) | (5L << 20) | 123456L;
        PValues values = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.UINT64).build())
                .addUint64Value(packedDateTime)
                .addUint64Value(0)
                .setHasNull(true)
                .build();

        List<Literal> result = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE
                .getResultExpression(DateTimeV2Type.of(6), values);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("2024-01-02 03:04:05.123456", result.get(0).getStringValue());
        Assertions.assertInstanceOf(NullLiteral.class, result.get(1));
    }

    private void assertTimeStampNsResult(long epochNanos, String expected) {
        PValues values = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.INT64).build())
                .addInt64Value(epochNanos)
                .build();
        List<Literal> result = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE
                .getResultExpression(TimeStampNsType.INSTANCE, values);

        Assertions.assertEquals(1, result.size());
        Assertions.assertInstanceOf(TimeStampNsLiteral.class, result.get(0));
        TimeStampNsLiteral literal = (TimeStampNsLiteral) result.get(0);
        Assertions.assertSame(TimeStampNsType.INSTANCE, literal.getDataType());
        Assertions.assertEquals(expected, literal.getStringValue());
    }

    @Test
    public void testGetResultExpressionArrayInt() {
        int num = 10;
        Integer[] elementsArray = new Integer[num];
        for (int i = 0; i < elementsArray.length; ++i) {
            elementsArray[i] = i;
        }
        DataType arrayType = ArrayType.of(IntegerType.INSTANCE);
        PGenericType.Builder childTypeBuilder = PGenericType.newBuilder();
        childTypeBuilder.setId(TypeId.INT32);
        PGenericType.Builder typeBuilder = PGenericType.newBuilder();
        typeBuilder.setId(TypeId.LIST);

        PValues.Builder childBuilder = PValues.newBuilder();
        PValues.Builder resultContentBuilder = PValues.newBuilder();
        for (int value : elementsArray) {
            childBuilder.addInt32Value(value);
        }
        childBuilder.setType(childTypeBuilder.build());
        PValues childValues = childBuilder.build();
        resultContentBuilder.setType(typeBuilder.build());
        resultContentBuilder.addChildElement(childValues);
        resultContentBuilder.addChildOffset(10);
        PValues resultContent = resultContentBuilder.build();
        List<Literal> resultExpression
                = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE.getResultExpression(arrayType,
                resultContent);
        Assertions.assertTrue(resultExpression.get(0).isLiteral());
    }

    @Test
    public void testGetResultExpressionArrayArrayInt() {
        int num = 10;
        Integer[] elementsArray = new Integer[num];
        for (int i = 0; i < elementsArray.length; ++i) {
            elementsArray[i] = i;
        }
        DataType nestedArrayType = ArrayType.of(IntegerType.INSTANCE);
        DataType outArrayType = ArrayType.of(nestedArrayType);
        PGenericType.Builder childTypeBuilder = PGenericType.newBuilder();
        childTypeBuilder.setId(TypeId.INT32);
        PGenericType.Builder typeBuilder = PGenericType.newBuilder();
        typeBuilder.setId(TypeId.LIST);
        PGenericType.Builder outTypeBuilder = PGenericType.newBuilder();
        outTypeBuilder.setId(TypeId.LIST);

        PValues.Builder childBuilder = PValues.newBuilder();
        PValues.Builder nestedContentBuilder = PValues.newBuilder();
        PValues.Builder outContentBuilder = PValues.newBuilder();
        for (int value : elementsArray) {
            childBuilder.addInt32Value(value);
        }
        childBuilder.setType(childTypeBuilder.build());
        PValues childValues = childBuilder.build();
        nestedContentBuilder.setType(typeBuilder.build());
        nestedContentBuilder.addChildElement(childValues);
        nestedContentBuilder.addChildOffset(10);
        PValues nestedResultContent = nestedContentBuilder.build();
        outContentBuilder.setType(outTypeBuilder.build());
        outContentBuilder.addChildElement(nestedResultContent);
        outContentBuilder.addChildOffset(1);
        PValues resultContent = outContentBuilder.build();
        List<Literal> resultExpression
                = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE.getResultExpression(outArrayType,
                resultContent);
        // [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]
        Assertions.assertTrue(resultExpression.get(0) instanceof ArrayLiteral);
        System.out.println(resultExpression.get(0).toString());
    }

    @Test
    public void testGetResultExpressionArrayArrayInt2() {
        int num = 10;
        Integer[] elementsArray = new Integer[num];
        for (int i = 0; i < elementsArray.length; ++i) {
            elementsArray[i] = i;
        }
        DataType nestedArrayType = ArrayType.of(IntegerType.INSTANCE);
        DataType outArrayType = ArrayType.of(nestedArrayType);
        PGenericType.Builder childTypeBuilder = PGenericType.newBuilder();
        childTypeBuilder.setId(TypeId.INT32);
        PGenericType.Builder typeBuilder = PGenericType.newBuilder();
        typeBuilder.setId(TypeId.LIST);
        PGenericType.Builder outTypeBuilder = PGenericType.newBuilder();
        outTypeBuilder.setId(TypeId.LIST);

        PValues.Builder childBuilder1 = PValues.newBuilder();
        PValues.Builder childBuilder2 = PValues.newBuilder();
        PValues.Builder nestedContentBuilder = PValues.newBuilder();
        PValues.Builder outContentBuilder = PValues.newBuilder();
        for (int i = 0; i < elementsArray.length; i = i + 2) {
            childBuilder1.addInt32Value(elementsArray[i]);
            childBuilder2.addInt32Value(elementsArray[i + 1]);
        }
        childBuilder1.setType(childTypeBuilder.build());
        childBuilder2.setType(childTypeBuilder.build());
        PValues childValues1 = childBuilder1.build();
        PValues childValues2 = childBuilder2.build();
        nestedContentBuilder.setType(typeBuilder.build());
        nestedContentBuilder.addChildElement(childValues1);
        nestedContentBuilder.addChildElement(childValues2);
        nestedContentBuilder.addChildOffset(5);
        nestedContentBuilder.addChildOffset(10);
        PValues nestedResultContent = nestedContentBuilder.build();
        outContentBuilder.setType(outTypeBuilder.build());
        outContentBuilder.addChildElement(nestedResultContent);
        outContentBuilder.addChildOffset(2);
        PValues resultContent = outContentBuilder.build();
        List<Literal> resultExpression
                = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE.getResultExpression(outArrayType,
                resultContent);
        // [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]
        Assertions.assertTrue(resultExpression.get(0) instanceof ArrayLiteral);
        System.out.println(resultExpression.get(0).toString());
    }

    @Test
    public void testGetResultExpressionArrayNull() {
        int num = 10;
        Integer[] elementsArray = new Integer[num];
        Boolean[] nullMap = new Boolean[num];
        for (int i = 0; i < elementsArray.length; ++i) {
            elementsArray[i] = i;
            nullMap[i] = (i % 2 == 1);
        }
        DataType arrayType = ArrayType.of(IntegerType.INSTANCE);
        PGenericType.Builder childTypeBuilder = PGenericType.newBuilder();
        childTypeBuilder.setId(TypeId.INT32);
        PGenericType.Builder typeBuilder = PGenericType.newBuilder();
        typeBuilder.setId(TypeId.LIST);

        PValues.Builder childBuilder = PValues.newBuilder();
        PValues.Builder resultContentBuilder = PValues.newBuilder();
        for (int value : elementsArray) {
            childBuilder.addInt32Value(value);
        }
        childBuilder.setType(childTypeBuilder.build());
        childBuilder.setHasNull(true);
        childBuilder.addAllNullMap(Arrays.asList(nullMap));
        PValues childValues = childBuilder.build();
        resultContentBuilder.setType(typeBuilder.build());
        resultContentBuilder.addChildElement(childValues);
        resultContentBuilder.addChildOffset(10);
        PValues resultContent = resultContentBuilder.build();
        List<Literal> resultExpression
                = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE.getResultExpression(arrayType,
                resultContent);
        // [0, NULL, 2, NULL, 4, NULL, 6, NULL, 8, NULL]
        Assertions.assertTrue(resultExpression.get(0).isLiteral());
        System.out.println(resultExpression.get(0).toString());
    }

    @Test
    public void testGetResultExpressionStruct() {
        int num = 10;
        Integer[] elementsArray = new Integer[num];
        for (int i = 0; i < elementsArray.length; ++i) {
            elementsArray[i] = i;
        }
        List<StructField> typeFields = new ArrayList<>();
        typeFields.add(new StructField("col1", IntegerType.INSTANCE, true, "comment1"));
        typeFields.add(new StructField("col2", StringType.INSTANCE, true, "comment1"));

        DataType structType = new StructType(typeFields);
        PGenericType.Builder childTypeBuilder1 = PGenericType.newBuilder();
        childTypeBuilder1.setId(TypeId.INT32);
        PGenericType.Builder childTypeBuilder2 = PGenericType.newBuilder();
        childTypeBuilder2.setId(TypeId.STRING);
        PGenericType.Builder typeBuilder = PGenericType.newBuilder();
        typeBuilder.setId(TypeId.STRUCT);

        PValues.Builder childBuilder1 = PValues.newBuilder();
        PValues.Builder childBuilder2 = PValues.newBuilder();
        PValues.Builder resultContentBuilder = PValues.newBuilder();
        for (int i = 0; i < elementsArray.length; i = i + 2) {
            childBuilder1.addInt32Value(elementsArray[i]);
            childBuilder2.addStringValue("str" + (i + 1));
        }
        childBuilder1.setType(childTypeBuilder1.build());
        childBuilder2.setType(childTypeBuilder2.build());
        PValues childValues1 = childBuilder1.build();
        PValues childValues2 = childBuilder2.build();

        resultContentBuilder.setType(typeBuilder.build());
        resultContentBuilder.addChildElement(childValues1);
        resultContentBuilder.addChildElement(childValues2);
        PValues resultContent = resultContentBuilder.build();
        List<Literal> resultExpression
                = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE.getResultExpression(structType,
                resultContent);
        Assertions.assertTrue(resultExpression.get(0).isLiteral());
        // STRUCT('col1':0,'col2':'str1')
        System.out.println(resultExpression.get(0).toString());
    }

    @Test
    public void testGetResultExpressionStructArray() {
        int num = 10;
        Integer[] elementsArray = new Integer[num];
        for (int i = 0; i < elementsArray.length; ++i) {
            elementsArray[i] = i;
        }
        List<StructField> typeFields = new ArrayList<>();
        typeFields.add(new StructField("col1", ArrayType.of(IntegerType.INSTANCE), true, "comment1"));
        typeFields.add(new StructField("col2", ArrayType.of(StringType.INSTANCE), true, "comment1"));

        DataType structType = new StructType(typeFields);
        PGenericType.Builder childTypeBuilder1 = PGenericType.newBuilder();
        childTypeBuilder1.setId(TypeId.INT32);
        PGenericType.Builder childTypeBuilder2 = PGenericType.newBuilder();
        childTypeBuilder2.setId(TypeId.STRING);
        PGenericType.Builder childTypeBuilder3 = PGenericType.newBuilder();
        childTypeBuilder3.setId(TypeId.LIST);
        PGenericType.Builder typeBuilder = PGenericType.newBuilder();
        typeBuilder.setId(TypeId.STRUCT);

        PValues.Builder childBuilder1 = PValues.newBuilder();
        PValues.Builder childBuilder2 = PValues.newBuilder();
        PValues.Builder arrayChildBuilder1 = PValues.newBuilder();
        PValues.Builder arrayChildBuilder2 = PValues.newBuilder();
        PValues.Builder resultContentBuilder = PValues.newBuilder();
        for (int i = 0; i < elementsArray.length; i = i + 2) {
            childBuilder1.addInt32Value(elementsArray[i]);
            childBuilder2.addStringValue("str" + (i + 1));
        }
        childBuilder1.setType(childTypeBuilder1.build());
        childBuilder2.setType(childTypeBuilder2.build());
        arrayChildBuilder1.setType(childTypeBuilder3.build());
        arrayChildBuilder2.setType(childTypeBuilder3.build());

        PValues childValues1 = childBuilder1.build();
        PValues childValues2 = childBuilder2.build();
        arrayChildBuilder1.addChildElement(childValues1);
        arrayChildBuilder1.addChildOffset(5);
        arrayChildBuilder2.addChildElement(childValues2);
        arrayChildBuilder2.addChildOffset(5);
        PValues arrayChildValues1 = arrayChildBuilder1.build();
        PValues arrayChildValues2 = arrayChildBuilder2.build();

        resultContentBuilder.setType(typeBuilder.build());
        resultContentBuilder.addChildElement(arrayChildValues1);
        resultContentBuilder.addChildElement(arrayChildValues2);
        PValues resultContent = resultContentBuilder.build();
        List<Literal> resultExpression
                = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE.getResultExpression(structType,
                resultContent);
        Assertions.assertTrue(resultExpression.get(0).isLiteral());
        // STRUCT('col1':[0, 2, 4, 6, 8],'col2':['str1', 'str3', 'str5', 'str7', 'str9'])
        System.out.println(resultExpression.get(0).toString());
    }

    @Test
    public void testGetResultExpressionStringKeepsItsBytes() {
        // BE sends the bytes of a string result as they are. Valid UTF-8, including a NUL byte, a supplementary
        // character (U+1F600) and U+FFFD itself, folds to a literal with the same bytes.
        List<String> hexes = ImmutableList.of("", "61", "C3A9", "610062", "F09F9880", "EFBFBD");
        List<Literal> result = org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE
                .getResultExpression(StringType.INSTANCE, stringValues(hexes));
        Assertions.assertEquals(hexes, stringLeaves(result));
    }

    @Test
    public void testGetResultExpressionLeavesInvalidUtf8Unfolded() {
        // A FE string literal cannot hold these bytes: 0xFF (unhex('FF')), a truncated sequence, an overlong NUL
        // and a UTF-16 surrogate. Decoding them would give U+FFFD, so neither the string nor an array, a map or
        // a struct holding it is folded, and BE evaluates the expression.
        for (String invalid : ImmutableList.of("FF", "61FF", "C3", "C080", "EDA080")) {
            for (Pair<DataType, PValues> result : stringResults(invalid)) {
                Assertions.assertEquals(ImmutableList.of(), org.apache.doris.nereids.rules.expression.rules
                        .FoldConstantRuleOnBE.getResultExpression(result.first, result.second),
                        invalid + " in " + result.first);
            }
        }
        // The same results fold when the bytes are valid UTF-8, here 'é' (C3A9).
        for (Pair<DataType, PValues> result : stringResults("C3A9")) {
            Assertions.assertEquals(ImmutableList.of("61", "C3A9"), stringLeaves(org.apache.doris.nereids.rules
                    .expression.rules.FoldConstantRuleOnBE.getResultExpression(result.first, result.second)),
                    result.first.toString());
        }
    }

    // Results of each shape that hold the strings 'a' and the bytes of secondHex, laid out as BE writes them.
    private static List<Pair<DataType, PValues>> stringResults(String secondHex) {
        PValues strings = stringValues(ImmutableList.of("61", secondHex));
        PValues ints = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.INT32).build())
                .addInt32Value(1)
                .addInt32Value(2)
                .build();
        PValues array = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.LIST).build())
                .addChildElement(strings)
                .addChildOffset(2)
                .build();
        PValues stringKeyMap = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.MAP).build())
                .addChildElement(strings)
                .addChildElement(ints)
                .addChildOffset(2)
                .build();
        PValues stringValueMap = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.MAP).build())
                .addChildElement(ints)
                .addChildElement(strings)
                .addChildOffset(2)
                .build();
        PValues struct = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.STRUCT).build())
                .addChildElement(ints)
                .addChildElement(strings)
                .build();
        StructType structType = new StructType(ImmutableList.of(
                new StructField("i", IntegerType.INSTANCE, true, ""),
                new StructField("s", StringType.INSTANCE, true, "")));
        return ImmutableList.of(
                Pair.of(StringType.INSTANCE, strings),
                Pair.of(VarcharType.createVarcharType(10), strings),
                Pair.of(CharType.createCharType(10), strings),
                Pair.of(ArrayType.of(StringType.INSTANCE), array),
                Pair.of(MapType.of(StringType.INSTANCE, IntegerType.INSTANCE), stringKeyMap),
                Pair.of(MapType.of(IntegerType.INSTANCE, StringType.INSTANCE), stringValueMap),
                Pair.of(structType, struct));
    }

    private static PValues stringValues(List<String> hexes) {
        PValues.Builder builder = PValues.newBuilder()
                .setType(PGenericType.newBuilder().setId(TypeId.STRING).build());
        hexes.forEach(hex -> builder.addStringValueBytes(ByteString.copyFrom(HexFormat.of().parseHex(hex))));
        return builder.build();
    }

    // The UTF-8 bytes, in hex, of the string literals in the results, in the order the results hold them.
    private static List<String> stringLeaves(List<? extends Literal> literals) {
        List<String> leaves = new ArrayList<>();
        for (Literal literal : literals) {
            if (literal instanceof StringLiteral) {
                leaves.add(HexFormat.of().withUpperCase()
                        .formatHex(((StringLiteral) literal).getValue().getBytes(StandardCharsets.UTF_8)));
            } else if (literal instanceof ArrayLiteral) {
                leaves.addAll(stringLeaves(((ArrayLiteral) literal).getValue()));
            } else if (literal instanceof StructLiteral) {
                leaves.addAll(stringLeaves(((StructLiteral) literal).getValue()));
            } else if (literal instanceof MapLiteral) {
                ((MapLiteral) literal).getValue().forEach((key, value) ->
                        leaves.addAll(stringLeaves(ImmutableList.of(key, value))));
            }
        }
        return leaves;
    }
}
