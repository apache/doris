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

import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.proto.Types.PGenericType;
import org.apache.doris.proto.Types.PGenericType.TypeId;
import org.apache.doris.proto.Types.PValues;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
    public void testGetResultExpressionArrayInt() {
        int num = 10;
        Integer[] elementsArray = new Integer[num];
        for (int i = 0; i < elementsArray.length; ++i) {
            elementsArray[i] = i;
        }
        DataType arrayType = ArrayType.of(IntegerType.INSTANCE, true);
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
        DataType nestedArrayType = ArrayType.of(IntegerType.INSTANCE, true);
        DataType outArrayType = ArrayType.of(nestedArrayType, true);
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
        DataType nestedArrayType = ArrayType.of(IntegerType.INSTANCE, true);
        DataType outArrayType = ArrayType.of(nestedArrayType, true);
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
        DataType arrayType = ArrayType.of(IntegerType.INSTANCE, true);
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
}
