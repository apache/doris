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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lcm;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapEntries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapFromEntries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RoundBankers;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToJson;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.MoreFieldsThread;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class DerivedFunctionSignatureTest {

    @Test
    void testStructRefreshesFieldNullabilityWithReusedBinding() {
        SlotReference required = new SlotReference("value", IntegerType.INSTANCE, false);
        CreateStruct original = new CreateStruct(required);
        ToJson originalJson = new ToJson(original);
        FunctionSignature originalSignature = original.getSignature();
        originalJson.getSignature();
        Assertions.assertFalse(fieldType(original).getFields().get(0).isNullable());

        SlotReference nullable = required.withNullable(true);
        CreateStruct rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(nullable)));
        ToJson rewrittenJson = MoreFieldsThread.keepFunctionSignature(
                () -> originalJson.withChildren(List.of(rewritten)));

        Assertions.assertTrue(rewritten.child(0).nullable());
        Assertions.assertTrue(fieldType(rewritten).getFields().get(0).isNullable());
        Assertions.assertEquals(originalSignature.argumentsTypes, rewritten.getSignature().argumentsTypes);
        Assertions.assertEquals(rewritten.getDataType(), rewrittenJson.getSignature().getArgType(0));
    }

    @Test
    void testNamedAndNestedStructRefreshDerivedFields() {
        SlotReference required = new SlotReference("value", IntegerType.INSTANCE, false);
        CreateNamedStruct inner = new CreateNamedStruct(new StringLiteral("inner"), required);
        CreateNamedStruct outer = new CreateNamedStruct(new StringLiteral("outer"), inner);
        inner.getSignature();
        outer.getSignature();

        SlotReference nullable = required.withNullable(true);
        CreateNamedStruct rewrittenInner = MoreFieldsThread.keepFunctionSignature(
                () -> inner.withChildren(List.of(new StringLiteral("inner"), nullable)));
        CreateNamedStruct rewrittenOuter = MoreFieldsThread.keepFunctionSignature(
                () -> outer.withChildren(List.of(new StringLiteral("outer"), rewrittenInner)));

        StructType innerType = fieldType(rewrittenInner);
        Assertions.assertEquals("inner", innerType.getFields().get(0).getName());
        Assertions.assertTrue(innerType.getFields().get(0).isNullable());
        Assertions.assertEquals(nullable.getDataType(), rewrittenInner.getSignature().getArgType(1));

        StructType outerType = fieldType(rewrittenOuter);
        Assertions.assertEquals("outer", outerType.getFields().get(0).getName());
        Assertions.assertFalse(outerType.getFields().get(0).isNullable());
        StructType nestedType = (StructType) outerType.getFields().get(0).getDataType();
        Assertions.assertTrue(nestedType.getFields().get(0).isNullable());
        Assertions.assertEquals(rewrittenInner.getDataType(), rewrittenOuter.getSignature().getArgType(1));
    }

    @Test
    void testNestedStructNullabilityPropagatesThroughMapEntries() {
        SlotReference nullable = new SlotReference("value", IntegerType.INSTANCE, true);
        CreateStruct originalStruct = new CreateStruct(nullable);
        CreateMap originalMap = new CreateMap(new IntegerLiteral(1), originalStruct);
        MapEntries originalEntries = new MapEntries(originalMap);
        MapFromEntries originalRoundTrip = new MapFromEntries(originalEntries);
        originalRoundTrip.getSignature();

        SlotReference required = nullable.withNullable(false);
        CreateStruct rewrittenStruct = MoreFieldsThread.keepFunctionSignature(
                () -> originalStruct.withChildren(List.of(required)));
        CreateMap rewrittenMap = MoreFieldsThread.keepFunctionSignature(
                () -> originalMap.withChildren(List.of(new IntegerLiteral(1), rewrittenStruct)));
        MapEntries rewrittenEntries = MoreFieldsThread.keepFunctionSignature(
                () -> originalEntries.withChildren(List.of(rewrittenMap)));
        MapFromEntries rewrittenRoundTrip = MoreFieldsThread.keepFunctionSignature(
                () -> originalRoundTrip.withChildren(List.of(rewrittenEntries)));

        StructType mapValueType = (StructType) ((MapType) rewrittenMap.getDataType()).getValueType();
        Assertions.assertFalse(mapValueType.getFields().get(0).isNullable());
        Assertions.assertEquals(rewrittenMap.getDataType(), rewrittenMap.getSignature().returnType);
        Assertions.assertEquals(rewrittenStruct.getDataType(), rewrittenMap.getSignature().getArgType(1));

        StructType entryType = (StructType) ((ArrayType) rewrittenEntries.getDataType()).getItemType();
        StructType entryValueType = (StructType) entryType.getFields().get(1).getDataType();
        Assertions.assertFalse(entryValueType.getFields().get(0).isNullable());
        Assertions.assertEquals(rewrittenMap.getDataType(), rewrittenEntries.getSignature().getArgType(0));
        Assertions.assertEquals(rewrittenEntries.getDataType(), rewrittenEntries.getSignature().returnType);
        Assertions.assertEquals(rewrittenEntries.getDataType(), rewrittenRoundTrip.getSignature().getArgType(0));
        Assertions.assertEquals(rewrittenMap.getDataType(), rewrittenRoundTrip.getSignature().returnType);
    }

    @Test
    void testChangedArityRefreshesChildDerivedSignature() {
        SlotReference first = new SlotReference("first", IntegerType.INSTANCE, false);
        CreateStruct original = new CreateStruct(first);
        original.getSignature();

        SlotReference second = new SlotReference("second", IntegerType.INSTANCE, false);
        CreateStruct rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(first, second)));

        Assertions.assertEquals(2, rewritten.getSignature().argumentsTypes.size());
        Assertions.assertEquals(2, fieldType(rewritten).getFields().size());
    }

    @Test
    void testSameTypeSlotReplacementReusesSignature() {
        SlotReference originalSlot = new SlotReference("original", IntegerType.INSTANCE, false);
        Abs original = new Abs(originalSlot);
        FunctionSignature originalSignature = original.getSignature();

        SlotReference replacement = new SlotReference("replacement", IntegerType.INSTANCE, false);
        Abs rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(replacement)));

        Assertions.assertSame(originalSignature, rewritten.getSignature());
    }

    @Test
    void testUnchangedDerivedMetadataReusesSignature() {
        SlotReference originalSlot = new SlotReference("original", IntegerType.INSTANCE, false);
        CreateStruct original = new CreateStruct(originalSlot);
        FunctionSignature originalSignature = original.getSignature();

        SlotReference replacement = new SlotReference("replacement", IntegerType.INSTANCE, false);
        CreateStruct rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(replacement)));

        Assertions.assertSame(originalSignature, rewritten.getSignature());
    }

    @Test
    void testOrdinaryPrecisionSignatureRemainsIdentical() {
        SlotReference decimal = new SlotReference(
                "decimal_value", DecimalV3Type.createDecimalV3Type(32, 6), false);
        SlotReference scale = new SlotReference("scale", IntegerType.INSTANCE, false);
        RoundBankers original = new RoundBankers(decimal, scale);
        FunctionSignature originalSignature = original.getSignature();
        Assertions.assertEquals(6, ((DecimalV3Type) originalSignature.returnType).getScale());

        RoundBankers rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(decimal, new IntegerLiteral(2))));

        Assertions.assertSame(originalSignature, rewritten.getSignature());
        Assertions.assertEquals(6,
                ((DecimalV3Type) rewritten.getSignature().returnType).getScale());
    }

    @Test
    void testPromotedSignatureIsNotComputedTwice() {
        Lcm original = new Lcm(new TinyIntLiteral((byte) 2), new TinyIntLiteral((byte) 4));
        FunctionSignature promotedSignature = original.getSignature();
        Assertions.assertEquals(SmallIntType.INSTANCE, promotedSignature.getArgType(0));

        Lcm rewritten = MoreFieldsThread.keepFunctionSignature(() -> original.withChildren(
                List.of(new SmallIntLiteral((short) 2), new SmallIntLiteral((short) 4))));
        Lcm rewrittenAgain = rewritten.withChildren(
                List.of(new SmallIntLiteral((short) 2), new SmallIntLiteral((short) 4)));

        Assertions.assertSame(promotedSignature, rewritten.getSignature());
        Assertions.assertSame(promotedSignature, rewrittenAgain.getSignature());
        Assertions.assertEquals(SmallIntType.INSTANCE, rewritten.getSignature().getArgType(0));
    }

    private StructType fieldType(BoundFunction function) {
        return (StructType) function.getSignature().returnType;
    }
}
