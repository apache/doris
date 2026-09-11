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
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFlatten;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayZip;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lcm;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapEntries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapFromArrays;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapFromEntries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapFromEntriesUnique;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapFromFilteredEntriesUnique;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapKeys;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapValues;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RoundBankers;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToJson;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;
import org.apache.doris.nereids.util.MoreFieldsThread;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

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
    void testMapFromEntriesInternalSubclassesSurviveSignatureRefresh() {
        StructType nullableValue = new StructType(List.of(
                new StructField("payload", IntegerType.INSTANCE, true, "")));
        StructType originalEntry = new StructType(List.of(
                new StructField("key", IntegerType.INSTANCE, false, ""),
                new StructField("value", nullableValue, true, "")));
        SlotReference originalEntries = new SlotReference(
                "entries", ArrayType.of(originalEntry), false);

        MapFromEntriesUnique unique = new MapFromEntriesUnique(originalEntries);
        MapFromFilteredEntriesUnique filtered = new MapFromFilteredEntriesUnique(originalEntries);
        FunctionSignature uniqueSignature = unique.getSignature();
        FunctionSignature filteredSignature = filtered.getSignature();

        StructType requiredValue = new StructType(List.of(
                new StructField("payload", IntegerType.INSTANCE, false, "")));
        StructType currentEntry = new StructType(List.of(
                new StructField("key", IntegerType.INSTANCE, false, ""),
                new StructField("value", requiredValue, true, "")));
        SlotReference currentEntries = new SlotReference(
                "entries", ArrayType.of(currentEntry), false);

        MapFromEntriesUnique rewrittenUnique = MoreFieldsThread.keepFunctionSignature(
                () -> unique.withChildren(List.of(currentEntries)));
        MapFromFilteredEntriesUnique rewrittenFiltered = MoreFieldsThread.keepFunctionSignature(
                () -> filtered.withChildren(List.of(currentEntries)));

        Assertions.assertEquals(MapFromEntriesUnique.class, rewrittenUnique.getClass());
        Assertions.assertEquals("%map_from_entries_unique%", rewrittenUnique.getName());
        Assertions.assertNotSame(uniqueSignature, rewrittenUnique.getSignature());
        assertRequiredNestedMapValue(rewrittenUnique.getSignature());

        Assertions.assertEquals(MapFromFilteredEntriesUnique.class, rewrittenFiltered.getClass());
        Assertions.assertEquals("%map_from_filtered_entries_unique%", rewrittenFiltered.getName());
        Assertions.assertNotSame(filteredSignature, rewrittenFiltered.getSignature());
        assertRequiredNestedMapValue(rewrittenFiltered.getSignature());
    }

    @Test
    void testContainerSignatureChainTracksRewrittenStructMetadata() {
        SlotReference nullable = new SlotReference("value", IntegerType.INSTANCE, true);
        CreateStruct originalStruct = new CreateStruct(nullable);
        Array originalValues = new Array(originalStruct);
        Array originalKeys = new Array(new IntegerLiteral(1));
        MapFromArrays originalMap = new MapFromArrays(originalKeys, originalValues);
        MapValues originalMapValues = new MapValues(originalMap);
        ElementAt originalValue = new ElementAt(originalMapValues, new IntegerLiteral(1));
        ElementAt originalField = new ElementAt(originalValue, new IntegerLiteral(1));
        originalField.getSignature();

        SlotReference required = nullable.withNullable(false);
        CreateStruct rewrittenStruct = MoreFieldsThread.keepFunctionSignature(
                () -> originalStruct.withChildren(List.of(required)));
        Array rewrittenValues = MoreFieldsThread.keepFunctionSignature(
                () -> originalValues.withChildren(List.of(rewrittenStruct)));
        MapFromArrays rewrittenMap = MoreFieldsThread.keepFunctionSignature(
                () -> originalMap.withChildren(List.of(originalKeys, rewrittenValues)));
        MapValues rewrittenMapValues = MoreFieldsThread.keepFunctionSignature(
                () -> originalMapValues.withChildren(List.of(rewrittenMap)));
        ElementAt rewrittenValue = MoreFieldsThread.keepFunctionSignature(
                () -> originalValue.withChildren(List.of(rewrittenMapValues, new BigIntLiteral(1))));
        ElementAt rewrittenField = MoreFieldsThread.keepFunctionSignature(
                () -> originalField.withChildren(List.of(rewrittenValue, new IntegerLiteral(1))));

        StructType arrayItemType = (StructType) ((ArrayType) rewrittenValues.getDataType()).getItemType();
        Assertions.assertEquals(rewrittenStruct.getDataType(), arrayItemType);
        Assertions.assertFalse(arrayItemType.getFields().get(0).isNullable());
        Assertions.assertEquals(rewrittenStruct.getDataType(), rewrittenValues.getSignature().getArgType(0));

        MapType mapType = (MapType) rewrittenMap.getDataType();
        Assertions.assertEquals(rewrittenValues.getDataType(), rewrittenMap.getSignature().getArgType(1));
        Assertions.assertEquals(arrayItemType, mapType.getValueType());

        Assertions.assertEquals(rewrittenMap.getDataType(), rewrittenMapValues.getSignature().getArgType(0));
        Assertions.assertEquals(arrayItemType, ((ArrayType) rewrittenMapValues.getDataType()).getItemType());
        Assertions.assertEquals(rewrittenMapValues.getDataType(), rewrittenValue.getSignature().getArgType(0));
        Assertions.assertEquals(arrayItemType, rewrittenValue.getDataType());
        Assertions.assertEquals(rewrittenValue.getDataType(), rewrittenField.getSignature().getArgType(0));
        Assertions.assertEquals(IntegerType.INSTANCE, rewrittenField.getDataType());
    }

    @Test
    void testContainerAccessPreservesIndependentNestedPrecision() {
        StructType valueType = (StructType) new CreateStruct(
                new SlotReference("event_time", DateTimeV2Type.of(6), false)).getDataType();
        MapType mapType = MapType.of(DateTimeV2Type.of(0), valueType);
        SlotReference map = new SlotReference("map_value", mapType, false);

        MapValues mapValues = new MapValues(map);
        ElementAt value = new ElementAt(mapValues, new IntegerLiteral(1));
        ElementAt field = new ElementAt(value, new IntegerLiteral(1));

        Assertions.assertEquals(mapType, mapValues.getSignature().getArgType(0));
        Assertions.assertEquals(ArrayType.of(valueType), mapValues.getSignature().returnType);
        Assertions.assertEquals(valueType, value.getSignature().returnType);
        Assertions.assertEquals(DateTimeV2Type.of(6), field.getSignature().returnType);

        ElementAt directMapValue = new ElementAt(
                map, new SlotReference("map_key", DateTimeV2Type.of(0), false));
        Assertions.assertEquals(valueType, directMapValue.getSignature().returnType);

        StructType keyType = (StructType) new CreateStruct(
                new SlotReference("key_time", DateTimeV2Type.of(6), false)).getDataType();
        MapKeys mapKeys = new MapKeys(new SlotReference(
                "reverse_map", MapType.of(keyType, DateTimeV2Type.of(0)), false));
        Assertions.assertEquals(ArrayType.of(keyType), mapKeys.getSignature().returnType);

        ArrayType structArrayType = ArrayType.of(valueType);
        ArrayType timeArrayType = ArrayType.of(DateTimeV2Type.of(0));
        ArrayZip arrayZip = new ArrayZip(
                new SlotReference("struct_array", structArrayType, false),
                new SlotReference("time_array", timeArrayType, false));
        StructType zippedType = (StructType) ((ArrayType) arrayZip.getSignature().returnType).getItemType();
        Assertions.assertEquals(valueType, zippedType.getFields().get(0).getDataType());
        Assertions.assertEquals(DateTimeV2Type.of(0), zippedType.getFields().get(1).getDataType());

        ArrayType nestedMapArrayType = ArrayType.of(ArrayType.of(mapType));
        ArrayFlatten arrayFlatten = new ArrayFlatten(
                new SlotReference("nested_map_array", nestedMapArrayType, false));
        Assertions.assertEquals(ArrayType.of(mapType), arrayFlatten.getSignature().returnType);

        Array mapArray = new Array(new SlotReference("map_item", mapType, false));
        Assertions.assertEquals(ArrayType.of(mapType), mapArray.getSignature().returnType);

        CreateMap createMap = new CreateMap(
                new SlotReference("create_key", DateTimeV2Type.of(0), false),
                new SlotReference("create_value", valueType, false));
        Assertions.assertEquals(mapType, createMap.getSignature().returnType);

        MapEntries mapEntries = new MapEntries(map);
        StructType entryType = (StructType) ((ArrayType) mapEntries.getSignature().returnType).getItemType();
        Assertions.assertEquals(DateTimeV2Type.of(0), entryType.getFields().get(0).getDataType());
        Assertions.assertEquals(valueType, entryType.getFields().get(1).getDataType());
        MapFromEntries mapFromEntries = new MapFromEntries(mapEntries);
        Assertions.assertEquals(mapType, mapFromEntries.getSignature().returnType);

        CreateStruct createStruct = new CreateStruct(
                new SlotReference("scale_zero", DateTimeV2Type.of(0), false),
                new SlotReference("scale_six", DateTimeV2Type.of(6), false));
        StructType structType = (StructType) createStruct.getSignature().returnType;
        Assertions.assertEquals(DateTimeV2Type.of(0), structType.getFields().get(0).getDataType());
        Assertions.assertEquals(DateTimeV2Type.of(6), structType.getFields().get(1).getDataType());

        CreateNamedStruct createNamedStruct = new CreateNamedStruct(
                new StringLiteral("scale_zero"),
                new SlotReference("named_scale_zero", DateTimeV2Type.of(0), false),
                new StringLiteral("scale_six"),
                new SlotReference("named_scale_six", DateTimeV2Type.of(6), false));
        StructType namedStructType = (StructType) createNamedStruct.getSignature().returnType;
        Assertions.assertEquals(DateTimeV2Type.of(0), namedStructType.getFields().get(0).getDataType());
        Assertions.assertEquals(DateTimeV2Type.of(6), namedStructType.getFields().get(1).getDataType());
    }

    @Test
    void testStructElementSelectorCanonicalizationRefreshesDynamicShape() {
        StructType structType = new StructType(List.of(
                new StructField("first", IntegerType.INSTANCE, false, ""),
                new StructField("second", DateTimeV2Type.of(6), true, "")));
        SlotReference struct = new SlotReference("document", structType, false);
        ElementAt ordinal = new ElementAt(struct, new IntegerLiteral(1));
        ordinal.getSignature();

        StringLiteral canonicalName = new StringLiteral("FIRST");
        ElementAt canonical = MoreFieldsThread.keepFunctionSignature(
                () -> ordinal.withChildren(List.of(struct, canonicalName)));
        Assertions.assertEquals(canonicalName.getDataType(), canonical.getSignature().getArgType(1));
        Assertions.assertEquals(IntegerType.INSTANCE, canonical.getSignature().returnType);

        ElementAt differentField = MoreFieldsThread.keepFunctionSignature(
                () -> ordinal.withChildren(List.of(struct, new StringLiteral("second"))));
        Assertions.assertThrows(AnalysisException.class, differentField::getSignature);

        ElementAt nonLiteral = MoreFieldsThread.keepFunctionSignature(
                () -> ordinal.withChildren(List.of(
                        struct, new SlotReference("selector", IntegerType.INSTANCE, false))));
        Assertions.assertThrows(AnalysisException.class, nonLiteral::getSignature);
        ElementAt missingField = MoreFieldsThread.keepFunctionSignature(
                () -> ordinal.withChildren(List.of(struct, new StringLiteral("missing"))));
        Assertions.assertThrows(AnalysisException.class, missingField::getSignature);

        ElementAt arrayElement = new ElementAt(
                new SlotReference("items", ArrayType.of(IntegerType.INSTANCE), false),
                new IntegerLiteral(1));
        arrayElement.getSignature();
        ElementAt incompatibleArraySelector = MoreFieldsThread.keepFunctionSignature(
                () -> arrayElement.withChildren(List.of(
                        arrayElement.getArgument(0), new StringLiteral("1"))));
        Assertions.assertThrows(AnalysisException.class, incompatibleArraySelector::getSignature);

        ElementAt mapElement = new ElementAt(
                new SlotReference("entries", MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), false),
                new IntegerLiteral(1));
        mapElement.getSignature();
        ElementAt incompatibleMapSelector = MoreFieldsThread.keepFunctionSignature(
                () -> mapElement.withChildren(List.of(
                        mapElement.getArgument(0), new StringLiteral("1"))));
        Assertions.assertThrows(AnalysisException.class, incompatibleMapSelector::getSignature);
    }

    @Test
    void testReusedScalarLeafAcceptsResolvedOrUnchangedOriginRawTypeOnly() {
        SlotReference stringMap = new SlotReference(
                "entries", MapType.of(StringType.INSTANCE, IntegerType.INSTANCE), false);
        VarcharLiteral rawKey = new VarcharLiteral("a");
        ElementAt original = new ElementAt(stringMap, rawKey);
        FunctionSignature resolved = original.getSignature();
        Assertions.assertEquals(StringType.INSTANCE, resolved.getArgType(1));

        ElementAt unchangedRaw = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(stringMap, new VarcharLiteral("b"))));
        Assertions.assertSame(resolved, unchangedRaw.getSignature());
        Assertions.assertEquals(StringType.INSTANCE, unchangedRaw.getSignature().getArgType(1));

        ElementAt thirdGeneration = MoreFieldsThread.keepFunctionSignature(
                () -> unchangedRaw.withChildren(List.of(stringMap, new VarcharLiteral("c"))));
        Assertions.assertSame(resolved, thirdGeneration.getSignature());

        ElementAt alreadyCoerced = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(
                        stringMap, new SlotReference("key", StringType.INSTANCE, false))));
        Assertions.assertSame(resolved, alreadyCoerced.getSignature());

        ElementAt changedRawType = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(stringMap, new VarcharLiteral("longer"))));
        Assertions.assertThrows(AnalysisException.class, changedRawType::getSignature);

        ElementAt unforced = original.withChildren(List.of(stringMap, new VarcharLiteral("longer")));
        FunctionSignature unforcedSignature = unforced.getSignature();
        ElementAt forcedAfterFreshBinding = MoreFieldsThread.keepFunctionSignature(
                () -> unforced.withChildren(List.of(stringMap, new VarcharLiteral("second"))));
        Assertions.assertSame(unforcedSignature, forcedAfterFreshBinding.getSignature());
    }

    @Test
    void testSignatureReuseContextRejectsUnboundOriginOrCurrentTree() {
        SlotReference stringMap = new SlotReference(
                "entries", MapType.of(StringType.INSTANCE, IntegerType.INSTANCE), false);
        ElementAt boundOrigin = new ElementAt(stringMap, new VarcharLiteral("a"));
        FunctionParams.SignatureReuseContext currentUnbound = MoreFieldsThread.keepFunctionSignature(
                () -> new FunctionParams(boundOrigin, boundOrigin.getName(),
                        List.of(stringMap, new UnboundSlot("key")), false).getSignatureReuseContext());
        Assertions.assertNull(currentUnbound);

        CreateStruct unboundOrigin = new CreateStruct(new UnboundSlot("value"));
        FunctionParams.SignatureReuseContext originUnbound = MoreFieldsThread.keepFunctionSignature(
                () -> new FunctionParams(unboundOrigin, unboundOrigin.getName(),
                        List.of(new IntegerLiteral(1)), false).getSignatureReuseContext());
        Assertions.assertNull(originUnbound);
    }

    @Test
    void testReusedDerivedSignaturesRejectChangedScalarLeaves() {
        SlotReference scaleZero = new SlotReference("scale_zero", DateTimeV2Type.of(0), false);
        SlotReference scaleSix = new SlotReference("scale_six", DateTimeV2Type.of(6), false);

        CreateStruct originalStruct = new CreateStruct(scaleZero);
        originalStruct.getSignature();
        CreateStruct rewrittenStruct = MoreFieldsThread.keepFunctionSignature(
                () -> originalStruct.withChildren(List.of(scaleSix)));
        Assertions.assertThrows(AnalysisException.class, rewrittenStruct::getSignature);

        CreateNamedStruct originalNamed = new CreateNamedStruct(new StringLiteral("event_time"), scaleZero);
        originalNamed.getSignature();
        CreateNamedStruct rewrittenNamed = MoreFieldsThread.keepFunctionSignature(
                () -> originalNamed.withChildren(List.of(new StringLiteral("event_time"), scaleSix)));
        Assertions.assertThrows(AnalysisException.class, rewrittenNamed::getSignature);

        MapType originalMapType = MapType.of(DateTimeV2Type.of(0), DateTimeV2Type.of(6));
        MapType currentMapType = MapType.of(DateTimeV2Type.of(6), DateTimeV2Type.of(0));
        MapEntries originalEntries = new MapEntries(new SlotReference("map_value", originalMapType, false));
        originalEntries.getSignature();
        MapEntries rewrittenEntries = MoreFieldsThread.keepFunctionSignature(
                () -> originalEntries.withChildren(List.of(
                        new SlotReference("map_value", currentMapType, false))));
        Assertions.assertThrows(AnalysisException.class, rewrittenEntries::getSignature);

        StructType originalEntryType = new StructType(List.of(
                new StructField("key", DateTimeV2Type.of(0), false, ""),
                new StructField("value", DateTimeV2Type.of(6), true, "")));
        StructType currentEntryType = new StructType(List.of(
                new StructField("key", DateTimeV2Type.of(6), true, ""),
                new StructField("value", DateTimeV2Type.of(0), false, "")));
        MapFromEntries originalFromEntries = new MapFromEntries(new SlotReference(
                "entries", ArrayType.of(originalEntryType), false));
        originalFromEntries.getSignature();
        MapFromEntries rewrittenFromEntries = MoreFieldsThread.keepFunctionSignature(
                () -> originalFromEntries.withChildren(List.of(new SlotReference(
                        "entries", ArrayType.of(currentEntryType), false))));
        Assertions.assertThrows(AnalysisException.class, rewrittenFromEntries::getSignature);

        CreateMap originalMap = new CreateMap(scaleZero, scaleSix);
        originalMap.getSignature();
        CreateMap rewrittenMap = MoreFieldsThread.keepFunctionSignature(
                () -> originalMap.withChildren(List.of(scaleSix, scaleZero)));
        Assertions.assertThrows(AnalysisException.class, rewrittenMap::getSignature);
    }

    @Test
    void testToJsonRefreshesNestedNullabilityWithoutReplacingPrecision() {
        StructType originalInner = new StructType(List.of(
                new StructField("event_time", DateTimeV2Type.of(0), false, "")));
        StructType originalOuter = new StructType(List.of(
                new StructField("payload", originalInner, false, "")));
        ToJson original = new ToJson(new SlotReference("document", originalOuter, false));
        FunctionSignature originalSignature = original.getSignature();

        ToJson unchanged = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(
                        new SlotReference("document", originalOuter, false))));
        Assertions.assertSame(originalSignature, unchanged.getSignature());

        StructType currentInner = new StructType(List.of(
                new StructField("event_time", DateTimeV2Type.of(0), true, "")));
        StructType currentOuter = new StructType(List.of(
                new StructField("payload", currentInner, true, "")));
        ToJson rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(new SlotReference("document", currentOuter, false))));

        StructType refreshedOuter = (StructType) rewritten.getSignature().getArgType(0);
        Assertions.assertTrue(refreshedOuter.getFields().get(0).isNullable());
        StructType refreshedInner = (StructType) refreshedOuter.getFields().get(0).getDataType();
        Assertions.assertTrue(refreshedInner.getFields().get(0).isNullable());
        Assertions.assertEquals(DateTimeV2Type.of(0),
                refreshedInner.getFields().get(0).getDataType());
        Assertions.assertSame(originalSignature.returnType, rewritten.getSignature().returnType);

        StructType changedPrecisionInner = new StructType(List.of(
                new StructField("event_time", DateTimeV2Type.of(6), true, "")));
        StructType changedPrecisionOuter = new StructType(List.of(
                new StructField("payload", changedPrecisionInner, true, "")));
        ToJson incompatible = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(
                        new SlotReference("document", changedPrecisionOuter, false))));
        Assertions.assertThrows(AnalysisException.class, incompatible::getSignature);
    }

    @Test
    void testToJsonStillComputesScalarPrecision() {
        ToJson datetime = new ToJson(
                new SlotReference("event_time", DateTimeV2Type.of(6), false));
        Assertions.assertEquals(DateTimeV2Type.of(6), datetime.getSignature().getArgType(0));
        assertConcreteSignature(datetime.getSignature());

        DecimalV3Type decimalType = DecimalV3Type.createDecimalV3Type(30, 8);
        ToJson decimal = new ToJson(new SlotReference("amount", decimalType, false));
        Assertions.assertEquals(decimalType, decimal.getSignature().getArgType(0));
        assertConcreteSignature(decimal.getSignature());
    }

    @Test
    void testArrayStillInfersCommonElementPrecision() {
        Array mixedPrecision = new Array(
                new SlotReference("scale_zero", DateTimeV2Type.of(0), false),
                new SlotReference("scale_six", DateTimeV2Type.of(6), false));
        FunctionSignature mixedTimeSignature = mixedPrecision.getSignature();
        Assertions.assertTrue(mixedTimeSignature.hasVarArgs);
        Assertions.assertEquals(1, mixedTimeSignature.argumentsTypes.size());
        Assertions.assertEquals(DateTimeV2Type.of(6), mixedTimeSignature.getArgType(0));
        Assertions.assertEquals(ArrayType.of(mixedTimeSignature.getArgType(0)), mixedTimeSignature.returnType);
        assertConcreteSignature(mixedTimeSignature);

        Array singlePrecision = new Array(
                new SlotReference("scale_zero", DateTimeV2Type.of(0), false));
        Assertions.assertEquals(
                ArrayType.of(DateTimeV2Type.of(0)), singlePrecision.getSignature().returnType);

        Array mixedDecimal = new Array(
                new SlotReference("decimal_small",
                        DecimalV3Type.createDecimalV3Type(12, 2), false),
                new SlotReference("decimal_large",
                        DecimalV3Type.createDecimalV3Type(20, 6), false));
        FunctionSignature mixedDecimalSignature = mixedDecimal.getSignature();
        Assertions.assertTrue(mixedDecimalSignature.getArgType(0) instanceof DecimalV3Type);
        Assertions.assertEquals(ArrayType.of(mixedDecimalSignature.getArgType(0)),
                mixedDecimalSignature.returnType);
        assertConcreteSignature(mixedDecimalSignature);

        StructType nestedType = new StructType(List.of(
                new StructField("event_time", DateTimeV2Type.of(6), true, ""),
                new StructField("amount", DecimalV3Type.createDecimalV3Type(20, 6), true, "")));
        Array nested = new Array(new SlotReference("nested", nestedType, false));
        assertConcreteSignature(nested.getSignature());
    }

    @Test
    void testArrayRejectsReuseAcrossEmptyAndVarargShapes() {
        Array empty = new Array();
        FunctionSignature emptySignature = empty.getSignature();
        Assertions.assertFalse(emptySignature.hasVarArgs);
        Assertions.assertTrue(emptySignature.argumentsTypes.isEmpty());

        Array emptyToNonEmpty = MoreFieldsThread.keepFunctionSignature(
                () -> empty.withChildren(List.of(new IntegerLiteral(1))));
        Assertions.assertThrows(AnalysisException.class, emptyToNonEmpty::getSignature);

        Array nonEmpty = new Array(new IntegerLiteral(1));
        FunctionSignature nonEmptySignature = nonEmpty.getSignature();
        Assertions.assertTrue(nonEmptySignature.hasVarArgs);
        Assertions.assertEquals(1, nonEmptySignature.argumentsTypes.size());
        Array nonEmptyToEmpty = MoreFieldsThread.keepFunctionSignature(
                () -> nonEmpty.withChildren(List.of()));
        Assertions.assertThrows(AnalysisException.class, nonEmptyToEmpty::getSignature);
    }

    @Test
    void testArrayRefreshCombinesMetadataFromEveryVararg() {
        SlotReference firstRequired = new SlotReference("first", IntegerType.INSTANCE, false);
        SlotReference secondRequired = new SlotReference("second", IntegerType.INSTANCE, false);
        CreateStruct first = new CreateStruct(firstRequired);
        CreateStruct second = new CreateStruct(secondRequired);
        Array allRequired = new Array(first, second);
        allRequired.getSignature();

        CreateStruct nullableSecond = MoreFieldsThread.keepFunctionSignature(
                () -> second.withChildren(List.of(secondRequired.withNullable(true))));
        Array widened = MoreFieldsThread.keepFunctionSignature(
                () -> allRequired.withChildren(List.of(first, nullableSecond)));
        StructType widenedItem = (StructType) ((ArrayType) widened.getDataType()).getItemType();
        Assertions.assertTrue(widenedItem.getFields().get(0).isNullable());

        CreateStruct originallyNullableSecond = new CreateStruct(secondRequired.withNullable(true));
        Array originallyNullable = new Array(first, originallyNullableSecond);
        originallyNullable.getSignature();
        CreateStruct rewrittenRequiredSecond = MoreFieldsThread.keepFunctionSignature(
                () -> originallyNullableSecond.withChildren(List.of(secondRequired)));
        Array narrowed = MoreFieldsThread.keepFunctionSignature(
                () -> originallyNullable.withChildren(List.of(first, rewrittenRequiredSecond)));
        StructType narrowedItem = (StructType) ((ArrayType) narrowed.getDataType()).getItemType();
        Assertions.assertFalse(narrowedItem.getFields().get(0).isNullable());
    }

    @Test
    void testArrayRefreshUsesCurrentCommonShapeAndRejectsIncompatibleArity() {
        StructType originalType = new StructType(List.of(
                new StructField("old_name", IntegerType.INSTANCE, false, "")));
        SlotReference originalFirst = new SlotReference("first", originalType, false);
        SlotReference originalSecond = new SlotReference("second", originalType, false);
        Array original = new Array(originalFirst, originalSecond);
        original.getSignature();

        StructType currentFirstType = new StructType(List.of(
                new StructField("current_first", IntegerType.INSTANCE, false, "")));
        StructType currentSecondType = new StructType(List.of(
                new StructField("current_second", IntegerType.INSTANCE, true, "")));
        SlotReference currentFirst = new SlotReference("first", currentFirstType, false);
        SlotReference currentSecond = new SlotReference("second", currentSecondType, false);
        Array compatible = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(currentFirst, currentSecond)));
        StructType commonItem = (StructType) ((ArrayType) compatible.getDataType()).getItemType();
        Assertions.assertEquals(1, commonItem.getFields().size());
        Assertions.assertEquals("current_first", commonItem.getFields().get(0).getName());
        Assertions.assertTrue(commonItem.getFields().get(0).isNullable());

        StructType incompatibleSecondType = new StructType(List.of(
                new StructField("current_second", IntegerType.INSTANCE, false, ""),
                new StructField("extra_second", IntegerType.INSTANCE, false, "")));
        SlotReference incompatibleSecond = new SlotReference("second", incompatibleSecondType, false);
        Array incompatible = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(currentFirst, incompatibleSecond)));
        Assertions.assertThrows(AnalysisException.class, incompatible::getSignature);

        SlotReference extendedFirst = new SlotReference("first", incompatibleSecondType, false);
        Array consistentlyExtended = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(extendedFirst, incompatibleSecond)));
        Assertions.assertThrows(AnalysisException.class, consistentlyExtended::getSignature);

        SlotReference wrongKind = new SlotReference(
                "second", ArrayType.of(IntegerType.INSTANCE), false);
        Array incompatibleKind = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(currentFirst, wrongKind)));
        Assertions.assertThrows(AnalysisException.class, incompatibleKind::getSignature);
    }

    @Test
    void testArrayRefreshRequiresTypedNullAndPreservesNullBinding() {
        StructType originalType = new StructType(List.of(
                new StructField("old_name", IntegerType.INSTANCE, false, "")));
        SlotReference originalStruct = new SlotReference("struct_value", originalType, false);
        Array structThenTypedNull = new Array(originalStruct, new NullLiteral(originalType));
        structThenTypedNull.getSignature();

        StructType currentType = new StructType(List.of(
                new StructField("current_name", IntegerType.INSTANCE, true, "")));
        SlotReference currentStruct = new SlotReference("struct_value", currentType, false);
        Array refreshed = MoreFieldsThread.keepFunctionSignature(
                () -> structThenTypedNull.withChildren(List.of(
                        currentStruct, new NullLiteral(originalType))));
        StructType refreshedItem = (StructType) ((ArrayType) refreshed.getDataType()).getItemType();
        Assertions.assertEquals("current_name", refreshedItem.getFields().get(0).getName());
        Assertions.assertTrue(refreshedItem.getFields().get(0).isNullable());

        Array bareNull = MoreFieldsThread.keepFunctionSignature(
                () -> structThenTypedNull.withChildren(List.of(currentStruct, NullLiteral.INSTANCE)));
        Assertions.assertThrows(AnalysisException.class, bareNull::getSignature);

        Array allNull = new Array(NullLiteral.INSTANCE, NullLiteral.INSTANCE);
        FunctionSignature allNullSignature = allNull.getSignature();
        Array rewrittenAllNull = MoreFieldsThread.keepFunctionSignature(
                () -> allNull.withChildren(List.of(NullLiteral.INSTANCE, NullLiteral.INSTANCE)));
        Assertions.assertSame(allNullSignature, rewrittenAllNull.getSignature());

        Array nullToConcrete = MoreFieldsThread.keepFunctionSignature(
                () -> allNull.withChildren(List.of(new IntegerLiteral(1), NullLiteral.INSTANCE)));
        Assertions.assertThrows(AnalysisException.class, nullToConcrete::getSignature);
        Assertions.assertTrue(ChildDerivedSignature.mergeNestedTypeMetadata(
                IntegerType.INSTANCE, List.of(), List.of()).isEmpty());
    }

    @Test
    void testMapKeysTracksNestedStructMetadata() {
        StructType nullableStruct = (StructType) new CreateStruct(
                new SlotReference("value", IntegerType.INSTANCE, true)).getDataType();
        SlotReference originalMap = new SlotReference(
                "map_value", MapType.of(nullableStruct, IntegerType.INSTANCE), false);
        MapKeys original = new MapKeys(originalMap);
        original.getSignature();

        StructType requiredStruct = (StructType) new CreateStruct(
                new SlotReference("value", IntegerType.INSTANCE, false)).getDataType();
        SlotReference rewrittenMap = new SlotReference(
                "map_value", MapType.of(requiredStruct, IntegerType.INSTANCE), false);
        MapKeys rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(rewrittenMap)));

        MapType argumentType = (MapType) rewritten.getSignature().getArgType(0);
        StructType keyType = (StructType) argumentType.getKeyType();
        Assertions.assertFalse(keyType.getFields().get(0).isNullable());
        Assertions.assertEquals(keyType, ((ArrayType) rewritten.getDataType()).getItemType());
    }

    @Test
    void testArrayTransformsTrackNestedStructMetadata() {
        SlotReference nullable = new SlotReference("value", IntegerType.INSTANCE, true);
        CreateStruct originalStruct = new CreateStruct(nullable);
        Array originalArray = new Array(originalStruct);
        Array originalNestedArray = new Array(originalArray);
        ArrayFlatten originalFlatten = new ArrayFlatten(originalNestedArray);
        ArrayZip originalZip = new ArrayZip(originalArray, originalArray);
        originalFlatten.getSignature();
        originalZip.getSignature();

        SlotReference required = nullable.withNullable(false);
        CreateStruct rewrittenStruct = MoreFieldsThread.keepFunctionSignature(
                () -> originalStruct.withChildren(List.of(required)));
        Array rewrittenArray = MoreFieldsThread.keepFunctionSignature(
                () -> originalArray.withChildren(List.of(rewrittenStruct)));
        Array rewrittenNestedArray = MoreFieldsThread.keepFunctionSignature(
                () -> originalNestedArray.withChildren(List.of(rewrittenArray)));
        ArrayFlatten rewrittenFlatten = MoreFieldsThread.keepFunctionSignature(
                () -> originalFlatten.withChildren(List.of(rewrittenNestedArray)));
        ArrayZip rewrittenZip = MoreFieldsThread.keepFunctionSignature(
                () -> originalZip.withChildren(List.of(rewrittenArray, rewrittenArray)));

        Assertions.assertEquals(rewrittenNestedArray.getDataType(), rewrittenFlatten.getSignature().getArgType(0));
        Assertions.assertEquals(rewrittenArray.getDataType(), rewrittenFlatten.getDataType());
        Assertions.assertEquals(rewrittenArray.getDataType(), rewrittenZip.getSignature().getArgType(0));
        StructType zippedItemType = (StructType) ((ArrayType) rewrittenZip.getDataType()).getItemType();
        StructType zippedFieldType = (StructType) zippedItemType.getFields().get(0).getDataType();
        Assertions.assertFalse(zippedFieldType.getFields().get(0).isNullable());
    }

    @Test
    void testChangedStructArityPreservesExistingBindingAndTypesNewField() {
        SlotReference first = new SlotReference("first", DateTimeV2Type.of(0), false);
        CreateStruct original = new CreateStruct(first);
        original.getSignature();

        SlotReference changedFirst = new SlotReference("first", DateTimeV2Type.of(0), true);
        SlotReference second = new SlotReference("second", DateTimeV2Type.of(6), false);
        CreateStruct rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(changedFirst, second)));

        Assertions.assertEquals(2, rewritten.getSignature().argumentsTypes.size());
        Assertions.assertEquals(2, fieldType(rewritten).getFields().size());
        Assertions.assertEquals(DateTimeV2Type.of(0), rewritten.getSignature().getArgType(0));
        Assertions.assertEquals(DateTimeV2Type.of(6), rewritten.getSignature().getArgType(1));
        Assertions.assertEquals(DateTimeV2Type.of(0),
                fieldType(rewritten).getFields().get(0).getDataType());
        Assertions.assertEquals(DateTimeV2Type.of(6),
                fieldType(rewritten).getFields().get(1).getDataType());

        CreateNamedStruct originalNamed = new CreateNamedStruct(new StringLiteral("first"), first);
        originalNamed.getSignature();
        CreateNamedStruct rewrittenNamed = MoreFieldsThread.keepFunctionSignature(
                () -> originalNamed.withChildren(List.of(
                        new StringLiteral("first"), changedFirst,
                        new StringLiteral("second"), second)));
        Assertions.assertEquals(DateTimeV2Type.of(0), rewrittenNamed.getSignature().getArgType(1));
        Assertions.assertEquals(DateTimeV2Type.of(6), rewrittenNamed.getSignature().getArgType(3));
        Assertions.assertEquals(DateTimeV2Type.of(0),
                fieldType(rewrittenNamed).getFields().get(0).getDataType());
        Assertions.assertEquals(DateTimeV2Type.of(6),
                fieldType(rewrittenNamed).getFields().get(1).getDataType());
    }

    @Test
    void testArrayZipAddedArgumentUsesNewLeafWithoutChangingExistingBinding() {
        SlotReference originalArray = new SlotReference(
                "first", ArrayType.of(DateTimeV2Type.of(0)), false);
        ArrayZip original = new ArrayZip(originalArray);
        original.getSignature();

        SlotReference changedFirst = new SlotReference(
                "first", ArrayType.of(DateTimeV2Type.of(0)), false);
        SlotReference addedSecond = new SlotReference(
                "second", ArrayType.of(DateTimeV2Type.of(6)), false);
        ArrayZip rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(changedFirst, addedSecond)));

        Assertions.assertEquals(ArrayType.of(DateTimeV2Type.of(0)),
                rewritten.getSignature().getArgType(0));
        Assertions.assertEquals(ArrayType.of(DateTimeV2Type.of(6)),
                rewritten.getSignature().getArgType(1));
        StructType itemType = (StructType) ((ArrayType) rewritten.getSignature().returnType).getItemType();
        Assertions.assertEquals(DateTimeV2Type.of(0), itemType.getFields().get(0).getDataType());
        Assertions.assertEquals(DateTimeV2Type.of(6), itemType.getFields().get(1).getDataType());
    }

    @Test
    void testArrayZipUsesEveryVariadicChildWhenAddingAndRemovingArguments() {
        SlotReference first = new SlotReference(
                "first", ArrayType.of(DateTimeV2Type.of(0)), false);
        SlotReference second = new SlotReference(
                "second", ArrayType.of(DateTimeV2Type.of(0)), false);
        SlotReference third = new SlotReference(
                "third", ArrayType.of(DateTimeV2Type.of(0)), false);
        SlotReference fourth = new SlotReference(
                "fourth", ArrayType.of(DateTimeV2Type.of(6)), false);

        ArrayZip one = new ArrayZip(first);
        Assertions.assertEquals(1, one.getSignature().argumentsTypes.size());
        Assertions.assertEquals(1, ((StructType) ((ArrayType) one.getDataType()).getItemType()).getFields().size());
        assertRendersEveryChild(one);

        ArrayZip three = new ArrayZip(first, second, third);
        three.getSignature();
        Assertions.assertEquals(3, three.getSignature().argumentsTypes.size());
        Assertions.assertEquals(3, ((StructType) ((ArrayType) three.getDataType()).getItemType()).getFields().size());
        assertRendersEveryChild(three);

        ArrayZip freshFour = new ArrayZip(first, second, third, fourth);
        Assertions.assertEquals(4, freshFour.getSignature().argumentsTypes.size());
        Assertions.assertEquals(4,
                ((StructType) ((ArrayType) freshFour.getDataType()).getItemType()).getFields().size());
        assertRendersEveryChild(freshFour);

        ArrayZip added = MoreFieldsThread.keepFunctionSignature(
                () -> three.withChildren(List.of(first, second, third, fourth)));
        Assertions.assertEquals(4, added.getSignature().argumentsTypes.size());
        StructType addedItem = (StructType) ((ArrayType) added.getDataType()).getItemType();
        Assertions.assertEquals(4, addedItem.getFields().size());
        Assertions.assertEquals(DateTimeV2Type.of(6), addedItem.getFields().get(3).getDataType());

        ArrayZip removed = MoreFieldsThread.keepFunctionSignature(
                () -> three.withChildren(List.of(first)));
        Assertions.assertEquals(1, removed.getSignature().argumentsTypes.size());
        Assertions.assertEquals(1,
                ((StructType) ((ArrayType) removed.getDataType()).getItemType()).getFields().size());

        ArrayZip addedNull = MoreFieldsThread.keepFunctionSignature(
                () -> one.withChildren(List.of(first, NullLiteral.INSTANCE)));
        StructType addedNullItem = (StructType) ((ArrayType) addedNull.getDataType()).getItemType();
        Assertions.assertEquals(TinyIntType.INSTANCE,
                addedNullItem.getFields().get(1).getDataType());

        ArrayZip replacedWithBareNull = MoreFieldsThread.keepFunctionSignature(
                () -> one.withChildren(List.of(NullLiteral.INSTANCE)));
        Assertions.assertThrows(AnalysisException.class, replacedWithBareNull::getSignature);
    }

    @Test
    void testArrayAddedArgumentMustMatchFrozenCommonLeaf() {
        SlotReference scaleZero = new SlotReference(
                "first", DateTimeV2Type.of(0), false);
        Array original = new Array(scaleZero);
        original.getSignature();

        Array compatible = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(scaleZero, new SlotReference(
                        "second", DateTimeV2Type.of(0), false))));
        Assertions.assertEquals(ArrayType.of(DateTimeV2Type.of(0)), compatible.getSignature().returnType);

        Array incompatible = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(scaleZero, new SlotReference(
                        "second", DateTimeV2Type.of(6), false))));
        Assertions.assertThrows(AnalysisException.class, incompatible::getSignature);
    }

    @Test
    void testCreateMapArityChangeFailsClosed() {
        CreateMap original = new CreateMap(new IntegerLiteral(1), new IntegerLiteral(2));
        original.getSignature();
        CreateMap rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(
                        new IntegerLiteral(1), new IntegerLiteral(2),
                        new IntegerLiteral(3), new IntegerLiteral(4))));
        Assertions.assertThrows(AnalysisException.class, rewritten::getSignature);
    }

    @Test
    void testCreateMapDataTypeAlwaysComesFromItsResolvedSignature() {
        StructType nestedValue = new StructType(List.of(
                new StructField("event_time", DateTimeV2Type.of(6), true, "")));
        List<CreateMap> maps = List.of(
                new CreateMap(),
                new CreateMap(new IntegerLiteral(1), new StringLiteral("one")),
                new CreateMap(
                        new TinyIntLiteral((byte) 1), new StringLiteral("a"),
                        new IntegerLiteral(2), new StringLiteral("longer")),
                new CreateMap(
                        new SlotReference("decimal_key",
                                DecimalV3Type.createDecimalV3Type(20, 2), false),
                        new SlotReference("decimal_value",
                                DecimalV3Type.createDecimalV3Type(30, 8), false)),
                new CreateMap(
                        new SlotReference("time_key", DateTimeV2Type.of(0), false),
                        new SlotReference("time_value", DateTimeV2Type.of(6), false)),
                new CreateMap(NullLiteral.INSTANCE, NullLiteral.INSTANCE),
                new CreateMap(new IntegerLiteral(1), NullLiteral.INSTANCE),
                new CreateMap(new IntegerLiteral(1),
                        new SlotReference("nested_value", nestedValue, false)));

        for (CreateMap map : maps) {
            FunctionSignature signature = map.getSignature();
            Assertions.assertSame(signature.returnType, map.getDataType());
            Assertions.assertTrue(signature.returnType instanceof MapType);
            MapType mapType = (MapType) signature.returnType;
            for (int i = 0; i < signature.argumentsTypes.size(); i++) {
                Assertions.assertEquals(i % 2 == 0 ? mapType.getKeyType() : mapType.getValueType(),
                        signature.getArgType(i));
            }
            assertConcreteSignature(signature);
        }

        SlotReference scaleZero = new SlotReference("scale_zero", DateTimeV2Type.of(0), false);
        SlotReference scaleSix = new SlotReference("scale_six", DateTimeV2Type.of(6), false);
        CreateMap original = new CreateMap(scaleZero, scaleSix);
        original.getSignature();
        CreateMap incompatible = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(scaleSix, scaleZero)));
        Assertions.assertThrows(AnalysisException.class, incompatible::getDataType);
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
    void testNamedStructCaseOnlyRenameRefreshesExternalMetadata() {
        SlotReference value = new SlotReference("value", IntegerType.INSTANCE, false);
        CreateNamedStruct original = new CreateNamedStruct(new StringLiteral("MixedCase"), value);
        FunctionSignature originalSignature = original.getSignature();

        CreateNamedStruct unchanged = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(new StringLiteral("MixedCase"), value)));
        Assertions.assertSame(originalSignature, unchanged.getSignature());

        CreateNamedStruct renamed = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(new StringLiteral("MIXEDCASE"), value)));
        FunctionSignature renamedSignature = renamed.getSignature();
        Assertions.assertNotSame(originalSignature, renamedSignature);
        StructField renamedField = ((StructType) renamedSignature.returnType).getFields().get(0);
        Assertions.assertEquals("mixedcase", renamedField.getName());
        Assertions.assertEquals("MIXEDCASE", renamedField.getOriginalName());
    }

    @Test
    void testNestedOriginalNameAndCommentMetadataPreventStaleIdentityReuse() {
        StructType originalInner = new StructType(List.of(
                new StructField("event", "EventTime", DateTimeV2Type.of(0), false,
                        "old comment", true),
                new StructField("marker", "Marker", IntegerType.INSTANCE, true, "", false)));
        StructType originalOuter = new StructType(List.of(
                new StructField("payload", "Payload", originalInner, false, "", false)));
        ToJson original = new ToJson(new SlotReference("document", originalOuter, false));
        FunctionSignature originalSignature = original.getSignature();

        StructType identicalInner = new StructType(List.of(
                new StructField("event", "EventTime", DateTimeV2Type.of(0), false,
                        "old comment", true),
                new StructField("marker", "Marker", IntegerType.INSTANCE, true, "", false)));
        StructType identicalOuter = new StructType(List.of(
                new StructField("payload", "Payload", identicalInner, false, "", false)));
        ToJson identical = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(new SlotReference("document", identicalOuter, false))));
        Assertions.assertSame(originalSignature, identical.getSignature());

        StructType currentInner = new StructType(List.of(
                new StructField("event", "EVENTTIME", DateTimeV2Type.of(0), false,
                        "new comment", true),
                new StructField("marker", "Marker", IntegerType.INSTANCE, true, "", true)));
        StructType currentOuter = new StructType(List.of(
                new StructField("payload", "Payload", currentInner, false, "", false)));
        ToJson rewritten = MoreFieldsThread.keepFunctionSignature(
                () -> original.withChildren(List.of(new SlotReference("document", currentOuter, false))));
        FunctionSignature refreshedSignature = rewritten.getSignature();

        Assertions.assertNotSame(originalSignature, refreshedSignature);
        StructType refreshedOuter = (StructType) refreshedSignature.getArgType(0);
        StructType refreshedInner = (StructType) refreshedOuter.getFields().get(0).getDataType();
        StructField event = refreshedInner.getFields().get(0);
        Assertions.assertEquals("EVENTTIME", event.getOriginalName());
        Assertions.assertEquals("new comment", event.getComment());
        Assertions.assertTrue(event.isCommentSpecified());
        StructField marker = refreshedInner.getFields().get(1);
        Assertions.assertTrue(marker.isCommentSpecified());
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

    private void assertRequiredNestedMapValue(FunctionSignature signature) {
        Assertions.assertTrue(signature.returnType instanceof MapType);
        DataType valueType = ((MapType) signature.returnType).getValueType();
        Assertions.assertTrue(valueType instanceof StructType);
        Assertions.assertFalse(((StructType) valueType).getFields().get(0).isNullable());
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
    }

    private void assertRendersEveryChild(ArrayZip arrayZip) {
        String argumentsSql = arrayZip.children().stream()
                .map(Expression::toSql)
                .collect(Collectors.joining(", "));
        String argumentsShape = arrayZip.children().stream()
                .map(Expression::shapeInfo)
                .collect(Collectors.joining(", "));
        Assertions.assertEquals("array_zip(" + argumentsSql + ")", arrayZip.toSql());
        Assertions.assertEquals("array_zip(" + argumentsShape + ")", arrayZip.shapeInfo());
    }

    private void assertConcreteSignature(FunctionSignature signature) {
        assertConcreteType(signature.returnType);
        signature.argumentsTypes.forEach(this::assertConcreteType);
    }

    private void assertConcreteType(DataType dataType) {
        Assertions.assertFalse(dataType instanceof AnyDataType, dataType::toString);
        Assertions.assertFalse(dataType instanceof FollowToAnyDataType, dataType::toString);
        Assertions.assertFalse(dataType instanceof FollowToArgumentType, dataType::toString);
        if (dataType instanceof DecimalV3Type) {
            Assertions.assertNotEquals(DecimalV3Type.WILDCARD, dataType);
        }
        if (dataType instanceof DateTimeV2Type) {
            Assertions.assertNotEquals(DateTimeV2Type.WILDCARD, dataType);
        }
        if (dataType instanceof ArrayType) {
            assertConcreteType(((ArrayType) dataType).getItemType());
        } else if (dataType instanceof MapType) {
            assertConcreteType(((MapType) dataType).getKeyType());
            assertConcreteType(((MapType) dataType).getValueType());
        } else if (dataType instanceof StructType) {
            ((StructType) dataType).getFields().stream()
                    .map(StructField::getDataType)
                    .forEach(this::assertConcreteType);
        }
    }

    private StructType fieldType(BoundFunction function) {
        return (StructType) function.getSignature().returnType;
    }
}
