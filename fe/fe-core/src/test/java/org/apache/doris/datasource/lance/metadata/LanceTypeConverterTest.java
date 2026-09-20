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

package org.apache.doris.datasource.lance.metadata;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LanceTypeConverterTest {

    @Test
    public void testDate32AndDate64MapToDate() {
        Assertions.assertEquals(Type.DATEV2, LanceTypeConverter.toDorisType(
                Field.nullable("date32_col", new ArrowType.Date(DateUnit.DAY))));
        Assertions.assertEquals(Type.DATEV2, LanceTypeConverter.toDorisType(
                Field.nullable("date64_col", new ArrowType.Date(DateUnit.MILLISECOND))));
    }

    @Test
    public void testFloat16IsWidenedToFloat() {
        Assertions.assertEquals(Type.FLOAT, LanceTypeConverter.toDorisType(
                Field.nullable("float16_col", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF))));
    }

    @Test
    public void testBinaryTypesPreserveDorisLengths() {
        Assertions.assertEquals(
                "varbinary(" + ScalarType.MAX_VARBINARY_LENGTH + ")",
                LanceTypeConverter.toDorisType(
                        Field.nullable("binary_col", ArrowType.Binary.INSTANCE)).toSql());
        Assertions.assertEquals(
                "varbinary(" + ScalarType.MAX_VARBINARY_LENGTH + ")",
                LanceTypeConverter.toDorisType(
                        Field.nullable("large_binary_col", ArrowType.LargeBinary.INSTANCE)).toSql());
        Assertions.assertEquals(
                "varbinary(16)",
                LanceTypeConverter.toDorisType(
                        Field.nullable("fixed_size_binary_col",
                                new ArrowType.FixedSizeBinary(16))).toSql());
    }

    @Test
    public void testTime32AndTime64MapToTimeV2() {
        Assertions.assertEquals("time(0)", LanceTypeConverter.toDorisType(
                Field.nullable("time32_s_col", new ArrowType.Time(TimeUnit.SECOND, 32))).toSql());
        Assertions.assertEquals("time(3)", LanceTypeConverter.toDorisType(
                Field.nullable("time32_ms_col", new ArrowType.Time(TimeUnit.MILLISECOND, 32))).toSql());
        Assertions.assertEquals("time(6)", LanceTypeConverter.toDorisType(
                Field.nullable("time64_us_col", new ArrowType.Time(TimeUnit.MICROSECOND, 64))).toSql());
        Assertions.assertEquals("time(6)", LanceTypeConverter.toDorisType(
                Field.nullable("time64_ns_col", new ArrowType.Time(TimeUnit.NANOSECOND, 64))).toSql());
    }

    @Test
    public void testTimestampTimezoneControlsDorisType() {
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(6), LanceTypeConverter.toDorisType(
                Field.nullable("timestamp_us_col",
                        new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))));
        Assertions.assertEquals(ScalarType.createTimeStampTzType(3), LanceTypeConverter.toDorisType(
                Field.nullable("timestamp_ms_utc_col",
                        new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))));
        Assertions.assertEquals(ScalarType.createTimeStampTzType(6), LanceTypeConverter.toDorisType(
                Field.nullable("timestamp_ns_shanghai_col",
                        new ArrowType.Timestamp(TimeUnit.NANOSECOND, "Asia/Shanghai"))));
    }

    @Test
    public void testUnsignedIntegersAreWidenedLosslessly() {
        Assertions.assertEquals(Type.SMALLINT, LanceTypeConverter.toDorisType(
                Field.nullable("uint8_col", new ArrowType.Int(8, false))));
        Assertions.assertEquals(Type.INT, LanceTypeConverter.toDorisType(
                Field.nullable("uint16_col", new ArrowType.Int(16, false))));
        Assertions.assertEquals(Type.BIGINT, LanceTypeConverter.toDorisType(
                Field.nullable("uint32_col", new ArrowType.Int(32, false))));
        Assertions.assertEquals(Type.LARGEINT, LanceTypeConverter.toDorisType(
                Field.nullable("uint64_col", new ArrowType.Int(64, false))));
    }

    /** Verifies Arrow Null and Duration mappings. */
    @Test
    public void testNullAndDurationMappings() {
        Field nullField = Field.nullable("null_col", ArrowType.Null.INSTANCE);
        Assertions.assertEquals(Type.NULL, LanceTypeConverter.toDorisType(nullField));
        Assertions.assertTrue(LanceTypeConverter.requiresCurrentBeReader(nullField));
        for (TimeUnit unit : TimeUnit.values()) {
            Field durationField =
                    Field.nullable("duration_col", new ArrowType.Duration(unit));
            Assertions.assertEquals(Type.BIGINT,
                    LanceTypeConverter.toDorisType(durationField));
            Assertions.assertTrue(
                    LanceTypeConverter.requiresCurrentBeReader(durationField));
        }
        Field durationList = new Field(
                "duration_list",
                FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(
                        Field.nullable("item", new ArrowType.Duration(TimeUnit.MICROSECOND))));
        Assertions.assertTrue(LanceTypeConverter.requiresCurrentBeReader(durationList));
    }

    @Test
    public void testNestedNullMappings() {
        Field nullItem = Field.nullable("item", ArrowType.Null.INSTANCE);
        for (ArrowType listType : Arrays.asList(ArrowType.List.INSTANCE,
                ArrowType.LargeList.INSTANCE, new ArrowType.FixedSizeList(2))) {
            Field list = new Field("null_list", FieldType.nullable(listType),
                    Collections.singletonList(nullItem));
            Type converted = LanceTypeConverter.toDorisType(list);
            Assertions.assertInstanceOf(ArrayType.class, converted);
            Assertions.assertEquals(Type.NULL, ((ArrayType) converted).getItemType());
            Assertions.assertTrue(LanceTypeConverter.requiresCurrentBeReader(list));
        }

        Field struct = new Field("null_struct", FieldType.nullable(ArrowType.Struct.INSTANCE),
                Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true)), nullItem));
        Type convertedStruct = LanceTypeConverter.toDorisType(struct);
        Assertions.assertInstanceOf(StructType.class, convertedStruct);
        Assertions.assertEquals(Type.NULL, ((StructType) convertedStruct).getFields().get(1).getType());

        Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                Arrays.asList(Field.notNullable("key", ArrowType.Utf8.INSTANCE), nullItem));
        Field map = new Field("null_map", FieldType.nullable(new ArrowType.Map(false)),
                Collections.singletonList(entries));
        Type convertedMap = LanceTypeConverter.toDorisType(map);
        Assertions.assertInstanceOf(MapType.class, convertedMap);
        Assertions.assertEquals(Type.NULL, ((MapType) convertedMap).getValueType());

        Field nested = new Field("nested", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(struct));
        ArrayType convertedNested = (ArrayType) LanceTypeConverter.toDorisType(nested);
        Assertions.assertEquals(Type.NULL,
                ((StructType) convertedNested.getItemType()).getFields().get(1).getType());
    }

    @Test
    public void testDeepNullComposites() {
        Field leaf = Field.nullable("item", ArrowType.Null.INSTANCE);
        Field list = new Field("item", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(leaf));
        Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                Arrays.asList(Field.notNullable("key", ArrowType.Utf8.INSTANCE), leaf));
        Field map = new Field("item", FieldType.nullable(new ArrowType.Map(false)),
                Collections.singletonList(entries));
        for (Field child : Arrays.asList(list, map)) {
            Type childType = LanceTypeConverter.toDorisType(child);
            for (ArrowType outer : Arrays.asList(ArrowType.List.INSTANCE,
                    ArrowType.LargeList.INSTANCE, new ArrowType.FixedSizeList(2))) {
                Field nested = new Field("nested", FieldType.nullable(outer),
                        Collections.singletonList(child));
                Assertions.assertEquals(new ArrayType(childType), LanceTypeConverter.toDorisType(nested));
            }
            Field struct = new Field("nested", FieldType.nullable(ArrowType.Struct.INSTANCE),
                    Collections.singletonList(child));
            Type converted = LanceTypeConverter.toDorisType(struct);
            Assertions.assertInstanceOf(StructType.class, converted);
            Assertions.assertEquals(childType, ((StructType) converted).getFields().get(0).getType());
            Field nestedEntries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                    Arrays.asList(Field.notNullable("key", ArrowType.Utf8.INSTANCE), child));
            Field nestedMap = new Field("nested", FieldType.nullable(new ArrowType.Map(false)),
                    Collections.singletonList(nestedEntries));
            Assertions.assertEquals(new MapType(Type.STRING, childType, false, true),
                    LanceTypeConverter.toDorisType(nestedMap));
        }
    }

    @Test
    public void testNonNullableNullLeavesAreUnsupported() {
        Field leaf = Field.notNullable("item", ArrowType.Null.INSTANCE);
        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(leaf));
        for (ArrowType outer : Arrays.asList(ArrowType.List.INSTANCE,
                ArrowType.LargeList.INSTANCE, new ArrowType.FixedSizeList(2), ArrowType.Struct.INSTANCE)) {
            Field nested = new Field("nested", FieldType.nullable(outer), Collections.singletonList(leaf));
            Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(nested));
        }
        for (boolean nullKey : Arrays.asList(false, true)) {
            Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                    Arrays.asList(nullKey ? leaf : Field.notNullable("key", ArrowType.Utf8.INSTANCE),
                            nullKey ? Field.nullable("value", ArrowType.Utf8.INSTANCE) : leaf));
            Field map = new Field("map", FieldType.nullable(new ArrowType.Map(false)),
                    Collections.singletonList(entries));
            Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(map));
            Field nested = new Field("nested", FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(map));
            Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(nested));
        }
    }

    /** Verifies known extension mappings and storage validation. */
    @Test
    public void testKnownExtensionMappingsAndStorageValidation() {
        Assertions.assertEquals(Type.JSONB, LanceTypeConverter.toDorisType(
                extensionField("arrow_json_col", ArrowType.Utf8.INSTANCE, "arrow.json")));
        Assertions.assertTrue(LanceTypeConverter.requiresCurrentBeReader(
                extensionField("arrow_json_col", ArrowType.Utf8.INSTANCE, "arrow.json")));
        Assertions.assertEquals(Type.JSONB, LanceTypeConverter.toDorisType(
                extensionField("lance_json_col", ArrowType.LargeBinary.INSTANCE, "lance.json")));

        Field bfloat16Item = extensionField(
                "item", new ArrowType.FixedSizeBinary(2), "lance.bfloat16");
        Assertions.assertEquals(Type.FLOAT, LanceTypeConverter.toDorisType(bfloat16Item));
        Field bfloat16Vector = new Field(
                "bfloat16_vector_col",
                FieldType.nullable(new ArrowType.FixedSizeList(4)),
                Collections.singletonList(bfloat16Item));
        Assertions.assertEquals("array<float>",
                LanceTypeConverter.toDorisType(bfloat16Vector).toSql());
        Assertions.assertTrue(LanceTypeConverter.requiresCurrentBeReader(bfloat16Vector));
        Assertions.assertFalse(LanceTypeConverter.requiresCurrentBeReader(
                Field.nullable("ordinary", ArrowType.Utf8.INSTANCE)));

        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(
                extensionField("invalid_json", ArrowType.Binary.INSTANCE, "arrow.json")));
        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(
                extensionField("invalid_bfloat16", new ArrowType.FixedSizeBinary(4),
                        "lance.bfloat16")));
    }

    /** Verifies unknown extensions and dictionary fields remain unsupported. */
    @Test
    public void testUnknownExtensionAndDictionaryMarkersAreUnsupported() {
        Field extensionField = extensionField(
                "extension_col", ArrowType.Utf8.INSTANCE, "doris.test.extension");
        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(extensionField));

        Field dictionaryField = new Field(
                "dictionary_col",
                new FieldType(
                        true,
                        ArrowType.Utf8.INSTANCE,
                        new DictionaryEncoding(1, false, new ArrowType.Int(16, true))),
                Collections.emptyList());
        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(dictionaryField));

        Field blobField = new Field(
                "blob_col",
                new FieldType(
                        true,
                        ArrowType.Struct.INSTANCE,
                        null,
                        Collections.singletonMap("ARROW:extension:name", "lance.blob.v2")),
                Collections.emptyList());
        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(blobField));
    }

    @Test
    public void testDorisSchemaRoundTripForSupportedTypes() {
        StructType struct = new StructType(new StructField(
                "required", Type.BOOLEAN, "nested comment", false));
        List<Column> columns = Arrays.asList(
                new Column("id", Type.INT, false, "identifier"),
                new Column("tags", new ArrayType(Type.STRING), true),
                new Column("attrs", new MapType(Type.STRING, Type.BIGINT, false, true), true),
                new Column("payload", struct, true),
                new Column("document", Type.JSONB, true));

        Schema schema = LanceTypeConverter.toArrowSchema(columns);

        Assertions.assertEquals(5, schema.getFields().size());
        Assertions.assertEquals("identifier", schema.findField("id").getMetadata().get("comment"));
        Assertions.assertFalse(schema.findField("id").isNullable());
        Assertions.assertEquals("arrow.json",
                schema.findField("document").getMetadata().get("ARROW:extension:name"));
        Assertions.assertFalse(schema.findField("payload").getChildren().get(0).isNullable());
        Assertions.assertEquals("nested comment",
                schema.findField("payload").getChildren().get(0).getMetadata().get("comment"));

        for (int i = 0; i < columns.size(); i++) {
            Assertions.assertEquals(columns.get(i).getType(),
                    LanceTypeConverter.toDorisType(schema.getFields().get(i)));
        }
    }

    @Test
    public void testDorisDecimalSelectsArrowBitWidth() {
        Schema schema = LanceTypeConverter.toArrowSchema(Arrays.asList(
                new Column("decimal128", ScalarType.createDecimalV3Type(38, 4), true),
                new Column("decimal256", ScalarType.createDecimalV3Type(40, 4), true)));

        Assertions.assertEquals(128,
                ((ArrowType.Decimal) schema.findField("decimal128").getType()).getBitWidth());
        Assertions.assertEquals(256,
                ((ArrowType.Decimal) schema.findField("decimal256").getType()).getBitWidth());
    }

    @Test
    public void testUnsupportedDorisTypeFailsBeforeTableCreation() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceTypeConverter.toArrowSchema(
                        Collections.singletonList(new Column("large", Type.LARGEINT, true))));
        Assertions.assertTrue(exception.getMessage().contains("largeint"));
    }

    /** Creates a field with Arrow extension metadata. */
    private static Field extensionField(String name, ArrowType storageType, String extensionName) {
        return new Field(
                name,
                new FieldType(
                        true,
                        storageType,
                        null,
                        Collections.singletonMap("ARROW:extension:name", extensionName)),
                Collections.emptyList());
    }
}
