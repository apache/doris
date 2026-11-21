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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;

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

/** The Arrow-to-Doris mapping Lance tables have shown since 4.1, one Arrow type family per case. */
class LanceTypeMappingTest {

    private static final ConnectorType UNSUPPORTED = ConnectorType.of("UNSUPPORTED");
    private static final ConnectorType NULL_TYPE = ConnectorType.of("NULL_TYPE");

    @Test
    void date32AndDate64MapToDate() {
        Assertions.assertEquals(ConnectorType.of("DATEV2"), map(
                Field.nullable("date32_col", new ArrowType.Date(DateUnit.DAY))));
        Assertions.assertEquals(ConnectorType.of("DATEV2"), map(
                Field.nullable("date64_col", new ArrowType.Date(DateUnit.MILLISECOND))));
    }

    @Test
    void float16IsWidenedToFloat() {
        Assertions.assertEquals(ConnectorType.of("FLOAT"), map(
                Field.nullable("float16_col", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF))));
        Assertions.assertEquals(ConnectorType.of("DOUBLE"), map(
                Field.nullable("float64_col", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
    }

    /** An unbounded VARBINARY is what the engine shows as varbinary(2147483647). */
    @Test
    void binaryTypesKeepTheirLengths() {
        Assertions.assertEquals(ConnectorType.of("VARBINARY"), map(
                Field.nullable("binary_col", ArrowType.Binary.INSTANCE)));
        Assertions.assertEquals(ConnectorType.of("VARBINARY"), map(
                Field.nullable("large_binary_col", ArrowType.LargeBinary.INSTANCE)));
        Assertions.assertEquals(ConnectorType.of("VARBINARY", 16, 0), map(
                Field.nullable("fixed_size_binary_col", new ArrowType.FixedSizeBinary(16))));
    }

    /** The scale rides in the precision slot, as for DATETIMEV2. */
    @Test
    void time32AndTime64MapToTimeV2() {
        Assertions.assertEquals(ConnectorType.of("TIMEV2", 0, 0), map(
                Field.nullable("time32_s_col", new ArrowType.Time(TimeUnit.SECOND, 32))));
        Assertions.assertEquals(ConnectorType.of("TIMEV2", 3, 0), map(
                Field.nullable("time32_ms_col", new ArrowType.Time(TimeUnit.MILLISECOND, 32))));
        Assertions.assertEquals(ConnectorType.of("TIMEV2", 6, 0), map(
                Field.nullable("time64_us_col", new ArrowType.Time(TimeUnit.MICROSECOND, 64))));
        Assertions.assertEquals(ConnectorType.of("TIMEV2", 6, 0), map(
                Field.nullable("time64_ns_col", new ArrowType.Time(TimeUnit.NANOSECOND, 64))));
        // A time unit stored in the other bit width is not a valid Arrow time.
        Assertions.assertEquals(UNSUPPORTED, map(
                Field.nullable("time64_ms_col", new ArrowType.Time(TimeUnit.MILLISECOND, 64))));
    }

    @Test
    void timestampTimeZoneControlsDorisType() {
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0), map(
                Field.nullable("timestamp_us_col", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))));
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 0, 0), map(
                Field.nullable("timestamp_s_col", new ArrowType.Timestamp(TimeUnit.SECOND, ""))));
        Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 3, 0), map(
                Field.nullable("timestamp_ms_utc_col", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))));
        Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 6, 0), map(
                Field.nullable("timestamp_ns_shanghai_col",
                        new ArrowType.Timestamp(TimeUnit.NANOSECOND, "Asia/Shanghai"))));
    }

    @Test
    void unsignedIntegersAreWidenedLosslessly() {
        Assertions.assertEquals(ConnectorType.of("SMALLINT"), map(
                Field.nullable("uint8_col", new ArrowType.Int(8, false))));
        Assertions.assertEquals(ConnectorType.of("INT"), map(
                Field.nullable("uint16_col", new ArrowType.Int(16, false))));
        Assertions.assertEquals(ConnectorType.of("BIGINT"), map(
                Field.nullable("uint32_col", new ArrowType.Int(32, false))));
        Assertions.assertEquals(ConnectorType.of("LARGEINT"), map(
                Field.nullable("uint64_col", new ArrowType.Int(64, false))));
        Assertions.assertEquals(ConnectorType.of("TINYINT"), map(
                Field.nullable("int8_col", new ArrowType.Int(8, true))));
    }

    @Test
    void decimalsUpTo76DigitsMapToDecimalV3() {
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 38, 10), map(
                Field.nullable("decimal128_col", new ArrowType.Decimal(38, 10, 128))));
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 76, 38), map(
                Field.nullable("decimal256_col", new ArrowType.Decimal(76, 38, 256))));
    }

    @Test
    void nullAndDurationMappings() {
        Field nullField = Field.nullable("null_col", ArrowType.Null.INSTANCE);
        Assertions.assertEquals(NULL_TYPE, map(nullField));
        Assertions.assertTrue(LanceTypeMapping.requiresCurrentBeReader(nullField));
        for (TimeUnit unit : TimeUnit.values()) {
            Field durationField = Field.nullable("duration_col", new ArrowType.Duration(unit));
            Assertions.assertEquals(ConnectorType.of("BIGINT"), map(durationField));
            Assertions.assertTrue(LanceTypeMapping.requiresCurrentBeReader(durationField));
        }
        Field durationList = new Field("duration_list", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(Field.nullable("item", new ArrowType.Duration(TimeUnit.MICROSECOND))));
        Assertions.assertTrue(LanceTypeMapping.requiresCurrentBeReader(durationList));
    }

    @Test
    void nestedNullMappings() {
        Field nullItem = Field.nullable("item", ArrowType.Null.INSTANCE);
        for (ArrowType listType : Arrays.asList(ArrowType.List.INSTANCE,
                ArrowType.LargeList.INSTANCE, new ArrowType.FixedSizeList(2))) {
            Field list = new Field("null_list", FieldType.nullable(listType), Collections.singletonList(nullItem));
            Assertions.assertEquals(ConnectorType.arrayOf(NULL_TYPE), map(list));
            Assertions.assertTrue(LanceTypeMapping.requiresCurrentBeReader(list));
        }

        Field struct = new Field("null_struct", FieldType.nullable(ArrowType.Struct.INSTANCE),
                Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true)), nullItem));
        ConnectorType convertedStruct = map(struct);
        Assertions.assertEquals("STRUCT", convertedStruct.getTypeName());
        Assertions.assertEquals(Arrays.asList("id", "item"), convertedStruct.getFieldNames());
        Assertions.assertEquals(NULL_TYPE, convertedStruct.getChildren().get(1));

        Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                Arrays.asList(Field.notNullable("key", ArrowType.Utf8.INSTANCE), nullItem));
        Field map = new Field("null_map", FieldType.nullable(new ArrowType.Map(false)),
                Collections.singletonList(entries));
        ConnectorType convertedMap = map(map);
        Assertions.assertEquals(ConnectorType.mapOf(ConnectorType.of("STRING"), NULL_TYPE), convertedMap);
        // Key and value nullability are kept as Arrow declares them.
        Assertions.assertFalse(convertedMap.isChildNullable(0));
        Assertions.assertTrue(convertedMap.isChildNullable(1));

        Field nested = new Field("nested", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(struct));
        Assertions.assertEquals(ConnectorType.arrayOf(convertedStruct), map(nested));
    }

    @Test
    void nonNullableNullLeavesAreUnsupported() {
        Field leaf = Field.notNullable("item", ArrowType.Null.INSTANCE);
        Assertions.assertEquals(UNSUPPORTED, map(leaf));
        for (ArrowType outer : Arrays.asList(ArrowType.List.INSTANCE,
                ArrowType.LargeList.INSTANCE, new ArrowType.FixedSizeList(2), ArrowType.Struct.INSTANCE)) {
            Field nested = new Field("nested", FieldType.nullable(outer), Collections.singletonList(leaf));
            Assertions.assertEquals(UNSUPPORTED, map(nested));
        }
        for (boolean nullKey : Arrays.asList(false, true)) {
            Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                    Arrays.asList(nullKey ? leaf : Field.notNullable("key", ArrowType.Utf8.INSTANCE),
                            nullKey ? Field.nullable("value", ArrowType.Utf8.INSTANCE) : leaf));
            Field map = new Field("map", FieldType.nullable(new ArrowType.Map(false)),
                    Collections.singletonList(entries));
            Assertions.assertEquals(UNSUPPORTED, map(map));
            Field nested = new Field("nested", FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(map));
            Assertions.assertEquals(UNSUPPORTED, map(nested));
        }
    }

    @Test
    void knownExtensionMappingsAndStorageValidation() {
        Assertions.assertEquals(ConnectorType.of("JSONB"), map(
                extensionField("arrow_json_col", ArrowType.Utf8.INSTANCE, "arrow.json")));
        Assertions.assertTrue(LanceTypeMapping.requiresCurrentBeReader(
                extensionField("arrow_json_col", ArrowType.Utf8.INSTANCE, "arrow.json")));
        Assertions.assertEquals(ConnectorType.of("JSONB"), map(
                extensionField("lance_json_col", ArrowType.LargeBinary.INSTANCE, "lance.json")));

        Field bfloat16Item = extensionField("item", new ArrowType.FixedSizeBinary(2), "lance.bfloat16");
        Assertions.assertEquals(ConnectorType.of("FLOAT"), map(bfloat16Item));
        Field bfloat16Vector = new Field("bfloat16_vector_col",
                FieldType.nullable(new ArrowType.FixedSizeList(4)), Collections.singletonList(bfloat16Item));
        Assertions.assertEquals(ConnectorType.arrayOf(ConnectorType.of("FLOAT")), map(bfloat16Vector));
        Assertions.assertTrue(LanceTypeMapping.requiresCurrentBeReader(bfloat16Vector));
        Assertions.assertFalse(LanceTypeMapping.requiresCurrentBeReader(
                Field.nullable("ordinary", ArrowType.Utf8.INSTANCE)));

        Assertions.assertEquals(UNSUPPORTED, map(
                extensionField("invalid_json", ArrowType.Binary.INSTANCE, "arrow.json")));
        Assertions.assertEquals(UNSUPPORTED, map(
                extensionField("invalid_bfloat16", new ArrowType.FixedSizeBinary(4), "lance.bfloat16")));
    }

    @Test
    void unknownExtensionAndDictionaryMarkersAreUnsupported() {
        Assertions.assertEquals(UNSUPPORTED, map(
                extensionField("extension_col", ArrowType.Utf8.INSTANCE, "doris.test.extension")));

        Field dictionaryField = new Field("dictionary_col",
                new FieldType(true, ArrowType.Utf8.INSTANCE,
                        new DictionaryEncoding(1, false, new ArrowType.Int(16, true))),
                Collections.emptyList());
        Assertions.assertEquals(UNSUPPORTED, map(dictionaryField));

        Field blobField = new Field("blob_col",
                new FieldType(true, ArrowType.Struct.INSTANCE, null,
                        Collections.singletonMap("ARROW:extension:name", "lance.blob.v2")),
                Collections.emptyList());
        Assertions.assertEquals(UNSUPPORTED, map(blobField));
    }

    /** A column the engine cannot read stays visible, and only a query reading it fails. */
    @Test
    void columnsKeepNullabilityAndCommentsAndUnsupportedOnesStay() {
        Field commented = new Field("label",
                new FieldType(false, ArrowType.Utf8.INSTANCE, null, Collections.singletonMap("comment", "a label")),
                Collections.emptyList());
        Field blob = extensionField("blob_col", ArrowType.LargeBinary.INSTANCE, "lance.blob.v2");
        List<ConnectorColumn> columns = LanceTypeMapping.toColumns(new Schema(Arrays.asList(commented, blob)));
        Assertions.assertEquals(2, columns.size());
        Assertions.assertEquals("label", columns.get(0).getName());
        Assertions.assertEquals("a label", columns.get(0).getComment());
        Assertions.assertFalse(columns.get(0).isNullable());
        Assertions.assertFalse(columns.get(0).isKey());
        Assertions.assertEquals(UNSUPPORTED, columns.get(1).getType());
        Assertions.assertTrue(columns.get(1).isNullable());
    }

    private static ConnectorType map(Field field) {
        return LanceTypeMapping.toConnectorType(field);
    }

    private static Field extensionField(String name, ArrowType storageType, String extensionName) {
        return new Field(name,
                new FieldType(true, storageType, null, Collections.singletonMap("ARROW:extension:name", extensionName)),
                Collections.emptyList());
    }
}
