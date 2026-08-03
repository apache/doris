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

package org.apache.doris.paimon;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VariantType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;

public class PaimonArrowConverterTest {

    @Test
    public void testTimestampWithoutTimeZonePreservesDstGapWallClock() {
        LocalDateTime wallClock = LocalDateTime.parse("2024-03-10T02:30:00.123456");
        long micros = wallClock.toEpochSecond(ZoneOffset.UTC) * 1_000_000L
                + wallClock.getNano() / 1_000L;
        ArrowType.Timestamp arrowType = new ArrowType.Timestamp(
                TimeUnit.MICROSECOND, null);

        PaimonArrowConverter converter = new PaimonArrowConverter(
                ZoneId.of("America/Los_Angeles"));
        Timestamp result = converter.toPaimonTimestamp(
                micros, arrowType, new TimestampType(6));

        Assertions.assertEquals(
                wallClock, result.toLocalDateTime());
    }

    @Test
    public void testLocalZonedTimestampPreservesInstant() {
        LocalDateTime wallClock = LocalDateTime.parse("2024-01-15T10:30:00.123456");
        long civilMicros = wallClock.toEpochSecond(ZoneOffset.UTC) * 1_000_000L
                + wallClock.getNano() / 1_000L;
        ArrowType.Timestamp arrowType = new ArrowType.Timestamp(
                TimeUnit.MICROSECOND, null);

        PaimonArrowConverter converter = new PaimonArrowConverter(
                ZoneId.of("Asia/Shanghai"));
        Timestamp result = converter.toPaimonTimestamp(
                civilMicros, arrowType, new LocalZonedTimestampType(6));

        long expectedMicros = wallClock.toEpochSecond(ZoneOffset.ofHours(8)) * 1_000_000L
                + wallClock.getNano() / 1_000L;
        Assertions.assertEquals(expectedMicros, result.toMicros());
        Assertions.assertEquals(expectedMicros,
                converter.toPaimonTimestamp(
                        wallClock, new LocalZonedTimestampType(6)).toMicros());
    }

    @Test
    public void testPaimonWriteRejectsTimezoneInArrowType() {
        PaimonArrowConverter converter = new PaimonArrowConverter(ZoneId.of("UTC"));
        ArrowType.Timestamp arrowType = new ArrowType.Timestamp(
                TimeUnit.MICROSECOND, "Asia/Shanghai");

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> converter.toPaimonTimestamp(
                        0, arrowType, new TimestampType(6)));
    }

    @Test
    public void testVariantJsonTransportIsRejected() {
        Field variantField = new Field(
                "payload", FieldType.nullable(new ArrowType.Utf8()), null);
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(
                        new Schema(Collections.singletonList(variantField)), allocator)) {
            VarCharVector vector = (VarCharVector) root.getVector("payload");
            root.allocateNew();
            vector.setSafe(0, "{\"legacy\":true}".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(1);

            PaimonArrowConverter.RowReader rows =
                    new PaimonArrowConverter(ZoneId.of("UTC")).rows(
                            root, new org.apache.paimon.types.DataType[] {new VariantType()});
            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> rows.values(0));
            Assertions.assertTrue(exception.getCause().getMessage().contains(
                    "only supports Variant V2"));
        }
    }

    @Test
    public void testVariantBinaryTransportPreservesValueAndMetadata() {
        GenericVariant expected = GenericVariant.fromJson(
                "{\"id\":1,\"nested\":[true,null,\"doris\"]}");
        Field variantField = variantField();

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(
                        new Schema(Collections.singletonList(variantField)), allocator)) {
            StructVector vector = (StructVector) root.getVector("payload");
            VarBinaryVector values = (VarBinaryVector) vector.getChild(
                    PaimonArrowConverter.VARIANT_VALUE_FIELD);
            VarBinaryVector metadata = (VarBinaryVector) vector.getChild(
                    PaimonArrowConverter.VARIANT_METADATA_FIELD);
            root.allocateNew();
            values.setSafe(0, expected.value());
            metadata.setSafe(0, expected.metadata());
            vector.setIndexDefined(0);
            vector.setNull(1);
            root.setRowCount(2);

            PaimonArrowConverter converter = new PaimonArrowConverter(ZoneId.of("UTC"));
            PaimonArrowConverter.RowReader rows = converter.rows(
                    root, new org.apache.paimon.types.DataType[] {new VariantType()});
            GenericVariant actual = (GenericVariant) rows.values(0)[0];

            Assertions.assertArrayEquals(expected.value(), actual.value());
            Assertions.assertArrayEquals(expected.metadata(), actual.metadata());
            Assertions.assertNull(rows.values(1)[0]);
        }
    }

    @Test
    public void testVariantBinaryTransportRejectsUnsupportedNestedPrimitive() {
        GenericVariant array = GenericVariant.fromJson("[0]");
        byte[] unsupportedValue = array.value().clone();
        int primitiveHeaderOffset = unsupportedValue.length - 2;
        // The final array element is INT1 (id 3). Replace it with TIME_NTZ_MICROS (id 17),
        // which Doris V2 can encode but Paimon 1.4.2 does not recognize.
        Assertions.assertEquals(3 << 2, unsupportedValue[primitiveHeaderOffset] & 0xff);
        unsupportedValue[primitiveHeaderOffset] = (byte) (17 << 2);

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(
                        new Schema(Collections.singletonList(variantField())), allocator)) {
            StructVector vector = (StructVector) root.getVector("payload");
            VarBinaryVector values = (VarBinaryVector) vector.getChild(
                    PaimonArrowConverter.VARIANT_VALUE_FIELD);
            VarBinaryVector metadata = (VarBinaryVector) vector.getChild(
                    PaimonArrowConverter.VARIANT_METADATA_FIELD);
            root.allocateNew();
            values.setSafe(0, unsupportedValue);
            metadata.setSafe(0, array.metadata());
            vector.setIndexDefined(0);
            root.setRowCount(1);

            PaimonArrowConverter.RowReader rows =
                    new PaimonArrowConverter(ZoneId.of("UTC")).rows(
                            root, new org.apache.paimon.types.DataType[] {new VariantType()});
            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> rows.values(0));
            Assertions.assertTrue(exception.getMessage().contains("payload"));
            Assertions.assertTrue(exception.getCause().getMessage().contains(
                    "UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT"));
        }
    }

    @Test
    public void testVariantBinaryTransportRejectsMissingMetadata() {
        Field variantField = new Field(
                "payload",
                FieldType.nullable(new ArrowType.Struct()),
                Arrays.asList(
                        new Field("value", FieldType.nullable(new ArrowType.Binary()), null),
                        new Field("metadata", FieldType.nullable(new ArrowType.Binary()), null)));
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(
                        new Schema(Collections.singletonList(variantField)), allocator)) {
            StructVector vector = (StructVector) root.getVector("payload");
            VarBinaryVector values = (VarBinaryVector) vector.getChild("value");
            root.allocateNew();
            values.setSafe(0, GenericVariant.fromJson("1").value());
            vector.setIndexDefined(0);
            root.setRowCount(1);

            PaimonArrowConverter.RowReader rows =
                    new PaimonArrowConverter(ZoneId.of("UTC")).rows(
                            root, new org.apache.paimon.types.DataType[] {new VariantType()});
            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> rows.values(0));
            Assertions.assertTrue(exception.getMessage().contains("payload"));
            Assertions.assertTrue(exception.getCause().getMessage().contains("metadata"));
        }
    }

    @Test
    public void testStructSchemaUsesPositionAndAcceptsCaseInsensitiveNames() {
        RowType rowType = mixedCaseRowType();
        Assertions.assertDoesNotThrow(() -> PaimonArrowConverter.validateStructSchema(
                rowType, Arrays.asList("foo", "FOO")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonArrowConverter.validateStructSchema(
                        rowType, Arrays.asList("foo", "different")));
    }

    private static RowType mixedCaseRowType() {
        return DataTypes.ROW(
                DataTypes.FIELD(0, "Foo", DataTypes.INT()),
                DataTypes.FIELD(1, "foo", DataTypes.INT()));
    }

    private static Field variantField() {
        return new Field(
                "payload",
                FieldType.nullable(new ArrowType.Struct()),
                Arrays.asList(
                        new Field(
                                PaimonArrowConverter.VARIANT_VALUE_FIELD,
                                FieldType.notNullable(new ArrowType.Binary()),
                                null),
                        new Field(
                                PaimonArrowConverter.VARIANT_METADATA_FIELD,
                                FieldType.notNullable(new ArrowType.Binary()),
                                null)));
    }
}
