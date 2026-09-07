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
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class PaimonArrowBatchAdapterTest {

    @Test
    public void testAllRegularPaimonTypesUseOfficialColumnarAdapter() throws Exception {
        RowType nestedType = DataTypes.ROW(
                DataTypes.FIELD(0, "code", DataTypes.INT()),
                DataTypes.FIELD(1, "label", DataTypes.STRING()));
        RowType inputType = DataTypes.ROW(
                DataTypes.FIELD(0, "boolean_value", DataTypes.BOOLEAN()),
                DataTypes.FIELD(1, "tinyint_value", DataTypes.TINYINT()),
                DataTypes.FIELD(2, "smallint_value", DataTypes.SMALLINT()),
                DataTypes.FIELD(3, "int_value", DataTypes.INT()),
                DataTypes.FIELD(4, "bigint_value", DataTypes.BIGINT()),
                DataTypes.FIELD(5, "float_value", DataTypes.FLOAT()),
                DataTypes.FIELD(6, "double_value", DataTypes.DOUBLE()),
                DataTypes.FIELD(7, "char_value", DataTypes.CHAR(8)),
                DataTypes.FIELD(8, "varchar_value", DataTypes.VARCHAR(32)),
                DataTypes.FIELD(9, "binary_value", DataTypes.BINARY(4)),
                DataTypes.FIELD(10, "varbinary_value", DataTypes.VARBINARY(32)),
                DataTypes.FIELD(11, "decimal_value", DataTypes.DECIMAL(10, 2)),
                DataTypes.FIELD(12, "date_value", DataTypes.DATE()),
                DataTypes.FIELD(13, "array_value", DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.FIELD(14, "map_value", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                DataTypes.FIELD(15, "row_value", nestedType));

        Map<BinaryString, Integer> mapValue = new LinkedHashMap<>();
        mapValue.put(BinaryString.fromString("one"), 1);
        mapValue.put(BinaryString.fromString("two"), 2);
        GenericRow source = GenericRow.of(
                true,
                (byte) 7,
                (short) 300,
                12345,
                9_876_543_210L,
                1.25F,
                2.5D,
                BinaryString.fromString("char"),
                BinaryString.fromString("varchar"),
                new byte[] {0, 1, (byte) 0xfe, (byte) 0xff},
                new byte[] {9, 8, 7},
                Decimal.fromBigDecimal(new BigDecimal("12345.67"), 10, 2),
                19_723,
                new GenericArray(new int[] {3, 4, 5}),
                new GenericMap(mapValue),
                GenericRow.of(42, BinaryString.fromString("nested")));
        GenericRow nulls = new GenericRow(inputType.getFieldCount());

        try (RootAllocator allocator = new RootAllocator()) {
            PaimonArrowBatchAdapter adapter = new PaimonArrowBatchAdapter(
                    inputType, ZoneId.of("UTC"), allocator);
            try (ArrowStreamReader reader = schemaReader(adapter, allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                root.allocateNew();
                for (int i = 0; i < inputType.getFieldCount(); i++) {
                    DataType type = inputType.getTypeAt(i);
                    ArrowFieldWriter writer = type.accept(ArrowFieldWriterFactoryVisitor.INSTANCE)
                            .create(root.getVector(i), type.isNullable());
                    writer.write(0, source, i);
                    writer.write(1, nulls, i);
                    root.getVector(i).setValueCount(2);
                }
                root.setRowCount(2);

                PaimonArrowBatchAdapter.Rows rows = adapter.rows(root);
                InternalRow actual = rows.row(0);
                Assertions.assertTrue(actual.getBoolean(0));
                Assertions.assertEquals(7, actual.getByte(1));
                Assertions.assertEquals(300, actual.getShort(2));
                Assertions.assertEquals(12345, actual.getInt(3));
                Assertions.assertEquals(9_876_543_210L, actual.getLong(4));
                Assertions.assertEquals(1.25F, actual.getFloat(5));
                Assertions.assertEquals(2.5D, actual.getDouble(6));
                Assertions.assertEquals("char", actual.getString(7).toString());
                Assertions.assertEquals("varchar", actual.getString(8).toString());
                Assertions.assertArrayEquals(
                        new byte[] {0, 1, (byte) 0xfe, (byte) 0xff}, actual.getBinary(9));
                Assertions.assertArrayEquals(new byte[] {9, 8, 7}, actual.getBinary(10));
                Assertions.assertEquals(
                        new BigDecimal("12345.67"), actual.getDecimal(11, 10, 2).toBigDecimal());
                Assertions.assertEquals(19_723, actual.getInt(12));

                InternalArray array = actual.getArray(13);
                Assertions.assertEquals(3, array.size());
                Assertions.assertEquals(4, array.getInt(1));
                InternalMap map = actual.getMap(14);
                Assertions.assertEquals(2, map.size());
                Assertions.assertEquals("one", map.keyArray().getString(0).toString());
                Assertions.assertEquals(1, map.valueArray().getInt(0));
                InternalRow nested = actual.getRow(15, 2);
                Assertions.assertEquals(42, nested.getInt(0));
                Assertions.assertEquals("nested", nested.getString(1).toString());

                InternalRow nullRow = rows.row(1);
                for (int i = 0; i < inputType.getFieldCount(); i++) {
                    Assertions.assertTrue(nullRow.isNullAt(i), "field " + i + " is not null");
                }
            }
        }
    }

    @Test
    public void testTargetSchemaDistinguishesNtzAndLtz() throws Exception {
        RowType inputType = DataTypes.ROW(
                DataTypes.FIELD(0, "ntz", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD(1, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
                DataTypes.FIELD(2, "ntz9", DataTypes.TIMESTAMP(9)));

        try (RootAllocator allocator = new RootAllocator()) {
            PaimonArrowBatchAdapter adapter = new PaimonArrowBatchAdapter(
                    inputType, ZoneId.of("Asia/Shanghai"), allocator);
            try (ArrowStreamReader reader = schemaReader(adapter, allocator)) {
                Schema schema = reader.getVectorSchemaRoot().getSchema();
                ArrowType.Timestamp ntz = (ArrowType.Timestamp) schema.findField("ntz").getType();
                ArrowType.Timestamp ltz = (ArrowType.Timestamp) schema.findField("ltz").getType();
                ArrowType.Timestamp ntz9 =
                        (ArrowType.Timestamp) schema.findField("ntz9").getType();

                Assertions.assertEquals(TimeUnit.MICROSECOND, ntz.getUnit());
                Assertions.assertNull(ntz.getTimezone());
                Assertions.assertEquals(TimeUnit.MICROSECOND, ltz.getUnit());
                Assertions.assertEquals("Asia/Shanghai", ltz.getTimezone());
                // Doris timestamps have microsecond precision, so a wider Paimon target still
                // uses a microsecond transport instead of pretending the input contains nanos.
                Assertions.assertEquals(TimeUnit.MICROSECOND, ntz9.getUnit());
                Assertions.assertNull(ntz9.getTimezone());
            }
        }
    }

    @Test
    public void testTimestampAdaptationDoesNotRepeatTimezoneConversion() throws Exception {
        RowType inputType = DataTypes.ROW(
                DataTypes.FIELD(0, "ntz", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD(1, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
                DataTypes.FIELD(2, "ntz9", DataTypes.TIMESTAMP(9)));
        long ntzMicros = 1_705_312_200_123_456L;
        long ltzInstantMicros = 1_705_283_400_123_456L;

        try (RootAllocator allocator = new RootAllocator()) {
            PaimonArrowBatchAdapter adapter = new PaimonArrowBatchAdapter(
                    inputType, ZoneId.of("Asia/Shanghai"), allocator);
            try (ArrowStreamReader reader = schemaReader(adapter, allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                root.allocateNew();
                ((TimeStampVector) root.getVector("ntz")).setSafe(0, ntzMicros);
                ((TimeStampVector) root.getVector("ltz")).setSafe(0, ltzInstantMicros);
                ((TimeStampVector) root.getVector("ntz9")).setSafe(0, ntzMicros);
                root.setRowCount(1);

                InternalRow row = adapter.rows(root).row(0);
                Assertions.assertEquals(ntzMicros, row.getTimestamp(0, 6).toMicros());
                Assertions.assertEquals(ltzInstantMicros, row.getTimestamp(1, 6).toMicros());
                Assertions.assertEquals(ntzMicros, row.getTimestamp(2, 9).toMicros());
            }
        }
    }

    @Test
    public void testVariantUsesPaimonValueMetadataColumnarView() throws Exception {
        RowType inputType = DataTypes.ROW(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));
        GenericVariant expected = GenericVariant.fromJson(
                "{\"id\":1,\"nested\":[true,null,\"doris\"]}");

        try (RootAllocator allocator = new RootAllocator()) {
            PaimonArrowBatchAdapter adapter = new PaimonArrowBatchAdapter(
                    inputType, ZoneId.of("UTC"), allocator);
            try (ArrowStreamReader reader = schemaReader(adapter, allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                StructVector vector = (StructVector) root.getVector("payload");
                VarBinaryVector values = (VarBinaryVector) vector.getChild(Variant.VALUE);
                VarBinaryVector metadata = (VarBinaryVector) vector.getChild(Variant.METADATA);
                root.allocateNew();
                values.setSafe(0, expected.value());
                metadata.setSafe(0, expected.metadata());
                vector.setIndexDefined(0);
                vector.setNull(1);
                root.setRowCount(2);

                PaimonArrowBatchAdapter.Rows rows = adapter.rows(root);
                Variant actual = rows.row(0).getVariant(0);
                Assertions.assertArrayEquals(expected.value(), actual.value());
                Assertions.assertArrayEquals(expected.metadata(), actual.metadata());
                Assertions.assertTrue(rows.row(1).isNullAt(0));
            }
        }
    }

    @Test
    public void testRequiredVariantKeepsInputValidityForSdkValidation() throws Exception {
        RowType tableType = DataTypes.ROW(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT().notNull()));
        PaimonWriteSchema writeSchema = PaimonWriteSchema.create(
                tableType, new String[] {"payload"});

        try (RootAllocator allocator = new RootAllocator()) {
            PaimonArrowBatchAdapter adapter = new PaimonArrowBatchAdapter(
                    writeSchema.inputType(), ZoneId.of("UTC"), allocator);
            try (ArrowStreamReader reader = schemaReader(adapter, allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                Assertions.assertTrue(root.getSchema().findField("payload").isNullable());
                root.allocateNew();
                ((StructVector) root.getVector("payload")).setNull(0);
                root.setRowCount(1);
                Assertions.assertTrue(adapter.rows(root).row(0).isNullAt(0));
            }
        }
    }

    @Test
    public void testUnexpectedArrowColumnCountIsRejectedBeforeRowsAreWritten() throws Exception {
        RowType inputType = DataTypes.ROW(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));
        Field legacyVariant = new Field(
                "payload", FieldType.nullable(new ArrowType.Utf8()), null);
        Field unexpected = new Field(
                "unexpected", FieldType.nullable(new ArrowType.Utf8()), null);

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(
                        new Schema(Arrays.asList(legacyVariant, unexpected)), allocator)) {
            PaimonArrowBatchAdapter adapter = new PaimonArrowBatchAdapter(
                    inputType, ZoneId.of("UTC"), allocator);
            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> adapter.rows(root));
            Assertions.assertTrue(exception.getMessage().contains("column count mismatch"));
        }
    }

    private static ArrowStreamReader schemaReader(
            PaimonArrowBatchAdapter adapter, RootAllocator allocator) throws Exception {
        return new ArrowStreamReader(
                new ByteArrayInputStream(adapter.serializedArrowSchema()), allocator);
    }
}
