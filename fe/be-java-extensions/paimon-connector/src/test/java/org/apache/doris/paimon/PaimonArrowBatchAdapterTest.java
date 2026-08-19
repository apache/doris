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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.time.ZoneId;
import java.util.Collections;

public class PaimonArrowBatchAdapterTest {

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
    public void testUnexpectedArrowSchemaIsRejectedBeforeRowsAreWritten() throws Exception {
        RowType inputType = DataTypes.ROW(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));
        Field legacyVariant = new Field(
                "payload", FieldType.nullable(new ArrowType.Utf8()), null);

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(
                        new Schema(Collections.singletonList(legacyVariant)), allocator)) {
            PaimonArrowBatchAdapter adapter = new PaimonArrowBatchAdapter(
                    inputType, ZoneId.of("UTC"), allocator);
            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> adapter.rows(root));
            Assertions.assertTrue(exception.getMessage().contains("schema mismatch"));
        }
    }

    private static ArrowStreamReader schemaReader(
            PaimonArrowBatchAdapter adapter, RootAllocator allocator) throws Exception {
        return new ArrowStreamReader(
                new ByteArrayInputStream(adapter.serializedArrowSchema()), allocator);
    }
}
