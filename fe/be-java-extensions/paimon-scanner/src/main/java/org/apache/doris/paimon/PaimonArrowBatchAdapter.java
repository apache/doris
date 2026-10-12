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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.paimon.arrow.ArrowFieldTypeConversion;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.converter.Arrow2PaimonVectorConverter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.TimestampColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VariantType;

import java.io.ByteArrayOutputStream;
import java.time.ZoneId;
import java.util.List;

/** Adapts the strict Paimon Arrow transport to Paimon's columnar row view. */
final class PaimonArrowBatchAdapter {

    private static final WriteVectorVisitor VECTOR_VISITOR = new WriteVectorVisitor();

    private final RowType inputType;
    private final Arrow2PaimonVectorConverter[] vectorAdapters;
    private final byte[] serializedArrowSchema;

    PaimonArrowBatchAdapter(RowType inputType, ZoneId sessionTimeZone, BufferAllocator allocator)
            throws Exception {
        this.inputType = inputType;
        this.vectorAdapters = new Arrow2PaimonVectorConverter[inputType.getFieldCount()];
        for (int i = 0; i < vectorAdapters.length; i++) {
            vectorAdapters[i] = Arrow2PaimonVectorConverter.construct(
                    VECTOR_VISITOR, inputType.getTypeAt(i));
        }

        try (VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(
                     inputType, allocator, true, new WriteFieldVisitor(sessionTimeZone));
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
            writer.start();
            writer.end();
            this.serializedArrowSchema = output.toByteArray();
        }
    }

    byte[] serializedArrowSchema() {
        return serializedArrowSchema;
    }

    Rows rows(VectorSchemaRoot root) {
        List<FieldVector> fieldVectors = root.getFieldVectors();
        if (fieldVectors.size() != vectorAdapters.length) {
            throw new IllegalArgumentException(
                    "Paimon Arrow column count mismatch: expected " + vectorAdapters.length
                            + ", actual " + fieldVectors.size());
        }

        ColumnVector[] columns = new ColumnVector[fieldVectors.size()];
        for (int i = 0; i < columns.length; i++) {
            try {
                columns[i] = vectorAdapters[i].convertVector(fieldVectors.get(i));
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        "Failed to adapt Arrow field '" + fieldVectors.get(i).getName()
                                + "' to Paimon " + inputType.getTypeAt(i),
                        e);
            }
        }

        VectorizedColumnBatch batch = new VectorizedColumnBatch(columns);
        batch.setNumRows(root.getRowCount());
        return new Rows(batch);
    }

    /** Reusable row cursor over one Arrow-backed Paimon columnar batch. */
    static final class Rows {
        private final ColumnarRow row;

        private Rows(VectorizedColumnBatch batch) {
            this.row = new ColumnarRow(batch);
        }

        InternalRow row(int rowIndex) {
            row.setRowId(rowIndex);
            return row;
        }
    }

    /**
     * Paimon 1.4.2 already adapts all regular Arrow vectors. Variant is represented by the same
     * value/metadata row used by Paimon's own Arrow schema, but its stock converter does not yet
     * bind VariantType, so bind that structural view here.
     */
    private static final class WriteVectorVisitor extends
            Arrow2PaimonVectorConverter.Arrow2PaimonVectorConvertorVisitor {
        @Override
        public Arrow2PaimonVectorConverter visit(VariantType variantType) {
            RowType binaryVariant = DataTypes.ROW(
                    DataTypes.FIELD(0, Variant.VALUE, DataTypes.BYTES().notNull()),
                    DataTypes.FIELD(1, Variant.METADATA, DataTypes.BYTES().notNull()));
            return visit(binaryVariant);
        }

        @Override
        public Arrow2PaimonVectorConverter visit(TimestampType timestampType) {
            return timestampAdapter();
        }

        @Override
        public Arrow2PaimonVectorConverter visit(
                LocalZonedTimestampType localZonedTimestampType) {
            return timestampAdapter();
        }

        private static Arrow2PaimonVectorConverter timestampAdapter() {
            return vector -> {
                TimeStampVector timestamps = (TimeStampVector) vector;
                TimeUnit unit = ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
                return new TimestampColumnVector() {
                    @Override
                    public boolean isNullAt(int index) {
                        return timestamps.isNull(index);
                    }

                    @Override
                    public Timestamp getTimestamp(int index, int precision) {
                        long value = timestamps.get(index);
                        switch (unit) {
                            case SECOND:
                                return Timestamp.fromEpochMillis(Math.multiplyExact(value, 1_000L));
                            case MILLISECOND:
                                return Timestamp.fromEpochMillis(value);
                            case MICROSECOND:
                                return Timestamp.fromMicros(value);
                            case NANOSECOND:
                                return Timestamp.fromEpochMillis(
                                        Math.floorDiv(value, 1_000_000L),
                                        (int) Math.floorMod(value, 1_000_000L));
                            default:
                                throw new IllegalArgumentException(
                                        "Unsupported Paimon Arrow timestamp unit: " + unit);
                        }
                    }
                };
            };
        }
    }

    /**
     * A non-empty Arrow timezone marks a Paimon LTZ target. BE uses the Doris session timezone to
     * encode civil values as instants; Java only exposes the resulting integer to Paimon.
     */
    private static final class WriteFieldVisitor extends
            ArrowFieldTypeConversion.ArrowFieldTypeVisitor {
        private final String sessionTimeZone;

        private WriteFieldVisitor(ZoneId sessionTimeZone) {
            this.sessionTimeZone = sessionTimeZone.getId();
        }

        @Override
        public FieldType visit(TimestampType timestampType) {
            return timestampField(timestampType.isNullable(), timestampType.getPrecision(), null);
        }

        @Override
        public FieldType visit(LocalZonedTimestampType localZonedTimestampType) {
            return timestampField(localZonedTimestampType.isNullable(),
                    localZonedTimestampType.getPrecision(), sessionTimeZone);
        }

        private static FieldType timestampField(
                boolean nullable, int precision, String timeZone) {
            TimeUnit unit = precision > 3
                    ? TimeUnit.MICROSECOND
                    : precision > 0 ? TimeUnit.MILLISECOND : TimeUnit.SECOND;
            return new FieldType(nullable, new ArrowType.Timestamp(unit, timeZone), null);
        }
    }
}
