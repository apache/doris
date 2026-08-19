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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

/** Converts Arrow columns into Paimon internal values without owning writer state. */
final class PaimonArrowConverter {

    static final String VARIANT_VALUE_FIELD = "value";
    static final String VARIANT_METADATA_FIELD = "metadata";

    private final ZoneId sessionTimeZone;

    PaimonArrowConverter(ZoneId sessionTimeZone) {
        this.sessionTimeZone = sessionTimeZone;
    }

    RowReader rows(VectorSchemaRoot root, DataType[] targetTypes) {
        List<Field> fields = root.getSchema().getFields();
        List<FieldVector> vectors = root.getFieldVectors();
        if (fields.size() != targetTypes.length) {
            throw new IllegalArgumentException("Arrow column count does not match Paimon write type");
        }
        ValueReader[] readers = new ValueReader[vectors.size()];
        for (int column = 0; column < vectors.size(); column++) {
            try {
                readers[column] = bindReader(vectors.get(column), targetTypes[column]);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        "Failed to bind Arrow column '" + fields.get(column).getName()
                                + "' to Paimon " + targetTypes[column],
                        e);
            }
        }
        return new RowReader(fields, readers);
    }

    @FunctionalInterface
    private interface ValueReader {
        Object read(int index);
    }

    /** Bound view of one Arrow batch which converts only the requested row. */
    final class RowReader {
        private final List<Field> fields;
        private final ValueReader[] readers;
        private final Object[] reusableValues;

        private RowReader(List<Field> fields, ValueReader[] readers) {
            this.fields = fields;
            this.readers = readers;
            this.reusableValues = new Object[readers.length];
        }

        Object[] values(int rowIndex) {
            for (int column = 0; column < readers.length; column++) {
                try {
                    reusableValues[column] = readers[column].read(rowIndex);
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(
                            "Failed to convert Arrow column '" + fields.get(column).getName()
                                    + "' at row " + rowIndex,
                            e);
                }
            }
            return reusableValues;
        }
    }

    private ValueReader bindReader(FieldVector vector, DataType targetType) {
        if (targetType instanceof VariantType) {
            return bindVariantReader(requireVector(vector, StructVector.class, targetType));
        }
        if (vector instanceof StructVector && targetType instanceof RowType) {
            return bindStructReader((StructVector) vector, (RowType) targetType);
        }
        if (vector instanceof MapVector && targetType instanceof MapType) {
            return bindMapReader((MapVector) vector, (MapType) targetType);
        }
        if (vector instanceof ListVector && targetType instanceof ArrayType) {
            return bindArrayReader((ListVector) vector, (ArrayType) targetType);
        }
        if (vector instanceof IntVector && targetType instanceof IntType) {
            IntVector typed = (IntVector) vector;
            return index -> typed.isNull(index) ? null : typed.get(index);
        }
        if (vector instanceof BigIntVector && targetType instanceof BigIntType) {
            BigIntVector typed = (BigIntVector) vector;
            return index -> typed.isNull(index) ? null : typed.get(index);
        }
        if (vector instanceof SmallIntVector && targetType instanceof SmallIntType) {
            SmallIntVector typed = (SmallIntVector) vector;
            return index -> typed.isNull(index) ? null : typed.get(index);
        }
        if (vector instanceof TinyIntVector && targetType instanceof TinyIntType) {
            TinyIntVector typed = (TinyIntVector) vector;
            return index -> typed.isNull(index) ? null : typed.get(index);
        }
        if (vector instanceof Float4Vector && targetType instanceof FloatType) {
            Float4Vector typed = (Float4Vector) vector;
            return index -> typed.isNull(index) ? null : typed.get(index);
        }
        if (vector instanceof Float8Vector && targetType instanceof DoubleType) {
            Float8Vector typed = (Float8Vector) vector;
            return index -> typed.isNull(index) ? null : typed.get(index);
        }
        if (vector instanceof BitVector && targetType instanceof BooleanType) {
            BitVector typed = (BitVector) vector;
            return index -> typed.isNull(index) ? null : typed.get(index) == 1;
        }
        if (vector instanceof DateDayVector && targetType instanceof DateType) {
            DateDayVector typed = (DateDayVector) vector;
            return index -> typed.isNull(index) ? null : typed.get(index);
        }
        if (vector instanceof VarCharVector
                && (targetType instanceof CharType || targetType instanceof VarCharType)) {
            VarCharVector typed = (VarCharVector) vector;
            return index -> typed.isNull(index) ? null : BinaryString.fromBytes(typed.get(index));
        }
        if (vector instanceof VarBinaryVector
                && (targetType instanceof BinaryType || targetType instanceof VarBinaryType)) {
            VarBinaryVector typed = (VarBinaryVector) vector;
            return index -> typed.isNull(index) ? null : typed.get(index);
        }
        if (vector instanceof TimeStampVector
                && (targetType instanceof TimestampType
                        || targetType instanceof LocalZonedTimestampType)) {
            return bindTimestampReader((TimeStampVector) vector, targetType);
        }
        if (vector instanceof DecimalVector && targetType instanceof DecimalType) {
            DecimalVector decimalVector = (DecimalVector) vector;
            DecimalType decimalType = (DecimalType) targetType;
            int precision = decimalVector.getPrecision();
            int scale = decimalVector.getScale();
            if (precision != decimalType.getPrecision() || scale != decimalType.getScale()) {
                throw unsupportedBinding(vector, targetType);
            }
            return index -> {
                if (decimalVector.isNull(index)) {
                    return null;
                }
                BigDecimal decimal = getBigDecimalFromArrowBuf(
                        decimalVector.getDataBuffer(), index, scale, DecimalVector.TYPE_WIDTH);
                return Decimal.fromBigDecimal(decimal, precision, scale);
            };
        }
        throw unsupportedBinding(vector, targetType);
    }

    private ValueReader bindTimestampReader(TimeStampVector vector, DataType targetType) {
        ArrowType.Timestamp arrowType = (ArrowType.Timestamp) vector.getField().getType();
        validateTimestampBinding(arrowType, targetType);
        LongUnaryOperator toMicros = bindArrowTimestampToMicros(arrowType);
        LongFunction<Timestamp> materialize = bindTimestampMaterializer(targetType);
        return index -> vector.isNull(index)
                ? null
                : materialize.apply(toMicros.applyAsLong(vector.get(index)));
    }

    private static <T extends FieldVector> T requireVector(
            FieldVector vector, Class<T> vectorClass, DataType targetType) {
        if (!vectorClass.isInstance(vector)) {
            throw unsupportedBinding(vector, targetType);
        }
        return vectorClass.cast(vector);
    }

    private static IllegalArgumentException unsupportedBinding(
            FieldVector vector, DataType targetType) {
        return new IllegalArgumentException(
                "No Doris-Paimon Arrow binding for " + vector.getField() + " -> " + targetType);
    }

    private ValueReader bindVariantReader(StructVector vector) {
        List<FieldVector> children = vector.getChildrenFromFields();
        if (children.size() != 2
                || !VARIANT_VALUE_FIELD.equals(children.get(0).getName())
                || !VARIANT_METADATA_FIELD.equals(children.get(1).getName())
                || !(children.get(0) instanceof VarBinaryVector)
                || !(children.get(1) instanceof VarBinaryVector)) {
            throw new IllegalArgumentException(
                    "Paimon VARIANT binary transport requires Arrow "
                            + "struct<value: binary, metadata: binary>, but got "
                            + vector.getField());
        }
        VarBinaryVector valueVector = (VarBinaryVector) children.get(0);
        VarBinaryVector metadataVector = (VarBinaryVector) children.get(1);
        return index -> {
            if (vector.isNull(index)) {
                return null;
            }
            if (valueVector.isNull(index) || metadataVector.isNull(index)) {
                throw new IllegalArgumentException(
                        "A non-null Paimon VARIANT struct requires non-null value and metadata");
            }
            // BE validates the complete Variant value for Paimon before Arrow transport.
            return new GenericVariant(valueVector.get(index), metadataVector.get(index));
        };
    }

    private ValueReader bindStructReader(StructVector vector, RowType rowType) {
        List<DataField> childFields = rowType.getFields();
        List<FieldVector> childVectors = vector.getChildrenFromFields();
        validateStructVectors(childFields, childVectors);
        ValueReader[] childReaders = new ValueReader[childFields.size()];
        for (int i = 0; i < childFields.size(); i++) {
            childReaders[i] = bindReader(childVectors.get(i), childFields.get(i).type());
        }
        return index -> {
            if (vector.isNull(index)) {
                return null;
            }
            GenericRow row = new GenericRow(childReaders.length);
            for (int i = 0; i < childReaders.length; i++) {
                row.setField(i, childReaders[i].read(index));
            }
            return row;
        };
    }

    private static void validateStructVectors(
            List<DataField> childFields, List<FieldVector> childVectors) {
        if (childVectors.size() != childFields.size()) {
            throw structFieldCountMismatch(childVectors.size(), childFields.size());
        }
        for (int i = 0; i < childFields.size(); i++) {
            validateStructField(i, childFields.get(i), childVectors.get(i).getName());
        }
    }

    static void validateStructSchema(RowType rowType, List<String> arrowFieldNames) {
        List<DataField> childFields = rowType.getFields();
        if (arrowFieldNames.size() != childFields.size()) {
            throw structFieldCountMismatch(arrowFieldNames.size(), childFields.size());
        }
        for (int i = 0; i < childFields.size(); i++) {
            validateStructField(i, childFields.get(i), arrowFieldNames.get(i));
        }
    }

    private static IllegalArgumentException structFieldCountMismatch(
            int arrowFieldCount, int paimonFieldCount) {
        return new IllegalArgumentException(
                "Arrow struct field count does not match Paimon row type: arrow="
                        + arrowFieldCount + ", paimon=" + paimonFieldCount);
    }

    private static void validateStructField(
            int position, DataField paimonField, String arrowFieldName) {
        if (!arrowFieldName.equalsIgnoreCase(paimonField.name())) {
            throw new IllegalArgumentException(
                    "Arrow struct field at position " + position + " does not match Paimon field "
                            + paimonField.name() + ": " + arrowFieldName);
        }
    }

    private ValueReader bindMapReader(MapVector vector, MapType mapType) {
        StructVector entries = (StructVector) vector.getDataVector();
        List<FieldVector> entryVectors = entries.getChildrenFromFields();
        if (entryVectors.size() != 2) {
            throw new IllegalArgumentException("Arrow map must contain exactly key and value vectors");
        }
        ValueReader keyReader = bindReader(entryVectors.get(0), mapType.getKeyType());
        ValueReader valueReader = bindReader(entryVectors.get(1), mapType.getValueType());
        return index -> {
            if (vector.isNull(index)) {
                return null;
            }
            int start = vector.getElementStartIndex(index);
            int end = vector.getElementEndIndex(index);
            Map<Object, Object> converted = new HashMap<>();
            for (int entryIndex = start; entryIndex < end; entryIndex++) {
                converted.put(keyReader.read(entryIndex), valueReader.read(entryIndex));
            }
            return new GenericMap(converted);
        };
    }

    private ValueReader bindArrayReader(ListVector vector, ArrayType arrayType) {
        FieldVector elementVector = vector.getDataVector();
        ValueReader elementReader = bindReader(elementVector, arrayType.getElementType());
        return index -> {
            if (vector.isNull(index)) {
                return null;
            }
            int start = vector.getElementStartIndex(index);
            int end = vector.getElementEndIndex(index);
            Object[] converted = new Object[end - start];
            for (int elementIndex = start; elementIndex < end; elementIndex++) {
                converted[elementIndex - start] = elementReader.read(elementIndex);
            }
            return new GenericArray(converted);
        };
    }

    private void validateTimestampBinding(ArrowType.Timestamp arrowType, DataType targetType) {
        requireTimezoneFree(arrowType);
        bindArrowTimestampToMicros(arrowType);
        bindTimestampMaterializer(targetType);
        int precision = targetType instanceof TimestampType
                ? ((TimestampType) targetType).getPrecision()
                : ((LocalZonedTimestampType) targetType).getPrecision();
        org.apache.arrow.vector.types.TimeUnit expectedUnit = precision > 3
                ? org.apache.arrow.vector.types.TimeUnit.MICROSECOND
                : precision > 0
                        ? org.apache.arrow.vector.types.TimeUnit.MILLISECOND
                        : org.apache.arrow.vector.types.TimeUnit.SECOND;
        if (arrowType.getUnit() != expectedUnit) {
            throw new IllegalArgumentException(
                    "Arrow timestamp unit " + arrowType.getUnit()
                            + " does not match Paimon precision " + precision);
        }
    }

    private static LongUnaryOperator bindArrowTimestampToMicros(ArrowType.Timestamp timestampType) {
        switch (timestampType.getUnit()) {
            case SECOND:
                return value -> Math.multiplyExact(value, 1_000_000L);
            case MILLISECOND:
                return value -> Math.multiplyExact(value, 1_000L);
            case MICROSECOND:
                return value -> value;
            case NANOSECOND:
                return value -> Math.floorDiv(value, 1_000L);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Arrow timestamp unit: " + timestampType.getUnit());
        }
    }

    private LongFunction<Timestamp> bindTimestampMaterializer(DataType targetType) {
        if (targetType instanceof LocalZonedTimestampType) {
            return micros -> Timestamp.fromInstant(
                    microsToCivilTime(micros).atZone(sessionTimeZone).toInstant());
        }
        if (targetType instanceof TimestampType) {
            return micros -> Timestamp.fromLocalDateTime(microsToCivilTime(micros));
        }
        throw new IllegalArgumentException(
                "Arrow timestamp cannot be written to Paimon type " + targetType);
    }

    private static void requireTimezoneFree(ArrowType.Timestamp arrowType) {
        String arrowTimeZone = arrowType.getTimezone();
        if (arrowTimeZone != null && !arrowTimeZone.isEmpty()) {
            throw new IllegalArgumentException(
                    "Paimon write timestamp must use a timezone-free Arrow type");
        }
    }

    private static LocalDateTime microsToCivilTime(long micros) {
        long epochSecond = Math.floorDiv(micros, 1_000_000L);
        long microsOfSecond = Math.floorMod(micros, 1_000_000L);
        return LocalDateTime.ofEpochSecond(
                epochSecond, (int) microsOfSecond * 1_000, ZoneOffset.UTC);
    }

    private static BigDecimal getBigDecimalFromArrowBuf(
            org.apache.arrow.memory.ArrowBuf buffer, int index, int scale, int byteWidth) {
        byte[] value = new byte[byteWidth];
        buffer.getBytes((long) index * byteWidth, value, 0, byteWidth);
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
            for (int i = 0; i < byteWidth / 2; i++) {
                byte temporary = value[i];
                int opposite = byteWidth - 1 - i;
                value[i] = value[opposite];
                value[opposite] = temporary;
            }
        }
        return new BigDecimal(new BigInteger(value), scale);
    }

    Timestamp toPaimonTimestamp(long micros, ArrowType.Timestamp arrowType,
                                DataType targetType) {
        requireTimezoneFree(arrowType);
        return bindTimestampMaterializer(targetType).apply(micros);
    }

    Timestamp toPaimonTimestamp(LocalDateTime civilTime, DataType targetType) {
        if (targetType instanceof LocalZonedTimestampType) {
            return Timestamp.fromInstant(civilTime.atZone(sessionTimeZone).toInstant());
        }
        if (targetType instanceof TimestampType) {
            return Timestamp.fromLocalDateTime(civilTime);
        }
        throw new IllegalArgumentException(
                "Arrow timestamp cannot be written to Paimon type " + targetType);
    }
}
