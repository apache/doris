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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converts Arrow columns into Paimon internal values without owning writer state. */
final class PaimonArrowConverter {

    private final ZoneId sessionTimeZone;

    PaimonArrowConverter(ZoneId sessionTimeZone) {
        this.sessionTimeZone = sessionTimeZone;
    }

    Object[][] convert(VectorSchemaRoot root, DataType[] targetTypes) {
        List<Field> fields = root.getSchema().getFields();
        List<FieldVector> vectors = root.getFieldVectors();
        if (fields.size() != targetTypes.length) {
            throw new IllegalArgumentException("Arrow column count does not match Paimon write type");
        }

        Object[][] columnValues = new Object[fields.size()][];
        for (int column = 0; column < fields.size(); column++) {
            columnValues[column] = extractColumnValues(
                    vectors.get(column), fields.get(column), targetTypes[column], root.getRowCount());
        }
        return columnValues;
    }

    private Object[] extractColumnValues(FieldVector vector, Field arrowField,
                                         DataType targetType, int rowCount) {
        Object[] values = new Object[rowCount];

        if (vector instanceof IntVector) {
            IntVector intVector = (IntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                values[i] = intVector.isNull(i) ? null : intVector.get(i);
            }
            return values;
        }
        if (vector instanceof BigIntVector) {
            BigIntVector bigIntVector = (BigIntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                values[i] = bigIntVector.isNull(i) ? null : bigIntVector.get(i);
            }
            return values;
        }
        if (vector instanceof SmallIntVector) {
            SmallIntVector smallIntVector = (SmallIntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                values[i] = smallIntVector.isNull(i) ? null : smallIntVector.get(i);
            }
            return values;
        }
        if (vector instanceof TinyIntVector) {
            TinyIntVector tinyIntVector = (TinyIntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                values[i] = tinyIntVector.isNull(i) ? null : tinyIntVector.get(i);
            }
            return values;
        }
        if (vector instanceof Float4Vector) {
            Float4Vector floatVector = (Float4Vector) vector;
            for (int i = 0; i < rowCount; i++) {
                values[i] = floatVector.isNull(i) ? null : floatVector.get(i);
            }
            return values;
        }
        if (vector instanceof Float8Vector) {
            Float8Vector doubleVector = (Float8Vector) vector;
            for (int i = 0; i < rowCount; i++) {
                values[i] = doubleVector.isNull(i) ? null : doubleVector.get(i);
            }
            return values;
        }
        if (vector instanceof BitVector) {
            BitVector bitVector = (BitVector) vector;
            for (int i = 0; i < rowCount; i++) {
                values[i] = bitVector.isNull(i) ? null : bitVector.get(i) == 1;
            }
            return values;
        }
        if (vector instanceof DateDayVector) {
            DateDayVector dateVector = (DateDayVector) vector;
            for (int i = 0; i < rowCount; i++) {
                values[i] = dateVector.isNull(i) ? null : dateVector.get(i);
            }
            return values;
        }
        if (vector instanceof VarCharVector) {
            VarCharVector stringVector = (VarCharVector) vector;
            boolean binary = targetType instanceof BinaryType || targetType instanceof VarBinaryType;
            for (int i = 0; i < rowCount; i++) {
                values[i] = stringVector.isNull(i) ? null
                        : binary ? stringVector.get(i) : BinaryString.fromBytes(stringVector.get(i));
            }
            return values;
        }
        if (vector instanceof VarBinaryVector) {
            VarBinaryVector binaryVector = (VarBinaryVector) vector;
            for (int i = 0; i < rowCount; i++) {
                values[i] = binaryVector.isNull(i) ? null : binaryVector.get(i);
            }
            return values;
        }
        if (vector instanceof TimeStampVector) {
            TimeStampVector timestampVector = (TimeStampVector) vector;
            ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowField.getType();
            for (int i = 0; i < rowCount; i++) {
                values[i] = timestampVector.isNull(i) ? null : toPaimonTimestamp(
                        arrowTimestampToMicros(timestampVector.get(i), timestampType),
                        timestampType, targetType);
            }
            return values;
        }
        if (vector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) vector;
            int precision = decimalVector.getPrecision();
            int scale = decimalVector.getScale();
            for (int i = 0; i < rowCount; i++) {
                if (decimalVector.isNull(i)) {
                    values[i] = null;
                } else {
                    BigDecimal decimal = getBigDecimalFromArrowBuf(
                            decimalVector.getDataBuffer(), i, scale, DecimalVector.TYPE_WIDTH);
                    values[i] = Decimal.fromBigDecimal(decimal, precision, scale);
                }
            }
            return values;
        }

        for (int i = 0; i < rowCount; i++) {
            values[i] = vector.isNull(i) ? null
                    : convertToPaimonType(vector.getObject(i), arrowField, targetType);
        }
        return values;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object convertToPaimonType(Object value, Field arrowField, DataType targetType) {
        if (value == null) {
            return null;
        }
        if (targetType instanceof BinaryType || targetType instanceof VarBinaryType) {
            if (value instanceof byte[]) {
                return value;
            }
            if (value instanceof BinaryString) {
                return ((BinaryString) value).toBytes();
            }
            if (value instanceof org.apache.arrow.vector.util.Text) {
                return ((org.apache.arrow.vector.util.Text) value).copyBytes();
            }
            if (value instanceof String) {
                return ((String) value).getBytes(StandardCharsets.UTF_8);
            }
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }
        if (value instanceof BinaryString) {
            return value;
        }
        if (value instanceof byte[]) {
            return BinaryString.fromBytes((byte[]) value);
        }
        if (value instanceof org.apache.arrow.vector.util.Text) {
            return BinaryString.fromBytes(((org.apache.arrow.vector.util.Text) value).copyBytes());
        }
        if (value instanceof org.apache.hadoop.io.Text) {
            org.apache.hadoop.io.Text text = (org.apache.hadoop.io.Text) value;
            return BinaryString.fromBytes(text.getBytes(), 0, text.getLength());
        }
        if (value instanceof CharSequence) {
            return BinaryString.fromString(value.toString());
        }

        ArrowType.ArrowTypeID typeId = arrowField == null
                ? null : arrowField.getType().getTypeID();
        if (value instanceof LocalDateTime) {
            return toPaimonTimestamp((LocalDateTime) value, targetType);
        }
        if (value instanceof Long && typeId == ArrowType.ArrowTypeID.Timestamp) {
            ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowField.getType();
            return toPaimonTimestamp(
                    arrowTimestampToMicros((Long) value, timestampType), timestampType, targetType);
        }
        if (value instanceof Integer && typeId == ArrowType.ArrowTypeID.Date) {
            return value;
        }
        if (value instanceof java.time.LocalDate) {
            return (int) ((java.time.LocalDate) value).toEpochDay();
        }
        if (value instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value;
            return Decimal.fromBigDecimal(decimal, decimal.precision(), decimal.scale());
        }
        if (targetType instanceof RowType && value instanceof Map) {
            return convertStruct((Map<?, ?>) value, (RowType) targetType, arrowField);
        }
        if (targetType instanceof MapType && value instanceof List) {
            return convertMap((List<?>) value, (MapType) targetType, arrowField);
        }
        if (targetType instanceof ArrayType && value instanceof List) {
            return convertArray((List<?>) value, (ArrayType) targetType, arrowField);
        }
        return value;
    }

    private GenericRow convertStruct(Map<?, ?> mapValue, RowType rowType, Field arrowField) {
        List<DataField> childFields = rowType.getFields();
        GenericRow row = new GenericRow(childFields.size());
        for (int i = 0; i < childFields.size(); i++) {
            DataField childField = childFields.get(i);
            row.setField(i, convertToPaimonType(mapValue.get(childField.name()),
                    findChildField(arrowField, childField.name()), childField.type()));
        }
        return row;
    }

    private GenericMap convertMap(List<?> values, MapType mapType, Field arrowField) {
        Field keyField = null;
        Field valueField = null;
        if (arrowField != null && !arrowField.getChildren().isEmpty()) {
            Field entries = arrowField.getChildren().get(0);
            if (entries.getChildren().size() >= 2) {
                keyField = entries.getChildren().get(0);
                valueField = entries.getChildren().get(1);
            }
        }
        String keyName = keyField == null ? "key" : keyField.getName();
        String valueName = valueField == null ? "value" : valueField.getName();
        Map<Object, Object> converted = new HashMap<>();
        for (Object element : values) {
            if (element instanceof Map) {
                Map<?, ?> entry = (Map<?, ?>) element;
                converted.put(
                        convertToPaimonType(entry.get(keyName), keyField, mapType.getKeyType()),
                        convertToPaimonType(entry.get(valueName), valueField,
                                mapType.getValueType()));
            }
        }
        return new GenericMap(converted);
    }

    private GenericArray convertArray(List<?> values, ArrayType arrayType, Field arrowField) {
        Object[] converted = new Object[values.size()];
        Field elementField = arrowField == null || arrowField.getChildren().isEmpty()
                ? null : arrowField.getChildren().get(0);
        for (int i = 0; i < values.size(); i++) {
            converted[i] = convertToPaimonType(
                    values.get(i), elementField, arrayType.getElementType());
        }
        return new GenericArray(converted);
    }

    private static Field findChildField(Field parent, String name) {
        if (parent == null || parent.getChildren() == null) {
            return null;
        }
        for (Field child : parent.getChildren()) {
            if (child.getName().equals(name)) {
                return child;
            }
        }
        return null;
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

    private static long arrowTimestampToMicros(
            long value, ArrowType.Timestamp timestampType) {
        switch (timestampType.getUnit()) {
            case SECOND:
                return Math.multiplyExact(value, 1_000_000L);
            case MILLISECOND:
                return Math.multiplyExact(value, 1_000L);
            case MICROSECOND:
                return value;
            case NANOSECOND:
                return Math.floorDiv(value, 1_000L);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Arrow timestamp unit: " + timestampType.getUnit());
        }
    }

    Timestamp toPaimonTimestamp(long micros, ArrowType.Timestamp arrowType,
                                DataType targetType) {
        String arrowTimeZone = arrowType.getTimezone();
        if (arrowTimeZone != null && !arrowTimeZone.isEmpty()) {
            throw new IllegalArgumentException(
                    "Paimon write timestamp must use a timezone-free Arrow type");
        }
        long epochSecond = Math.floorDiv(micros, 1_000_000L);
        long microsOfSecond = Math.floorMod(micros, 1_000_000L);
        LocalDateTime civilTime = LocalDateTime.ofEpochSecond(
                epochSecond, (int) microsOfSecond * 1_000, ZoneOffset.UTC);
        return toPaimonTimestamp(civilTime, targetType);
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
