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

package org.apache.doris.maxcompute;

import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableTimeStampNanoHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

/**
 * MaxCompute Column value in vector column
 */
public class MaxComputeColumnValue implements ColumnValue {
    private static final Logger LOG = Logger.getLogger(MaxComputeColumnValue.class);
    private int idx;
    private ValueVector column;
    private ZoneId timeZone;

    public MaxComputeColumnValue() {
        idx = 0;
    }

    public void setColumnIdx(int idx) {
        this.idx = idx;
    }

    public MaxComputeColumnValue(ValueVector valueVector, int i) {
        this.column = valueVector;
        this.idx = i;
    }

    public MaxComputeColumnValue(ValueVector valueVector, int i, ZoneId timeZone) {
        this.column = valueVector;
        this.idx = i;
        this.timeZone = timeZone;
    }

    public void reset(ValueVector column) {
        this.column = column;
        this.idx = 0;
    }

    public void setTimeZone(ZoneId timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean isNull() {
        return column.isNull(idx);
    }

    @Override
    public boolean getBoolean() {
        BitVector bitCol = (BitVector) column;
        return bitCol.get(idx) != 0;
    }

    @Override
    public byte getByte() {
        TinyIntVector tinyIntCol = (TinyIntVector) column;
        return tinyIntCol.get(idx);
    }

    @Override
    public short getShort() {
        SmallIntVector smallIntCol = (SmallIntVector) column;
        return smallIntCol.get(idx);
    }

    @Override
    public int getInt() {
        IntVector intCol = (IntVector) column;
        return intCol.get(idx);
    }

    @Override
    public float getFloat() {
        Float4Vector floatCol = (Float4Vector) column;
        return floatCol.get(idx);
    }

    @Override
    public long getLong() {
        BigIntVector longCol = (BigIntVector) column;
        return longCol.get(idx);
    }

    @Override
    public double getDouble() {
        Float8Vector doubleCol = (Float8Vector) column;
        return doubleCol.get(idx);
    }

    @Override
    public BigInteger getBigInteger() {
        BigIntVector longCol = (BigIntVector) column;
        return BigInteger.valueOf(longCol.get(idx));
    }

    @Override
    public BigDecimal getDecimal() {
        DecimalVector decimalCol = (DecimalVector) column;
        return getBigDecimalFromArrowBuf(column.getDataBuffer(), idx,
                    decimalCol.getScale(), DecimalVector.TYPE_WIDTH);
    }

    /**
     * copy from arrow vector DecimalUtility.getBigDecimalFromArrowBuf
     * @param byteBuf byteBuf
     * @param index index
     * @param scale scale
     * @param byteWidth DecimalVector TYPE_WIDTH
     * @return java BigDecimal
     */
    public static BigDecimal getBigDecimalFromArrowBuf(ArrowBuf byteBuf, int index, int scale, int byteWidth) {
        byte[] value = new byte[byteWidth];
        byte temp;
        final long startIndex = (long) index * byteWidth;

        byteBuf.getBytes(startIndex, value, 0, byteWidth);
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
            // Decimal stored as native endian, need to swap bytes to make BigDecimal if native endian is LE
            int stop = byteWidth / 2;
            for (int i = 0, j; i < stop; i++) {
                temp = value[i];
                j = (byteWidth - 1) - i;
                value[i] = value[j];
                value[j] = temp;
            }
        }
        BigInteger unscaledValue = new BigInteger(value);
        return new BigDecimal(unscaledValue, scale);
    }

    @Override
    public String getString() {
        VarCharVector varcharCol = (VarCharVector) column;
        String v = varcharCol.getObject(idx).toString();
        return v == null ? new String(new byte[0]) : v;
    }



    public String getChar() {
        VarCharVector varcharCol = (VarCharVector) column;
        return varcharCol.getObject(idx).toString().stripTrailing();
    }

    // Maybe I can use `appendBytesAndOffset(byte[] src, int offset, int length)` to reduce the creation of byte[].
    // But I haven't figured out how to write it elegantly.
    public byte[] getCharAsBytes() {
        VarCharVector varcharCol = (VarCharVector) column;
        byte[] v = varcharCol.getObject(idx).getBytes();

        if (v == null) {
            return new byte[0];
        }

        int end = v.length - 1;
        while (end >= 0 && v[end] == ' ') {
            end--;
        }
        return (end == -1) ? new byte[0] : Arrays.copyOfRange(v, 0, end + 1);
    }


    @Override
    public byte[] getStringAsBytes() {
        VarCharVector varcharCol = (VarCharVector) column;
        byte[] v = varcharCol.getObject(idx).getBytes();
        return v == null ? new byte[0] : v;
    }

    @Override
    public LocalDate getDate() {
        DateDayVector dateCol = (DateDayVector) column;
        Integer intVal = dateCol.getObject(idx);
        return LocalDate.ofEpochDay(intVal == null ? 0 : intVal);
    }

    @Override
    public LocalDateTime getDateTime() {
        LocalDateTime result;

        ArrowType.Timestamp timestampType = ( ArrowType.Timestamp) column.getField().getFieldType().getType();
        if (timestampType.getUnit() ==  org.apache.arrow.vector.types.TimeUnit.MILLISECOND) {
            result = convertToLocalDateTime((TimeStampMilliTZVector) column, idx);
        } else {
            NullableTimeStampNanoHolder valueHoder = new NullableTimeStampNanoHolder();
            ((TimeStampNanoVector) column).get(idx, valueHoder);
            long timestampNanos = valueHoder.value;

            result = LocalDateTime.ofEpochSecond(timestampNanos / 1_000_000_000,
                    (int) (timestampNanos % 1_000_000_000), java.time.ZoneOffset.UTC);
        }

        /*
        timestampType.getUnit()
        result = switch (timestampType.getUnit()) {
            case MICROSECOND -> convertToLocalDateTime((TimeStampMicroTZVector) column, idx);
            case SECOND -> convertToLocalDateTime((TimeStampSecTZVector) column, idx);
            case MILLISECOND -> convertToLocalDateTime((TimeStampMilliTZVector) column, idx);
            case NANOSECOND -> convertToLocalDateTime((TimeStampNanoTZVector) column, idx);
        };

        Because :
        MaxCompute type    => Doris Type
        DATETIME  => ScalarType.createDatetimeV2Type(3)
        TIMESTAMP_NTZ => ScalarType.createDatetimeV2Type(6);

        and
        TableBatchReadSession
            .withArrowOptions (
                ArrowOptions.newBuilder()
                .withDatetimeUnit(TimestampUnit.MILLI)
                .withTimestampUnit(TimestampUnit.NANO)
                .build()
            )
        ,
        TIMESTAMP_NTZ is NTZ  => column is  TimeStampNanoVector

        So:
            case SECOND -> convertToLocalDateTime((TimeStampSecTZVector) column, idx);
            case MICROSECOND -> convertToLocalDateTime((TimeStampMicroTZVector) column, idx);
            case NANOSECOND -> convertToLocalDateTime((TimeStampNanoTZVector) column, idx);
            may never be used.
        */

        return result;
    }

    @Override
    public byte[] getBytes() {
        VarBinaryVector binaryCol = (VarBinaryVector) column;
        byte[] v = binaryCol.getObject(idx);
        return v == null ? new byte[0] : v;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        ListVector listCol = (ListVector) column;
        int elemSize = listCol.getElementEndIndex(idx) - listCol.getElementStartIndex(idx);
        int offset = listCol.getElementStartIndex(idx);
        for (int i = 0; i < elemSize; i++) {
            MaxComputeColumnValue val = new MaxComputeColumnValue(listCol.getDataVector(), offset, timeZone);
            values.add(val);
            offset++;
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapVector mapCol = (MapVector) column;
        int elemSize = mapCol.getElementEndIndex(idx) - mapCol.getElementStartIndex(idx);
        int offset = mapCol.getElementStartIndex(idx);
        List<FieldVector> innerCols = ((StructVector) mapCol.getDataVector()).getChildrenFromFields();
        FieldVector keyList = innerCols.get(0);
        FieldVector valList = innerCols.get(1);
        for (int i = 0; i < elemSize; i++) {
            MaxComputeColumnValue key = new MaxComputeColumnValue(keyList, offset, timeZone);
            keys.add(key);
            MaxComputeColumnValue val = new MaxComputeColumnValue(valList, offset, timeZone);
            values.add(val);
            offset++;
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        StructVector structCol = (StructVector) column;
        List<FieldVector> innerCols = structCol.getChildrenFromFields();
        for (Integer fieldIndex : structFieldIndex) {
            MaxComputeColumnValue val = new MaxComputeColumnValue(innerCols.get(fieldIndex), idx, timeZone);
            values.add(val);
        }
    }

    public LocalDateTime convertToLocalDateTime(TimeStampMilliTZVector milliTZVector, int index) {
        long timestampMillis = milliTZVector.get(index);
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestampMillis), timeZone);
    }

}
