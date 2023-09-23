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
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * MaxCompute Column value in vector column
 */
public class MaxComputeColumnValue implements ColumnValue {
    private static final Logger LOG = Logger.getLogger(MaxComputeColumnValue.class);
    private int idx;
    private FieldVector column;

    public MaxComputeColumnValue() {
        idx = 0;
    }

    public void reset(FieldVector column) {
        this.column = column;
        this.idx = 0;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean isNull() {
        return column.isNull(idx);
    }

    private void skippedIfNull() {
        // null has been process by appendValue with isNull()
        try {
            if (column.isNull(idx)) {
                idx++;
            }
        } catch (IndexOutOfBoundsException e) {
            // skip left rows
            idx++;
        }
    }

    @Override
    public boolean getBoolean() {
        skippedIfNull();
        BitVector bitCol = (BitVector) column;
        return bitCol.get(idx++) != 0;
    }

    @Override
    public byte getByte() {
        skippedIfNull();
        TinyIntVector tinyIntCol = (TinyIntVector) column;
        return tinyIntCol.get(idx++);
    }

    @Override
    public short getShort() {
        skippedIfNull();
        SmallIntVector smallIntCol = (SmallIntVector) column;
        return smallIntCol.get(idx++);
    }

    @Override
    public int getInt() {
        skippedIfNull();
        IntVector intCol = (IntVector) column;
        return intCol.get(idx++);
    }

    @Override
    public float getFloat() {
        skippedIfNull();
        Float4Vector floatCol = (Float4Vector) column;
        return floatCol.get(idx++);
    }

    @Override
    public long getLong() {
        skippedIfNull();
        BigIntVector longCol = (BigIntVector) column;
        return longCol.get(idx++);
    }

    @Override
    public double getDouble() {
        skippedIfNull();
        Float8Vector doubleCol = (Float8Vector) column;
        return doubleCol.get(idx++);
    }

    @Override
    public BigInteger getBigInteger() {
        skippedIfNull();
        BigIntVector longCol = (BigIntVector) column;
        return BigInteger.valueOf(longCol.get(idx++));
    }

    @Override
    public BigDecimal getDecimal() {
        skippedIfNull();
        DecimalVector decimalCol = (DecimalVector) column;
        return getBigDecimalFromArrowBuf(column.getDataBuffer(), idx++,
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
        skippedIfNull();
        VarCharVector varcharCol = (VarCharVector) column;
        String v = varcharCol.getObject(idx++).toString();
        return v == null ? new String(new byte[0]) : v;
    }

    @Override
    public byte[] getStringAsBytes() {
        skippedIfNull();
        VarCharVector varcharCol = (VarCharVector) column;
        byte[] v = varcharCol.getObject(idx++).getBytes();
        return v == null ? new byte[0] : v;
    }

    @Override
    public LocalDate getDate() {
        skippedIfNull();
        DateDayVector dateCol = (DateDayVector) column;
        Integer intVal = dateCol.getObject(idx++);
        return LocalDate.ofEpochDay(intVal == null ? 0 : intVal);
    }

    @Override
    public LocalDateTime getDateTime() {
        skippedIfNull();
        LocalDateTime result;
        if (column instanceof DateMilliVector) {
            DateMilliVector datetimeCol = (DateMilliVector) column;
            result = datetimeCol.getObject(idx++);
        } else {
            TimeStampNanoVector datetimeCol = (TimeStampNanoVector) column;
            result = datetimeCol.getObject(idx++);
        }
        return result == null ? LocalDateTime.MIN : result;
    }

    @Override
    public byte[] getBytes() {
        skippedIfNull();
        VarBinaryVector binaryCol = (VarBinaryVector) column;
        byte[] v = binaryCol.getObject(idx++);
        return v == null ? new byte[0] : v;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {

    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {

    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {

    }
}
