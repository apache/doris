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

package org.apache.doris.hudi;


import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.common.jni.vec.NativeColumnValue;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

public class HudiColumnValue implements ColumnValue, NativeColumnValue {
    private boolean isUnsafe;
    private InternalRow internalRow;
    private int ordinal;
    private int precision;
    private int scale;

    HudiColumnValue() {
    }

    HudiColumnValue(InternalRow internalRow, int ordinal, int precision, int scale) {
        this.isUnsafe = internalRow instanceof UnsafeRow;
        this.internalRow = internalRow;
        this.ordinal = ordinal;
        this.precision = precision;
        this.scale = scale;
    }

    public void reset(InternalRow internalRow, int ordinal, int precision, int scale) {
        this.isUnsafe = internalRow instanceof UnsafeRow;
        this.internalRow = internalRow;
        this.ordinal = ordinal;
        this.precision = precision;
        this.scale = scale;
    }

    public void reset(int ordinal, int precision, int scale) {
        this.ordinal = ordinal;
        this.precision = precision;
        this.scale = scale;
    }

    public void reset(InternalRow internalRow) {
        this.isUnsafe = internalRow instanceof UnsafeRow;
        this.internalRow = internalRow;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean isNull() {
        return internalRow.isNullAt(ordinal);
    }

    @Override
    public boolean getBoolean() {
        return internalRow.getBoolean(ordinal);
    }

    @Override
    public byte getByte() {
        return internalRow.getByte(ordinal);
    }

    @Override
    public short getShort() {
        return internalRow.getShort(ordinal);
    }

    @Override
    public int getInt() {
        return internalRow.getInt(ordinal);
    }

    @Override
    public float getFloat() {
        return internalRow.getFloat(ordinal);
    }

    @Override
    public long getLong() {
        return internalRow.getLong(ordinal);
    }

    @Override
    public double getDouble() {
        return internalRow.getDouble(ordinal);
    }

    @Override
    public BigInteger getBigInteger() {
        throw new UnsupportedOperationException("Hoodie type does not support largeint");
    }

    @Override
    public BigDecimal getDecimal() {
        return internalRow.getDecimal(ordinal, precision, scale).toJavaBigDecimal();
    }

    @Override
    public String getString() {
        return internalRow.getUTF8String(ordinal).toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        return internalRow.getUTF8String(ordinal).getBytes();
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay(internalRow.getInt(ordinal));
    }

    @Override
    public LocalDateTime getDateTime() {
        long datetime = internalRow.getLong(ordinal);
        long seconds;
        long nanoseconds;
        if (precision == 3) {
            seconds = datetime / 1000;
            nanoseconds = (datetime % 1000) * 1000000;
        } else if (precision == 6) {
            seconds = datetime / 1000000;
            nanoseconds = (datetime % 1000000) * 1000;
        } else {
            throw new RuntimeException("Hoodie timestamp only support milliseconds and microseconds");
        }
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanoseconds), ZoneId.systemDefault());
    }

    @Override
    public byte[] getBytes() {
        return internalRow.getBinary(ordinal);
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

    @Override
    public NativeValue getNativeValue(ColumnType.Type type) {
        if (isUnsafe) {
            UnsafeRow unsafeRow = (UnsafeRow) internalRow;
            switch (type) {
                case CHAR:
                case VARCHAR:
                case BINARY:
                case STRING:
                    long offsetAndSize = unsafeRow.getLong(ordinal);
                    int offset = (int) (offsetAndSize >> 32);
                    int size = (int) offsetAndSize;
                    return new NativeValue(unsafeRow.getBaseObject(), offset, size);
                default:
                    return null;
            }
        }
        return null;
    }
}
