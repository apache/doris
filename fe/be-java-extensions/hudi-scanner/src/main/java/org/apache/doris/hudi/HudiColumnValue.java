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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

public class HudiColumnValue implements ColumnValue {
    private SpecializedGetters data;
    private int ordinal;
    private ColumnType columnType;

    HudiColumnValue() {
    }

    HudiColumnValue(SpecializedGetters data, int ordinal, ColumnType columnType) {
        this.data = data;
        this.ordinal = ordinal;
        this.columnType = columnType;
    }

    public void reset(SpecializedGetters data, int ordinal, ColumnType columnType) {
        this.data = data;
        this.ordinal = ordinal;
        this.columnType = columnType;
    }

    public void reset(int ordinal, ColumnType columnType) {
        this.ordinal = ordinal;
        this.columnType = columnType;
    }

    public void reset(SpecializedGetters data) {
        this.data = data;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean isNull() {
        return data.isNullAt(ordinal);
    }

    @Override
    public boolean getBoolean() {
        return data.getBoolean(ordinal);
    }

    @Override
    public byte getByte() {
        return data.getByte(ordinal);
    }

    @Override
    public short getShort() {
        return data.getShort(ordinal);
    }

    @Override
    public int getInt() {
        return data.getInt(ordinal);
    }

    @Override
    public float getFloat() {
        return data.getFloat(ordinal);
    }

    @Override
    public long getLong() {
        return data.getLong(ordinal);
    }

    @Override
    public double getDouble() {
        return data.getDouble(ordinal);
    }

    @Override
    public BigInteger getBigInteger() {
        throw new UnsupportedOperationException("Hoodie type does not support largeint");
    }

    @Override
    public BigDecimal getDecimal() {
        return data.getDecimal(ordinal, columnType.getPrecision(), columnType.getScale()).toJavaBigDecimal();
    }

    @Override
    public String getString() {
        return data.getUTF8String(ordinal).toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        return data.getUTF8String(ordinal).getBytes();
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay(data.getInt(ordinal));
    }

    @Override
    public LocalDateTime getDateTime() {
        long datetime = data.getLong(ordinal);
        long seconds;
        long nanoseconds;
        if (columnType.getPrecision() == 3) {
            seconds = datetime / 1000;
            nanoseconds = (datetime % 1000) * 1000000;
        } else if (columnType.getPrecision() == 6) {
            seconds = datetime / 1000000;
            nanoseconds = (datetime % 1000000) * 1000;
        } else {
            throw new RuntimeException("Hoodie timestamp only support milliseconds and microseconds, wrong precision = "
                    + columnType.getPrecision());
        }
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanoseconds), ZoneId.systemDefault());
    }

    @Override
    public byte[] getBytes() {
        return data.getBinary(ordinal);
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        ArrayData array = data.getArray(ordinal);
        for (int i = 0; i < array.numElements(); ++i) {
            values.add(new HudiColumnValue(array, i, columnType.getChildTypes().get(0)));
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapData map = data.getMap(ordinal);
        ArrayData key = map.keyArray();
        for (int i = 0; i < key.numElements(); ++i) {
            keys.add(new HudiColumnValue(key, i, columnType.getChildTypes().get(0)));
        }
        ArrayData value = map.valueArray();
        for (int i = 0; i < value.numElements(); ++i) {
            values.add(new HudiColumnValue(value, i, columnType.getChildTypes().get(1)));
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        // todo: support pruned struct fields
        InternalRow struct = data.getStruct(ordinal, structFieldIndex.size());
        for (int i : structFieldIndex) {
            values.add(new HudiColumnValue(struct, i, columnType.getChildTypes().get(i)));
        }
    }
}
