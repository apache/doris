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

package org.apache.doris.iceberg;

import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.iceberg.StructLike;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

public class IcebergSysTableColumnValue implements ColumnValue {
    private static final String DEFAULT_TIME_ZONE = "Asia/Shanghai";

    private final Object fieldData;
    private final String timezone;

    public IcebergSysTableColumnValue(Object fieldData) {
        this(fieldData, DEFAULT_TIME_ZONE);
    }

    public IcebergSysTableColumnValue(Object fieldData, String timezone) {
        this.fieldData = fieldData;
        this.timezone = timezone;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean isNull() {
        return fieldData == null;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) fieldData;
    }

    @Override
    public byte getByte() {
        return (byte) fieldData;
    }

    @Override
    public short getShort() {
        return (short) fieldData;
    }

    @Override
    public int getInt() {
        return (int) fieldData;
    }

    @Override
    public float getFloat() {
        return (float) fieldData;
    }

    @Override
    public long getLong() {
        return (long) fieldData;
    }

    @Override
    public double getDouble() {
        return (double) fieldData;
    }

    @Override
    public BigInteger getBigInteger() {
        return (BigInteger) fieldData;
    }

    @Override
    public BigDecimal getDecimal() {
        return (BigDecimal) fieldData;
    }

    @Override
    public String getString() {
        return (String) fieldData;
    }

    @Override
    public byte[] getStringAsBytes() {
        if (fieldData instanceof String) {
            return ((String) fieldData).getBytes();
        } else if (fieldData instanceof byte[]) {
            return (byte[]) fieldData;
        } else if (fieldData instanceof CharBuffer) {
            CharBuffer buffer = (CharBuffer) fieldData;
            return buffer.toString().getBytes();
        } else if (fieldData instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) fieldData;
            byte[] res = new byte[buffer.limit()];
            buffer.get(res);
            return res;
        } else {
            throw new UnsupportedOperationException(
                    "Cannot convert fieldData of type " + (fieldData == null ? "null" : fieldData.getClass().getName())
                            + " to byte[].");
        }
    }

    @Override
    public LocalDate getDate() {
        return Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).plusDays((int) fieldData).toLocalDate();
    }

    @Override
    public LocalDateTime getDateTime() {
        Instant instant = Instant.ofEpochMilli((((long) fieldData) / 1000));
        return LocalDateTime.ofInstant(instant, ZoneId.of(timezone));
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) fieldData;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        List<?> items = (List<?>) fieldData;
        for (Object item : items) {
            values.add(new IcebergSysTableColumnValue(item, timezone));
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        Map<?, ?> data = (Map<?, ?>) fieldData;
        data.forEach((key, value) -> {
            keys.add(new IcebergSysTableColumnValue(key, timezone));
            values.add(new IcebergSysTableColumnValue(value, timezone));
        });
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        StructLike record = (StructLike) fieldData;
        for (Integer fieldIndex : structFieldIndex) {
            Object rawValue = record.get(fieldIndex, Object.class);
            values.add(new IcebergSysTableColumnValue(rawValue, timezone));
        }
    }
}
