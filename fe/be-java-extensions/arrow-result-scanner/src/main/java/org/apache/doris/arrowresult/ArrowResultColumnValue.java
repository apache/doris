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

package org.apache.doris.arrowresult;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;

public class ArrowResultColumnValue implements ColumnValue {
    private static final Logger LOG = LoggerFactory.getLogger(ArrowResultColumnValue.class);
    private Object value;
    private ColumnType type;

    @Override
    public boolean canGetStringAsBytes() {
        return value != null;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public boolean getBoolean() {
        return (Boolean) value;
    }

    @Override
    public byte getByte() {
        return (Byte) value;
    }

    @Override
    public short getShort() {
        return (Short) value;
    }

    @Override
    public int getInt() {
        return (Integer) value;
    }

    @Override
    public float getFloat() {
        return (Float) value;
    }

    @Override
    public long getLong() {
        return (Long) value;
    }

    @Override
    public double getDouble() {
        return (Double) value;
    }

    @Override
    public BigInteger getBigInteger() {
        if (value.toString().isEmpty()) {
            return BigInteger.valueOf(0);
        }
        return BigInteger.valueOf(Long.parseLong(value.toString()));
    }

    @Override
    public BigDecimal getDecimal() {
        return (BigDecimal) value;
    }

    @Override
    public String getString() {
        return value.toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        return value.toString().getBytes();
    }

    @Override
    public LocalDate getDate() {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }

        return LocalDate.ofEpochDay(Integer.parseInt(value.toString()));
    }

    @Override
    public LocalDateTime getDateTime() {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        return convertToLocalDateTime(Long.parseLong(value.toString()), type.getPrecision());
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) value;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        JsonStringArrayList jsonArray = (JsonStringArrayList) value;
        for (Object v : jsonArray) {
            ArrowResultColumnValue arrowResultColumnValue = new ArrowResultColumnValue();
            arrowResultColumnValue.setValue(v);
            arrowResultColumnValue.setColumnType(type.getChildTypes().get(0));
            values.add(arrowResultColumnValue);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        JsonStringArrayList jsonArray = (JsonStringArrayList) value;
        ColumnType keyType = type.getChildTypes().get(0);
        ColumnType valueType = type.getChildTypes().get(1);
        for (Object map : jsonArray) {
            JsonStringHashMap jsonHashMap = (JsonStringHashMap) map;
            ArrowResultColumnValue kColumnValue = new ArrowResultColumnValue();
            kColumnValue.setValue(jsonHashMap.get("key"));
            kColumnValue.setColumnType(keyType);
            keys.add(kColumnValue);

            ArrowResultColumnValue vColumnValue = new ArrowResultColumnValue();
            vColumnValue.setValue(jsonHashMap.get("value"));
            vColumnValue.setColumnType(valueType);
            values.add(vColumnValue);
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        JsonStringHashMap jsonHashMap = (JsonStringHashMap) value;
        Iterator valuesIterator = jsonHashMap.values().iterator();
        for (Integer fieldIndex : structFieldIndex) {
            ArrowResultColumnValue structColumnValue = new ArrowResultColumnValue();
            structColumnValue.setColumnType(type.getChildTypes().get(fieldIndex));
            if (valuesIterator.hasNext()) {
                structColumnValue.setValue(valuesIterator.next());
            }
            values.add(structColumnValue);
        }
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setColumnType(ColumnType type) {
        this.type = type;
    }

    public static LocalDateTime convertToLocalDateTime(long timestamp, int precision) {
        if (precision == 0) {
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault());
        } else if (precision <= 3) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        } else {
            long epochSecond = timestamp / 1_000_000;
            long micros = timestamp % 1_000_000;
            long nanoAdjustment = micros * 1_000;

            return LocalDateTime.ofInstant(
                Instant.ofEpochSecond(epochSecond, nanoAdjustment),
                ZoneId.systemDefault()
            );
        }
    }
}
