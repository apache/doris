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

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class PaimonColumnValue implements ColumnValue {
    private int idx;
    private DataGetters record;
    private ColumnType dorisType;

    public PaimonColumnValue() {
    }

    public PaimonColumnValue(DataGetters record, int idx, ColumnType columnType) {
        this.idx = idx;
        this.record = record;
        this.dorisType = columnType;
    }

    public void setIdx(int idx, ColumnType dorisType) {
        this.idx = idx;
        this.dorisType = dorisType;
    }

    public void setOffsetRow(InternalRow record) {
        this.record = record;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean getBoolean() {
        return record.getBoolean(idx);
    }

    @Override
    public byte getByte() {
        return record.getByte(idx);
    }

    @Override
    public short getShort() {
        return record.getShort(idx);
    }

    @Override
    public int getInt() {
        return record.getInt(idx);
    }

    @Override
    public float getFloat() {
        return record.getFloat(idx);
    }

    @Override
    public long getLong() {
        return record.getLong(idx);
    }

    @Override
    public double getDouble() {
        return record.getDouble(idx);
    }

    @Override
    public BigInteger getBigInteger() {
        return BigInteger.valueOf(record.getInt(idx));
    }

    @Override
    public BigDecimal getDecimal() {
        return record.getDecimal(idx, dorisType.getPrecision(), dorisType.getScale()).toBigDecimal();
    }

    @Override
    public String getString() {
        return record.getString(idx).toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        return record.getString(idx).toBytes();
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay(record.getLong(idx));
    }

    @Override
    public LocalDateTime getDateTime() {
        return Instant.ofEpochMilli(record.getTimestamp(idx, 3)
            .getMillisecond()).atZone(ZoneOffset.ofHours(0)).toLocalDateTime();
    }

    @Override
    public boolean isNull() {
        return record.isNullAt(idx);
    }

    @Override
    public byte[] getBytes() {
        return record.getBinary(idx);
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        InternalArray recordArray = record.getArray(idx);
        for (int i = 0; i < recordArray.size(); i++) {
            PaimonColumnValue arrayColumnValue = new PaimonColumnValue((DataGetters) recordArray, i,
                    dorisType.getChildTypes().get(0));
            values.add(arrayColumnValue);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        InternalMap map = record.getMap(idx);
        InternalArray key = map.keyArray();
        for (int i = 0; i < key.size(); i++) {
            PaimonColumnValue keyColumnValue = new PaimonColumnValue((DataGetters) key, i,
                    dorisType.getChildTypes().get(0));
            keys.add(keyColumnValue);
        }
        InternalArray value = map.valueArray();
        for (int i = 0; i < value.size(); i++) {
            PaimonColumnValue valueColumnValue = new PaimonColumnValue((DataGetters) value, i,
                    dorisType.getChildTypes().get(1));
            values.add(valueColumnValue);
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {

    }
}
