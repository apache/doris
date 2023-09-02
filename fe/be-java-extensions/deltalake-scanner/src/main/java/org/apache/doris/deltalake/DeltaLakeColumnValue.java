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

package org.apache.doris.deltalake;

import org.apache.doris.common.jni.vec.ColumnValue;

import io.delta.standalone.data.RowRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class DeltaLakeColumnValue implements ColumnValue {

    private String fieldName;
    private RowRecord record;

    public DeltaLakeColumnValue() {

    }

    public void setfieldName(String name) {
        this.fieldName = name;
    }

    public void setRecord(RowRecord record) {
        this.record = record;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean isNull() {
        return record.isNullAt(fieldName);
    }

    @Override
    public boolean getBoolean() {
        return record.getBoolean(fieldName);
    }

    @Override
    public byte getByte() {
        return record.getByte(fieldName);
    }

    @Override
    public short getShort() {
        return record.getShort(fieldName);
    }

    @Override
    public int getInt() {
        return record.getInt(fieldName);
    }

    @Override
    public float getFloat() {
        return record.getFloat(fieldName);
    }

    @Override
    public long getLong() {
        return record.getLong(fieldName);
    }

    @Override
    public double getDouble() {
        return record.getDouble(fieldName);
    }

    @Override
    public BigInteger getBigInteger() {
        return BigInteger.valueOf(record.getInt(fieldName));
    }

    @Override
    public BigDecimal getDecimal() {
        return record.getBigDecimal(fieldName);
    }

    @Override
    public String getString() {
        return record.getString(fieldName);
    }

    @Override
    public byte[] getStringAsBytes() {
        return record.getString(fieldName).getBytes();
    }

    @Override
    public LocalDate getDate() {
        return record.getDate(fieldName).toLocalDate();
    }

    @Override
    public LocalDateTime getDateTime() {
        return record.getTimestamp(fieldName).toLocalDateTime();
    }

    @Override
    public byte[] getBytes() {
        return record.getBinary(fieldName);
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
