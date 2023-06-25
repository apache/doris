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

package org.apache.doris.avro;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class AvroColumnValue implements ColumnValue {

    private byte[] valueBytes;
    private final ColumnType columnType;
    private ByteBuffer byteBuffer;

    public AvroColumnValue(Object value, ColumnType columnType) {
        this.columnType = columnType;
        convert2Bytes(value);
    }

    public void convert2Bytes(Object value) {
        switch (columnType.getType()) {
            case BOOLEAN:
                byteBuffer = ByteBuffer.allocate(1);
                byteBuffer.put((byte) ((boolean) value ? 1 : 0));
                break;
            case TINYINT:
                byteBuffer = ByteBuffer.allocate(1);
                byteBuffer.putInt((int) value);
                break;
            case SMALLINT:
                byteBuffer = ByteBuffer.allocate(2);
                byteBuffer.putShort((short) value);
                break;
            case INT:
                byteBuffer = ByteBuffer.allocate(4);
                byteBuffer.putInt((int) value);
                break;
            case BIGINT:
                byteBuffer = ByteBuffer.allocate(8);
                byteBuffer.putLong((long) value);
                break;
            case FLOAT:
                byteBuffer = ByteBuffer.allocate(4);
                byteBuffer.putFloat((float) value);
                break;
            case DOUBLE:
                byteBuffer = ByteBuffer.allocate(8);
                byteBuffer.putDouble((double) value);
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
            default:
                byteBuffer = ByteBuffer.allocate(value.toString().length());
                valueBytes = ByteBuffer.allocate(value.toString().length())
                        .put(value.toString().getBytes(StandardCharsets.UTF_8)).array();
        }
    }

    private Object inspectObject() {
        byteBuffer.flip();
        switch (columnType.getType()) {
            case BOOLEAN:
                return byteBuffer.get() == 1;
            case TINYINT:
                return byteBuffer.get();
            case SMALLINT:
                return byteBuffer.getShort();
            case INT:
                return byteBuffer.getInt();
            case BIGINT:
                return byteBuffer.getLong();
            case FLOAT:
                return byteBuffer.getFloat();
            case DOUBLE:
                return byteBuffer.getDouble();
            case CHAR:
            case VARCHAR:
            case STRING:
                return new String(valueBytes, StandardCharsets.UTF_8);
            case BINARY:
                return valueBytes;
            default:
                return new Object();
        }
    }

    @Override
    public boolean canGetStringAsBytes() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) inspectObject();
    }

    @Override
    public byte getByte() {
        return (byte) inspectObject();
    }

    @Override
    public short getShort() {
        return (short) inspectObject();
    }

    @Override
    public int getInt() {
        return (int) inspectObject();
    }

    @Override
    public float getFloat() {
        return (float) inspectObject();
    }

    @Override
    public long getLong() {
        return (long) inspectObject();
    }

    @Override
    public double getDouble() {
        return (double) inspectObject();
    }

    @Override
    public BigInteger getBigInteger() {
        return null;
    }

    @Override
    public BigDecimal getDecimal() {
        return (BigDecimal) inspectObject();
    }

    @Override
    public String getString() {
        return inspectObject().toString();
    }

    @Override
    public LocalDate getDate() {
        // avro has no date type
        return null;
    }

    @Override
    public LocalDateTime getDateTime() {
        // avro has no dateTime type
        return null;
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) inspectObject();
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
