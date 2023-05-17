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

package org.apache.doris.jni.vec;

import org.apache.arrow.memory.ArrowBuf;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * MaxCompute Column value in vector column
 */
public class MaxComputeColumnValue implements ColumnValue {
    private long offset;
    private ArrowBuf buffers;

    public MaxComputeColumnValue() {
        offset = 0L;
    }

    public void setBuffers(ArrowBuf buffers) {
        this.buffers = buffers;
        this.offset = 0L;
    }

    @Override
    public boolean getBoolean() {
        byte v = buffers.getByte(offset);
        offset += Byte.BYTES;
        return v > 0;
    }

    @Override
    public byte getByte() {
        byte v = buffers.getByte(offset);
        offset += Byte.BYTES;
        return v;
    }

    @Override
    public short getShort() {
        short v = buffers.getShort(offset);
        offset += Short.BYTES;
        return v;
    }

    @Override
    public int getInt() {
        int v = buffers.getInt(offset);
        offset += Integer.BYTES;
        return v;
    }

    @Override
    public float getFloat() {
        float v = buffers.getFloat(offset);
        offset += Float.BYTES;
        return v;
    }

    @Override
    public long getLong() {
        long v = buffers.getLong(offset);
        offset += Long.BYTES;
        return v;
    }

    @Override
    public double getDouble() {
        Double v = buffers.getDouble(offset);
        offset += Double.BYTES;
        return v;
    }

    @Override
    public BigDecimal getDecimal() {
        return BigDecimal.valueOf(getDouble());
    }

    @Override
    public String getString() {
        return "row";
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.now();
    }

    @Override
    public LocalDateTime getDateTime() {
        return LocalDateTime.now();
    }

    @Override
    public byte[] getBytes() {
        return "bytes".getBytes();
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
