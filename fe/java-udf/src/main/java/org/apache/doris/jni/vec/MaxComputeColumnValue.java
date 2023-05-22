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
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * MaxCompute Column value in vector column
 */
public class MaxComputeColumnValue implements ColumnValue {
    private long offset;
    private int idx;
    private ArrowBuf buffers;
    private FieldVector column;

    public MaxComputeColumnValue() {
        offset = 0L;
        idx = 0;
    }

    public void reset(FieldVector column) {
        this.column = column;
        this.buffers = column.getDataBuffer();
        this.offset = 0L;
        this.idx = 0;
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
        double v = buffers.getDouble(offset);
        offset += Double.BYTES;
        return v;
    }

    @Override
    public BigDecimal getDecimal() {
        DecimalVector decimalCol = (DecimalVector) column;
        return decimalCol.getObject(idx++);
    }

    @Override
    public String getString() {
        VarCharVector varcharCol = (VarCharVector) column;
        return varcharCol.getObject(idx++).toString();
    }

    @Override
    public LocalDate getDate() {
        DateDayVector dateCol = (DateDayVector) column;
        return LocalDate.ofEpochDay(dateCol.getObject(idx++));
    }

    @Override
    public LocalDateTime getDateTime() {
        DateMilliVector datetimeCol = (DateMilliVector) column;
        return datetimeCol.getObject(idx++);
    }

    @Override
    public byte[] getBytes() {
        VarBinaryVector binaryCol = (VarBinaryVector) column;
        return binaryCol.getObject(idx++);
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
