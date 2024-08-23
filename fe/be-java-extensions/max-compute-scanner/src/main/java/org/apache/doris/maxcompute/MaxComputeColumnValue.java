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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
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
    private int offset = 0; // for complex type
    private ValueVector column;

    public MaxComputeColumnValue() {
        idx = 0;
    }

    public MaxComputeColumnValue(ValueVector valueVector, int i) {
        this.column = valueVector;
        this.idx = i;
    }

    public void reset(ValueVector column) {
        this.column = column;
        this.idx = 0;
        this.offset = 0;
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
        return ((BitVector) column).getObject(idx++);
    }

    @Override
    public byte getByte() {
        skippedIfNull();
        return ((TinyIntVector) column).getObject(idx++);
    }

    @Override
    public short getShort() {
        skippedIfNull();
        return ((SmallIntVector) column).getObject(idx++);
    }

    @Override
    public int getInt() {
        skippedIfNull();
        return ((IntVector) column).getObject(idx++);
    }

    @Override
    public float getFloat() {
        skippedIfNull();
        return ((Float4Vector) column).getObject(idx++);
    }

    @Override
    public long getLong() {
        skippedIfNull();
        return ((BigIntVector) column).getObject(idx++);
    }

    @Override
    public double getDouble() {
        skippedIfNull();
        return ((Float8Vector) column).getObject(idx++);
    }

    @Override
    public BigInteger getBigInteger() {
        skippedIfNull();
        return BigInteger.valueOf(((BigIntVector) column).getObject(idx++));
    }

    @Override
    public BigDecimal getDecimal() {
        skippedIfNull();
        return ((DecimalVector) column).getObject(idx++);
    }

    @Override
    public String getString() {
        skippedIfNull();
        return ((VarCharVector) column).getObject(idx++).toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        return getString().getBytes();
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
        skippedIfNull();
        ListVector listCol = (ListVector) column;
        int elemSize = listCol.getObject(idx).size();
        for (int i = 0; i < elemSize; i++) {
            MaxComputeColumnValue val = new MaxComputeColumnValue(listCol.getDataVector(), offset);
            values.add(val);
            offset++;
        }
        idx++;
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        skippedIfNull();
        MapVector mapCol = (MapVector) column;
        int elemSize = mapCol.getObject(idx).size();
        FieldVector keyList = mapCol.getDataVector().getChildrenFromFields().get(0);
        FieldVector valList = mapCol.getDataVector().getChildrenFromFields().get(1);
        for (int i = 0; i < elemSize; i++) {
            MaxComputeColumnValue key = new MaxComputeColumnValue(keyList, offset);
            keys.add(key);
            MaxComputeColumnValue val = new MaxComputeColumnValue(valList, offset);
            values.add(val);
            offset++;
        }
        idx++;
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        skippedIfNull();
        StructVector structCol = (StructVector) column;
        for (Integer fieldIndex : structFieldIndex) {
            MaxComputeColumnValue val = new MaxComputeColumnValue(structCol.getChildByOrdinal(fieldIndex), idx);
            values.add(val);
        }
        idx++;
    }
}
