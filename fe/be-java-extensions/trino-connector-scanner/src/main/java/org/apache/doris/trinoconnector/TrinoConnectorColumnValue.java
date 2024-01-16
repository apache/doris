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

package org.apache.doris.trinoconnector;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import io.trino.spi.block.Block;
import static java.lang.Double.longBitsToDouble;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class TrinoConnectorColumnValue implements ColumnValue {

    private int position;
    private Block block;
    ColumnType dorisType;

    public TrinoConnectorColumnValue() {
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setColumnType(ColumnType dorisType) {
        this.dorisType = dorisType;
    }

    public void setBlock(Block block) {
        this.block = block;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean getBoolean() {
        return block.getByte(position, 0) != 0;
    }

    @Override
    public byte getByte() {
        return block.getByte(position, 0);
    }

    @Override
    public short getShort() {
        return block.getShort(position, 0);
    }

    @Override
    public int getInt() {
        return block.getInt(position, 0);
    }

    @Override
    public float getFloat() {
        return 0;
    }

    @Override
    public long getLong() {
        return block.getLong(position, 0);
    }

    @Override
    public double getDouble() {
        return longBitsToDouble(block.getLong(position, 0));
    }

    @Override
    public BigInteger getBigInteger() {
        return BigInteger.valueOf(block.getInt(position, 0));
    }

    @Override
    public BigDecimal getDecimal() {
        // return record.getDecimal(idx, dorisType.getPrecision(), dorisType.getScale()).toBigDecimal();
        return null;
    }

    @Override
    public String getString() {
        int length = block.getSliceLength(position);
        return block.getSlice(position, 0, length).toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        int length = block.getSliceLength(position);
        return block.getSlice(position, 0, length).getBytes();
    }

    @Override
    public LocalDate getDate() {
        return null;
    }

    @Override
    public LocalDateTime getDateTime() {
        return null;

    }

    @Override
    public boolean isNull() {
        return block.isNull(position);
    }

    @Override
    public byte[] getBytes() {
        return null;
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
