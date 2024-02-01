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
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.util.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class TrinoConnectorColumnValue implements ColumnValue {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorColumnValue.class);
    private int position;
    private Block block;
    ColumnType dorisType;
    Type trinoType;

    public TrinoConnectorColumnValue() {
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setColumnType(ColumnType dorisType) {
        this.dorisType = dorisType;
    }

    public void setTrinoType(Type trinoType) {
        this.trinoType = trinoType;
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
        return block.getSlice(position, 0, block.getSliceLength(position)).getFloat(0);
    }

    @Override
    public long getLong() {
        return block.getLong(position, 0);
    }

    @Override
    public double getDouble() {
        return block.getSlice(position, 0, block.getSliceLength(position)).getDouble(0);
    }

    @Override
    public BigInteger getBigInteger() {
        return BigInteger.valueOf(block.getInt(position, 0));
    }

    // block is LongArrayBlock type
    @Override
    public BigDecimal getDecimal() {
        return Decimals.readBigDecimal((DecimalType) trinoType, block, position);
    }

    // block is VariableWidthBlock
    @Override
    public String getString() {
        return block.getSlice(position, 0, block.getSliceLength(position)).toStringUtf8();
    }

    // block is VariableWidthBlock
    @Override
    public byte[] getStringAsBytes() {
        return block.getSlice(position, 0, block.getSliceLength(position)).getBytes();
    }

    // block is IntArrayBlock
    @Override
    public LocalDate getDate() {
        return LocalDate.parse(DateTimeUtils.printDate(block.getInt(position, 0)), DateTimeFormatter.ISO_DATE);
    }

    // block is LongArrayBlock
    @Override
    public LocalDateTime getDateTime() {
        long timestamp = DateTimeEncoding.unpackMillisUtc(block.getLong(position, 0));
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    @Override
    public boolean isNull() {
        return block.isNull(position);
    }

    @Override
    public byte[] getBytes() {
        return block.getSlice(position, 0, block.getSliceLength(position)).getBytes();
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
