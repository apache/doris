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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * MaxCompute Column value in vector column
 */
public class MaxComputePartitionValue implements ColumnValue {
    private String partitionValue;

    public MaxComputePartitionValue(String partitionValue) {
        reset(partitionValue);
    }

    public void reset(String partitionValue) {
        this.partitionValue = partitionValue;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt() {
        return Integer.parseInt(partitionValue);
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong() {
        return Long.parseLong(partitionValue);
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigInteger getBigInteger() {
        return BigInteger.valueOf(getLong());
    }

    @Override
    public BigDecimal getDecimal() {
        return BigDecimal.valueOf(getDouble());
    }

    @Override
    public String getString() {
        return partitionValue;
    }

    @Override
    public byte[] getStringAsBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate getDate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDateTime getDateTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes() {
        return partitionValue.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        throw new UnsupportedOperationException();
    }
}
