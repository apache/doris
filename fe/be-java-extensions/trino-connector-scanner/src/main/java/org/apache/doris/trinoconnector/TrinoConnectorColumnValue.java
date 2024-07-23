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

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class TrinoConnectorColumnValue implements ColumnValue {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorColumnValue.class);
    private int position;
    private Block block;
    ColumnType dorisType;
    Type trinoType;
    ConnectorSession connectorSession;

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

    public void setConnectorSession(ConnectorSession connectorSession) {
        this.connectorSession = connectorSession;
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
        return RealType.REAL.getFloat(block, position);
    }

    // BigInt
    @Override
    public long getLong() {
        return block.getLong(position, 0);
    }

    // block is LongArrayBlock type
    @Override
    public double getDouble() {
        return DoubleType.DOUBLE.getDouble(block, position);
    }

    // LargeInt
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
        // Trino TimeType -> Doris StringType
        if (trinoType instanceof TimeType) {
            return ((SqlTime) (trinoType.getObjectValue(connectorSession, block, position))).toString();
        }
        return block.getSlice(position, 0, block.getSliceLength(position)).toStringUtf8();
    }

    // block is VariableWidthBlock
    @Override
    public byte[] getStringAsBytes() {
        // Trino TimeType -> Doris StringType
        if (trinoType instanceof TimeType) {
            return ((SqlTime) (trinoType.getObjectValue(connectorSession, block, position))).toString().getBytes();
        }
        return block.getSlice(position, 0, block.getSliceLength(position)).getBytes();
    }

    // block is IntArrayBlock
    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay(block.getInt(position, 0));
    }

    // block is LongArrayBlock
    @Override
    public LocalDateTime getDateTime() {
        Object o = trinoType.getObjectValue(connectorSession, block, position);
        if (o instanceof SqlTimestampWithTimeZone) {
            return ((SqlTimestampWithTimeZone) o).toZonedDateTime().toLocalDateTime();
        } else if (o instanceof SqlTimestamp) {
            return ((SqlTimestamp) o).toLocalDateTime();
        }
        return null;
    }

    @Override
    public boolean isNull() {
        return block.isNull(position);
    }

    @Override
    public byte[] getBytes() {
        return block.getSlice(position, 0, block.getSliceLength(position)).getBytes();
    }

    // block is ArrayBlock
    @Override
    public void unpackArray(List<ColumnValue> values) {
        Block array = ((ArrayBlock) block).getArray(position);
        for (int i = 0; i < array.getPositionCount(); ++i) {
            TrinoConnectorColumnValue trinoColumnValue = new TrinoConnectorColumnValue();
            trinoColumnValue.setBlock(array);
            trinoColumnValue.setColumnType(dorisType.getChildTypes().get(0));
            trinoColumnValue.setTrinoType(((ArrayType) trinoType).getElementType());
            trinoColumnValue.setPosition(i);
            values.add(trinoColumnValue);
        }
    }

    // block is MapBlock
    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapBlock singleMapBlock = ((MapBlock) block).getSingleValueBlock(position);

        // add key ColumnValue
        Block keyBlock = singleMapBlock.getChildren().get(0);
        for (int i = 0; i < keyBlock.getPositionCount(); ++i) {
            TrinoConnectorColumnValue trinoColumnValue = new TrinoConnectorColumnValue();
            trinoColumnValue.setBlock(keyBlock);
            trinoColumnValue.setColumnType(dorisType.getChildTypes().get(0));
            trinoColumnValue.setTrinoType(((MapType) trinoType).getKeyType());
            trinoColumnValue.setPosition(i);
            keys.add(trinoColumnValue);
        }

        // add value ColumnValue
        Block valueBlock = singleMapBlock.getChildren().get(1);
        for (int i = 0; i < valueBlock.getPositionCount(); ++i) {
            TrinoConnectorColumnValue trinoColumnValue = new TrinoConnectorColumnValue();
            trinoColumnValue.setBlock(valueBlock);
            trinoColumnValue.setColumnType(dorisType.getChildTypes().get(1));
            trinoColumnValue.setTrinoType(((MapType) trinoType).getValueType());
            trinoColumnValue.setPosition(i);
            values.add(trinoColumnValue);
        }
    }

    // block is RowBlock
    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        for (int i : structFieldIndex) {
            TrinoConnectorColumnValue trinoColumnValue = new TrinoConnectorColumnValue();
            trinoColumnValue.setBlock(((RowBlock) block).getFieldBlock(i));
            trinoColumnValue.setColumnType(dorisType.getChildTypes().get(i));
            trinoColumnValue.setTrinoType(trinoType.getTypeParameters().get(i));
            trinoColumnValue.setPosition(position);
            values.add(trinoColumnValue);
        }
    }
}
