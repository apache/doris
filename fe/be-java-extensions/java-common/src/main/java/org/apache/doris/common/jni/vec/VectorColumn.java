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

package org.apache.doris.common.jni.vec;

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.utils.TypeNativeBytes;
import org.apache.doris.common.jni.vec.ColumnType.Type;
import org.apache.doris.common.jni.vec.NativeColumnValue.NativeValue;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reference to Apache Spark
 * see <a href="https://github.com/apache/spark/blob/master/sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/WritableColumnVector.java">WritableColumnVector</a>
 */
public class VectorColumn {
    // String is stored as array<byte>
    // The default string length to initialize the capacity.
    private static final int DEFAULT_STRING_LENGTH = 4;

    // NullMap column address
    private long nullMap;
    private boolean[] nulls = null;
    // Data column address
    private long data;

    // For String / Array / Map.
    private long offsets;
    // String's offset is int32, while Array&Map's offset is int64
    // todo: how to solve the overflow of offsets when taking Array&Map's offset as int32
    private boolean isComplexType = false;
    // Number of elements in vector column
    private int capacity;
    // Upper limit for the maximum capacity for this column.
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 15;
    private final ColumnType columnType;

    private int numNulls;

    private int appendIndex;

    // For nested column type: String / Array/ Map / Struct
    private VectorColumn[] childColumns;

    // For struct, only support to read all fields in struct now
    // todo: support pruned struct fields
    private List<Integer> structFieldIndex;

    // Create writable column
    private VectorColumn(ColumnType columnType, int capacity) {
        this.columnType = columnType;
        this.capacity = 0;
        this.nullMap = 0;
        this.data = 0;
        this.offsets = 0;
        this.numNulls = 0;
        this.appendIndex = 0;
        if (columnType.isComplexType()) {
            isComplexType = true;
            List<ColumnType> children = columnType.getChildTypes();
            childColumns = new VectorColumn[children.size()];
            for (int i = 0; i < children.size(); ++i) {
                childColumns[i] = new VectorColumn(children.get(i), capacity);
            }
            if (columnType.isStruct()) {
                structFieldIndex = new ArrayList<>();
                for (int i = 0; i < children.size(); ++i) {
                    structFieldIndex.add(i);
                }
            }
        } else if (columnType.isStringType()) {
            childColumns = new VectorColumn[1];
            childColumns[0] = new VectorColumn(new ColumnType("#stringBytes", Type.BYTE),
                    capacity * DEFAULT_STRING_LENGTH);
        }

        reserveCapacity(capacity);
    }

    // restore the child of string column & restore meta column
    private VectorColumn(long address, int capacity, ColumnType columnType) {
        this.columnType = columnType;
        this.capacity = capacity;
        this.nullMap = 0;
        this.data = address;
        this.offsets = 0;
        this.numNulls = 0;
        this.appendIndex = capacity;
        if (columnType.isStruct()) {
            List<ColumnType> children = columnType.getChildTypes();
            structFieldIndex = new ArrayList<>();
            for (int i = 0; i < children.size(); ++i) {
                structFieldIndex.add(i);
            }
        }
    }

    // Create readable column
    private VectorColumn(ColumnType columnType, int numRows, long columnMetaAddress) {
        if (columnType.isUnsupported()) {
            throw new RuntimeException("Unsupported type for column: " + columnType.getName());
        }
        long address = columnMetaAddress;
        this.capacity = numRows;
        this.columnType = columnType;
        this.nullMap = OffHeap.getLong(null, address);
        address += 8;
        this.numNulls = 0;
        if (this.nullMap != 0) {
            nulls = OffHeap.getBoolean(null, nullMap, numRows);
            for (boolean isNull : nulls) {
                if (isNull) {
                    this.numNulls++;
                }
            }
        }
        this.appendIndex = numRows;

        if (columnType.isComplexType()) {
            isComplexType = true;
            int childRows = numRows;
            if (!columnType.isStruct()) {
                this.offsets = OffHeap.getLong(null, address);
                address += 8;
                childRows = getArrayEndOffset(numRows - 1);
            }
            this.data = 0;
            List<ColumnType> children = columnType.getChildTypes();
            childColumns = new VectorColumn[children.size()];
            for (int i = 0; i < children.size(); ++i) {
                childColumns[i] = new VectorColumn(children.get(i), childRows, address);
                address += children.get(i).metaSize() * 8L;
            }
        } else if (columnType.isStringType()) {
            this.offsets = OffHeap.getLong(null, address);
            address += 8;
            this.data = 0;
            int length = OffHeap.getInt(null, this.offsets + (numRows - 1) * 4L);
            childColumns = new VectorColumn[1];
            childColumns[0] = new VectorColumn(OffHeap.getLong(null, address), length,
                    new ColumnType("#stringBytes", Type.BYTE));
        } else {
            this.data = OffHeap.getLong(null, address);
            this.offsets = 0;
        }
    }

    public static VectorColumn createWritableColumn(ColumnType columnType, int capacity) {
        return new VectorColumn(columnType, capacity);
    }

    public static VectorColumn createReadableColumn(ColumnType columnType, int numRows, long columnMetaAddress) {
        return new VectorColumn(columnType, numRows, columnMetaAddress);
    }

    public static VectorColumn createReadableColumn(long address, int capacity, ColumnType columnType) {
        return new VectorColumn(address, capacity, columnType);
    }

    private int getArrayEndOffset(int rowId) {
        if (rowId >= 0 && rowId < appendIndex) {
            if (isComplexType) {
                // maybe overflowed
                return (int) OffHeap.getLong(null, offsets + 8L * rowId);
            } else {
                return OffHeap.getInt(null, offsets + 4L * rowId);
            }
        } else {
            return 0;
        }
    }

    public long nullMapAddress() {
        return nullMap;
    }

    public long dataAddress() {
        return data;
    }

    public int numRows() {
        return appendIndex;
    }

    public long offsetAddress() {
        return offsets;
    }

    public ColumnType.Type getColumnTyp() {
        return columnType.getType();
    }

    /**
     * Release columns and meta information
     */
    public void close() {
        if (childColumns != null) {
            for (int i = 0; i < childColumns.length; i++) {
                childColumns[i].close();
                childColumns[i] = null;
            }
            childColumns = null;
        }

        if (nullMap != 0) {
            OffHeap.freeMemory(nullMap);
        }
        if (data != 0) {
            OffHeap.freeMemory(data);
        }
        if (offsets != 0) {
            OffHeap.freeMemory(offsets);
        }
        nullMap = 0;
        data = 0;
        offsets = 0;
        capacity = 0;
        numNulls = 0;
        appendIndex = 0;
    }

    private void throwReserveException(int requiredCapacity, Throwable cause) {
        String message = "Cannot reserve enough bytes in off heap memory ("
                + (requiredCapacity >= 0 ? "requested " + requiredCapacity + " bytes" : "integer overflow).");
        throw new RuntimeException(message, cause);
    }

    private void reserve(int requiredCapacity) {
        if (requiredCapacity < 0) {
            throwReserveException(requiredCapacity, null);
        } else if (requiredCapacity > capacity) {
            int newCapacity = (int) Math.min(MAX_CAPACITY, requiredCapacity * 2L);
            if (requiredCapacity <= newCapacity) {
                try {
                    reserveCapacity(newCapacity);
                } catch (OutOfMemoryError outOfMemoryError) {
                    throwReserveException(requiredCapacity, outOfMemoryError);
                }
            } else {
                // overflow
                throwReserveException(requiredCapacity, null);
            }
        }
    }

    private void reserveCapacity(int newCapacity) {
        long offsetLength = isComplexType ? 8L : 4L;
        long oldCapacity = capacity;
        long oldOffsetSize = capacity * offsetLength;
        long newOffsetSize = newCapacity * offsetLength;
        long typeSize = columnType.getTypeSize();
        if (columnType.isUnsupported()) {
            // do nothing
            return;
        } else if (typeSize != -1) {
            this.data = OffHeap.reallocateMemory(data, oldCapacity * typeSize, newCapacity * typeSize);
        } else if (columnType.isStringType() || columnType.isArray() || columnType.isMap()) {
            this.offsets = OffHeap.reallocateMemory(offsets, oldOffsetSize, newOffsetSize);
        } else if (!columnType.isStruct()) {
            throw new RuntimeException("Unhandled type: " + columnType);
        }
        if (!"#stringBytes".equals(columnType.getName())) {
            this.nullMap = OffHeap.reallocateMemory(nullMap, oldCapacity, newCapacity);
            OffHeap.setMemory(nullMap + oldCapacity, (byte) 0, newCapacity - oldCapacity);
        }
        capacity = newCapacity;
    }

    public void reset() {
        if (childColumns != null) {
            for (VectorColumn c : childColumns) {
                c.reset();
            }
        }
        appendIndex = 0;
        if (numNulls > 0) {
            putNotNulls(0, capacity);
            numNulls = 0;
        }
    }

    public final boolean isNullAt(int rowId) {
        if (numNulls == 0 || nullMap == 0) {
            return false;
        } else if (nulls != null) {
            return nulls[rowId];
        } else {
            return OffHeap.getBoolean(null, nullMap + rowId);
        }
    }

    public final boolean hasNull() {
        return numNulls > 0;
    }

    private void putNotNulls(int rowId, int count) {
        if (!hasNull()) {
            return;
        }
        long offset = nullMap + rowId;
        for (int i = 0; i < count; ++i, ++offset) {
            OffHeap.putByte(null, offset, (byte) 0);
        }
    }

    public int appendNull(ColumnType.Type typeValue) {
        reserve(appendIndex + 1);
        putNull(appendIndex);
        // append default value
        switch (typeValue) {
            case BOOLEAN:
                return appendBoolean(false);
            case TINYINT:
                return appendByte((byte) 0);
            case SMALLINT:
                return appendShort((short) 0);
            case INT:
                return appendInt(0);
            case BIGINT:
                return appendLong(0);
            case LARGEINT:
                return appendBigInteger(BigInteger.ZERO);
            case FLOAT:
                return appendFloat(0);
            case DOUBLE:
                return appendDouble(0);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return appendDecimal(new BigDecimal(0));
            case DATE:
            case DATEV2:
                return appendDate(LocalDate.MIN);
            case DATETIME:
            case DATETIMEV2:
                return appendDateTime(LocalDateTime.MIN);
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
                return appendBytesAndOffset(new byte[0]);
            case ARRAY:
                return appendArray(Collections.emptyList());
            case MAP:
                return appendMap(Collections.emptyList(), Collections.emptyList());
            case STRUCT:
                return appendStruct(structFieldIndex, null);
            default:
                throw new RuntimeException("Unknown type value: " + typeValue);
        }
    }

    private void putNull(int rowId) {
        OffHeap.putByte(null, nullMap + rowId, (byte) 1);
        ++numNulls;
    }

    public int appendBoolean(boolean v) {
        reserve(appendIndex + 1);
        putBoolean(appendIndex, v);
        return appendIndex++;
    }

    public void appendBoolean(Boolean[] batch, boolean isNullable) {
        int rows = batch.length;
        reserve(appendIndex + rows);
        byte[] batchData = new byte[rows];
        if (isNullable) {
            byte[] batchNulls = new byte[rows];
            for (int i = 0; i < rows; ++i) {
                if (batch[i] == null) {
                    batchNulls[i] = 1;
                    numNulls++;
                } else {
                    batchNulls[i] = 0;
                    batchData[i] = (byte) (batch[i] ? 1 : 0);
                }
            }
            OffHeap.UNSAFE.copyMemory(batchNulls, OffHeap.BYTE_ARRAY_OFFSET, null, nullMap + appendIndex, rows);
        } else {
            for (int i = 0; i < rows; ++i) {
                batchData[i] = (byte) (batch[i] ? 1 : 0);
            }
        }
        OffHeap.UNSAFE.copyMemory(batchData, OffHeap.BYTE_ARRAY_OFFSET, null, data + appendIndex, rows);
        appendIndex += rows;
    }

    private void putBoolean(int rowId, boolean value) {
        OffHeap.putByte(null, data + rowId, (byte) (value ? 1 : 0));
    }

    public boolean getBoolean(int rowId) {
        return OffHeap.getByte(null, data + rowId) == 1;
    }

    public Boolean[] getBooleanColumn(int start, int end) {
        int length = end - start;
        Boolean[] result = new Boolean[length];
        boolean[] batch = OffHeap.getBoolean(null, data + start, length);
        if (hasNull()) {
            for (int i = 0; i < length; ++i) {
                if (!isNullAt(start + i)) {
                    result[i] = batch[i];
                }
            }
        } else {
            for (int i = 0; i < length; ++i) {
                result[i] = batch[i];
            }
        }
        return result;
    }

    public int appendByte(byte v) {
        reserve(appendIndex + 1);
        putByte(appendIndex, v);
        return appendIndex++;
    }

    public void appendByte(Byte[] batch, boolean isNullable) {
        int rows = batch.length;
        reserve(appendIndex + rows);
        byte[] batchData = new byte[rows];
        if (isNullable) {
            byte[] batchNulls = new byte[rows];
            for (int i = 0; i < rows; ++i) {
                if (batch[i] == null) {
                    batchNulls[i] = 1;
                    numNulls++;
                } else {
                    batchNulls[i] = 0;
                    batchData[i] = batch[i];
                }
            }
            OffHeap.UNSAFE.copyMemory(batchNulls, OffHeap.BYTE_ARRAY_OFFSET, null, nullMap + appendIndex, rows);
        } else {
            for (int i = 0; i < rows; ++i) {
                batchData[i] = batch[i];
            }
        }
        OffHeap.UNSAFE.copyMemory(batchData, OffHeap.BYTE_ARRAY_OFFSET, null, data + appendIndex, rows);
        appendIndex += rows;
    }

    public void putByte(int rowId, byte value) {
        OffHeap.putByte(null, data + (long) rowId, value);
    }

    public byte getByte(int rowId) {
        return OffHeap.getByte(null, data + (long) rowId);
    }

    public Byte[] getByteColumn(int start, int end) {
        int length = end - start;
        Byte[] result = new Byte[length];
        byte[] batch = OffHeap.getByte(null, data + start, length);
        if (hasNull()) {
            for (int i = 0; i < length; ++i) {
                if (!isNullAt(start + i)) {
                    result[i] = batch[i];
                }
            }
        } else {
            for (int i = 0; i < length; ++i) {
                result[i] = batch[i];
            }
        }
        return result;
    }

    public int appendShort(short v) {
        reserve(appendIndex + 1);
        putShort(appendIndex, v);
        return appendIndex++;
    }

    public void appendShort(Short[] batch, boolean isNullable) {
        int rows = batch.length;
        reserve(appendIndex + rows);
        short[] batchData = new short[rows];
        if (isNullable) {
            byte[] batchNulls = new byte[rows];
            for (int i = 0; i < rows; ++i) {
                if (batch[i] == null) {
                    batchNulls[i] = 1;
                    numNulls++;
                } else {
                    batchNulls[i] = 0;
                    batchData[i] = batch[i];
                }
            }
            OffHeap.UNSAFE.copyMemory(batchNulls, OffHeap.BYTE_ARRAY_OFFSET, null, nullMap + appendIndex, rows);
        } else {
            for (int i = 0; i < rows; ++i) {
                batchData[i] = batch[i];
            }
        }
        OffHeap.UNSAFE.copyMemory(batchData, OffHeap.SHORT_ARRAY_OFFSET, null, data + 2L * appendIndex, 2L * rows);
        appendIndex += rows;
    }

    private void putShort(int rowId, short value) {
        OffHeap.putShort(null, data + 2L * rowId, value);
    }

    public short getShort(int rowId) {
        return OffHeap.getShort(null, data + 2L * rowId);
    }

    public Short[] getShortColumn(int start, int end) {
        int length = end - start;
        Short[] result = new Short[length];
        short[] batch = OffHeap.getShort(null, data + 2L * start, length);
        if (hasNull()) {
            for (int i = 0; i < length; ++i) {
                if (!isNullAt(start + i)) {
                    result[i] = batch[i];
                }
            }
        } else {
            for (int i = 0; i < length; ++i) {
                result[i] = batch[i];
            }
        }
        return result;
    }

    public int appendInt(int v) {
        reserve(appendIndex + 1);
        putInt(appendIndex, v);
        return appendIndex++;
    }

    public void appendInt(Integer[] batch, boolean isNullable) {
        int rows = batch.length;
        reserve(appendIndex + rows);
        int[] batchData = new int[rows];
        if (isNullable) {
            byte[] batchNulls = new byte[rows];
            for (int i = 0; i < rows; ++i) {
                if (batch[i] == null) {
                    batchNulls[i] = 1;
                    numNulls++;
                } else {
                    batchNulls[i] = 0;
                    batchData[i] = batch[i];
                }
            }
            OffHeap.UNSAFE.copyMemory(batchNulls, OffHeap.BYTE_ARRAY_OFFSET, null, nullMap + appendIndex, rows);
        } else {
            for (int i = 0; i < rows; ++i) {
                batchData[i] = batch[i];
            }
        }
        OffHeap.UNSAFE.copyMemory(batchData, OffHeap.INT_ARRAY_OFFSET, null, data + 4L * appendIndex, 4L * rows);
        appendIndex += rows;
    }

    private void putInt(int rowId, int value) {
        OffHeap.putInt(null, data + 4L * rowId, value);
    }

    public int getInt(int rowId) {
        return OffHeap.getInt(null, data + 4L * rowId);
    }

    public Integer[] getIntColumn(int start, int end) {
        int length = end - start;
        Integer[] result = new Integer[length];
        int[] batch = OffHeap.getInt(null, data + 4L * start, length);
        if (hasNull()) {
            for (int i = 0; i < length; ++i) {
                if (!isNullAt(start + i)) {
                    result[i] = batch[i];
                }
            }
        } else {
            for (int i = 0; i < length; ++i) {
                result[i] = batch[i];
            }
        }
        return result;
    }

    public int appendFloat(float v) {
        reserve(appendIndex + 1);
        putFloat(appendIndex, v);
        return appendIndex++;
    }

    public void appendFloat(Float[] batch, boolean isNullable) {
        int rows = batch.length;
        reserve(appendIndex + rows);
        float[] batchData = new float[rows];
        if (isNullable) {
            byte[] batchNulls = new byte[rows];
            for (int i = 0; i < rows; ++i) {
                if (batch[i] == null) {
                    batchNulls[i] = 1;
                    numNulls++;
                } else {
                    batchNulls[i] = 0;
                    batchData[i] = batch[i];
                }
            }
            OffHeap.UNSAFE.copyMemory(batchNulls, OffHeap.BYTE_ARRAY_OFFSET, null, nullMap + appendIndex, rows);
        } else {
            for (int i = 0; i < rows; ++i) {
                batchData[i] = batch[i];
            }
        }
        OffHeap.UNSAFE.copyMemory(batchData, OffHeap.FLOAT_ARRAY_OFFSET, null, data + 4L * appendIndex, 4L * rows);
        appendIndex += rows;
    }

    private void putFloat(int rowId, float value) {
        OffHeap.putFloat(null, data + rowId * 4L, value);
    }

    public float getFloat(int rowId) {
        return OffHeap.getFloat(null, data + rowId * 4L);
    }

    public Float[] getFloatColumn(int start, int end) {
        int length = end - start;
        Float[] result = new Float[length];
        float[] batch = OffHeap.getFloat(null, data + 4L * start, length);
        if (hasNull()) {
            for (int i = 0; i < length; ++i) {
                if (!isNullAt(start + i)) {
                    result[i] = batch[i];
                }
            }
        } else {
            for (int i = 0; i < length; ++i) {
                result[i] = batch[i];
            }
        }
        return result;
    }

    public int appendLong(long v) {
        reserve(appendIndex + 1);
        putLong(appendIndex, v);
        return appendIndex++;
    }

    public void appendLong(Long[] batch, boolean isNullable) {
        int rows = batch.length;
        reserve(appendIndex + rows);
        long[] batchData = new long[rows];
        if (isNullable) {
            byte[] batchNulls = new byte[rows];
            for (int i = 0; i < rows; ++i) {
                if (batch[i] == null) {
                    batchNulls[i] = 1;
                    numNulls++;
                } else {
                    batchNulls[i] = 0;
                    batchData[i] = batch[i];
                }
            }
            OffHeap.UNSAFE.copyMemory(batchNulls, OffHeap.BYTE_ARRAY_OFFSET, null, nullMap + appendIndex, rows);
        } else {
            for (int i = 0; i < rows; ++i) {
                batchData[i] = batch[i];
            }
        }
        OffHeap.UNSAFE.copyMemory(batchData, OffHeap.LONG_ARRAY_OFFSET, null, data + 8L * appendIndex, 8L * rows);
        appendIndex += rows;
    }

    private void putLong(int rowId, long value) {
        OffHeap.putLong(null, data + 8L * rowId, value);
    }

    public long getLong(int rowId) {
        return OffHeap.getLong(null, data + 8L * rowId);
    }

    public Long[] getLongColumn(int start, int end) {
        int length = end - start;
        Long[] result = new Long[length];
        long[] batch = OffHeap.getLong(null, data + 8L * start, length);
        if (hasNull()) {
            for (int i = 0; i < length; ++i) {
                if (!isNullAt(start + i)) {
                    result[i] = batch[i];
                }
            }
        } else {
            for (int i = 0; i < length; ++i) {
                result[i] = batch[i];
            }
        }
        return result;
    }

    public int appendDouble(double v) {
        reserve(appendIndex + 1);
        putDouble(appendIndex, v);
        return appendIndex++;
    }

    public void appendDouble(Double[] batch, boolean isNullable) {
        int rows = batch.length;
        reserve(appendIndex + rows);
        double[] batchData = new double[rows];
        if (isNullable) {
            byte[] batchNulls = new byte[rows];
            for (int i = 0; i < rows; ++i) {
                if (batch[i] == null) {
                    batchNulls[i] = 1;
                    numNulls++;
                } else {
                    batchNulls[i] = 0;
                    batchData[i] = batch[i];
                }
            }
            OffHeap.UNSAFE.copyMemory(batchNulls, OffHeap.BYTE_ARRAY_OFFSET, null, nullMap + appendIndex, rows);
        } else {
            for (int i = 0; i < rows; ++i) {
                batchData[i] = batch[i];
            }
        }
        OffHeap.UNSAFE.copyMemory(batchData, OffHeap.DOUBLE_ARRAY_OFFSET, null, data + 8L * appendIndex, 8L * rows);
        appendIndex += rows;
    }

    private void putDouble(int rowId, double value) {
        OffHeap.putDouble(null, data + rowId * 8L, value);
    }

    public double getDouble(int rowId) {
        return OffHeap.getDouble(null, data + rowId * 8L);
    }

    public Double[] getDoubleColumn(int start, int end) {
        int length = end - start;
        Double[] result = new Double[length];
        double[] batch = OffHeap.getDouble(null, data + 8L * start, length);
        if (hasNull()) {
            for (int i = 0; i < length; ++i) {
                if (!isNullAt(start + i)) {
                    result[i] = batch[i];
                }
            }
        } else {
            for (int i = 0; i < length; ++i) {
                result[i] = batch[i];
            }
        }
        return result;
    }

    public int appendBigInteger(BigInteger v) {
        reserve(appendIndex + 1);
        putBigInteger(appendIndex, v);
        return appendIndex++;
    }

    public void appendBigInteger(BigInteger[] batch, boolean isNullable) {
        reserve(appendIndex + batch.length);
        for (BigInteger v : batch) {
            if (v == null) {
                putNull(appendIndex);
                putBigInteger(appendIndex, BigInteger.ZERO);
            } else {
                putBigInteger(appendIndex, v);
            }
            appendIndex++;
        }
    }

    private void putBigInteger(int rowId, BigInteger v) {
        int typeSize = columnType.getTypeSize();
        byte[] bytes = TypeNativeBytes.getBigIntegerBytes(v);
        OffHeap.copyMemory(bytes, OffHeap.BYTE_ARRAY_OFFSET, null, data + (long) rowId * typeSize, typeSize);
    }

    public byte[] getBigIntegerBytes(int rowId) {
        int typeSize = columnType.getTypeSize();
        byte[] bytes = new byte[typeSize];
        OffHeap.copyMemory(null, data + (long) rowId * typeSize, bytes, OffHeap.BYTE_ARRAY_OFFSET, typeSize);
        return bytes;
    }

    public BigInteger getBigInteger(int rowId) {
        return TypeNativeBytes.getBigInteger(getBigIntegerBytes(rowId));
    }

    public BigInteger[] getBigIntegerColumn(int start, int end) {
        BigInteger[] result = new BigInteger[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                result[i - start] = getBigInteger(i);
            }
        }
        return result;
    }

    public int appendDecimal(BigDecimal v) {
        reserve(appendIndex + 1);
        putDecimal(appendIndex, v);
        return appendIndex++;
    }

    public void appendDecimal(BigDecimal[] batch, boolean isNullable) {
        reserve(appendIndex + batch.length);
        for (BigDecimal v : batch) {
            if (v == null) {
                putNull(appendIndex);
                putDecimal(appendIndex, new BigDecimal(0));
            } else {
                putDecimal(appendIndex, v);
            }
            appendIndex++;
        }
    }

    private void putDecimal(int rowId, BigDecimal v) {
        int typeSize = columnType.getTypeSize();
        byte[] bytes = TypeNativeBytes.getDecimalBytes(v, columnType.getScale(), typeSize);
        OffHeap.copyMemory(bytes, OffHeap.BYTE_ARRAY_OFFSET, null, data + (long) rowId * typeSize, typeSize);
    }

    public byte[] getDecimalBytes(int rowId) {
        int typeSize = columnType.getTypeSize();
        byte[] bytes = new byte[typeSize];
        OffHeap.copyMemory(null, data + (long) rowId * typeSize, bytes, OffHeap.BYTE_ARRAY_OFFSET, typeSize);
        return bytes;
    }

    public BigDecimal getDecimal(int rowId) {
        return TypeNativeBytes.getDecimal(getDecimalBytes(rowId), columnType.getScale());
    }

    public BigDecimal[] getDecimalColumn(int start, int end) {
        BigDecimal[] result = new BigDecimal[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                result[i - start] = getDecimal(i);
            }
        }
        return result;
    }

    public int appendDate(LocalDate v) {
        reserve(appendIndex + 1);
        putDate(appendIndex, v);
        return appendIndex++;
    }

    public void appendDate(LocalDate[] batch, boolean isNullable) {
        reserve(appendIndex + batch.length);
        for (LocalDate v : batch) {
            if (v == null) {
                putNull(appendIndex);
                putDate(appendIndex, LocalDate.MIN);
            } else {
                putDate(appendIndex, v);
            }
            appendIndex++;
        }
    }

    private void putDate(int rowId, LocalDate v) {
        if (columnType.isDateV2()) {
            int date = TypeNativeBytes.convertToDateV2(v.getYear(), v.getMonthValue(), v.getDayOfMonth());
            OffHeap.putInt(null, data + rowId * 4L, date);
        } else {
            long date = TypeNativeBytes.convertToDateTime(v.getYear(), v.getMonthValue(), v.getDayOfMonth(), 0,
                    0, 0, true);
            OffHeap.putLong(null, data + rowId * 8L, date);
        }
    }

    public LocalDate getDate(int rowId) {
        if (columnType.isDateV2()) {
            int date = OffHeap.getInt(null, data + rowId * 4L);
            return TypeNativeBytes.convertToJavaDateV2(date);
        } else {
            long date = OffHeap.getLong(null, data + rowId * 8L);
            return TypeNativeBytes.convertToJavaDateV1(date);
        }
    }

    public LocalDate[] getDateColumn(int start, int end) {
        LocalDate[] result = new LocalDate[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                result[i - start] = getDate(i);
            }
        }
        return result;
    }

    public Object[] getDateColumn(int start, int end, Class clz) {
        Object[] result = new Object[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                if (columnType.isDateV2()) {
                    result[i - start] = TypeNativeBytes.convertToJavaDateV2(
                            OffHeap.getInt(null, data + i * 4L), clz);
                } else {
                    result[i - start] = TypeNativeBytes.convertToJavaDateV1(
                            OffHeap.getLong(null, data + i * 8L), clz);
                }
            }
        }
        return result;
    }

    public int appendDateTime(LocalDateTime v) {
        reserve(appendIndex + 1);
        putDateTime(appendIndex, v);
        return appendIndex++;
    }

    public void appendDateTime(LocalDateTime[] batch, boolean isNullable) {
        reserve(appendIndex + batch.length);
        for (LocalDateTime v : batch) {
            if (v == null) {
                putNull(appendIndex);
                putDateTime(appendIndex, LocalDateTime.MIN);
            } else {
                putDateTime(appendIndex, v);
            }
            appendIndex++;
        }
    }

    public LocalDateTime getDateTime(int rowId) {
        long time = OffHeap.getLong(null, data + rowId * 8L);
        if (columnType.isDateTimeV2()) {
            return TypeNativeBytes.convertToJavaDateTimeV2(time);
        } else {
            return TypeNativeBytes.convertToJavaDateTimeV1(time);
        }
    }

    public LocalDateTime[] getDateTimeColumn(int start, int end) {
        LocalDateTime[] result = new LocalDateTime[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                result[i - start] = getDateTime(i);
            }
        }
        return result;
    }

    public Object[] getDateTimeColumn(int start, int end, Class clz) {
        Object[] result = new Object[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                long time = OffHeap.getLong(null, data + i * 8L);
                if (columnType.isDateTimeV2()) {
                    result[i - start] = TypeNativeBytes.convertToJavaDateTimeV2(time, clz);
                } else {
                    result[i - start] = TypeNativeBytes.convertToJavaDateTimeV1(time, clz);
                }
            }
        }
        return result;
    }

    private void putDateTime(int rowId, LocalDateTime v) {
        long time;
        if (columnType.isDateTimeV2()) {
            time = TypeNativeBytes.convertToDateTimeV2(v.getYear(), v.getMonthValue(), v.getDayOfMonth(), v.getHour(),
                    v.getMinute(), v.getSecond(), v.getNano() / 1000);
        } else {
            time = TypeNativeBytes.convertToDateTime(v.getYear(), v.getMonthValue(), v.getDayOfMonth(), v.getHour(),
                    v.getMinute(), v.getSecond(), false);
        }
        OffHeap.putLong(null, data + rowId * 8L, time);
    }

    private void putBytes(int rowId, byte[] src, int offset, int length) {
        OffHeap.copyMemory(src, OffHeap.BYTE_ARRAY_OFFSET + offset, null, data + rowId, length);
    }

    private byte[] getBytes(int rowId, int length) {
        byte[] array = new byte[length];
        OffHeap.copyMemory(null, data + rowId, array, OffHeap.BYTE_ARRAY_OFFSET, length);
        return array;
    }

    public int appendBytes(byte[] src, int offset, int length) {
        reserve(appendIndex + length);
        int result = appendIndex;
        putBytes(appendIndex, src, offset, length);
        appendIndex += length;
        return result;
    }

    public int appendString(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        return appendBytes(bytes, 0, bytes.length);
    }

    public int appendBytesAndOffset(byte[] src) {
        return appendBytesAndOffset(src, 0, src.length);
    }

    public int appendBytesAndOffset(byte[] src, int offset, int length) {
        int startOffset = childColumns[0].appendBytes(src, offset, length);
        reserve(appendIndex + 1);
        if (startOffset + length < 0) {
            throw new RuntimeException("String overflow, offset=" + startOffset + ", length=" + length);
        }
        OffHeap.putInt(null, offsets + 4L * appendIndex, startOffset + length);
        return appendIndex++;
    }

    public int appendStringAndOffset(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        return appendBytesAndOffset(bytes, 0, bytes.length);
    }

    public void appendStringAndOffset(String[] batch, boolean isNullable) {
        reserve(appendIndex + batch.length);
        for (String v : batch) {
            byte[] bytes;
            if (v == null) {
                putNull(appendIndex);
                bytes = new byte[0];
            } else {
                bytes = v.getBytes(StandardCharsets.UTF_8);
            }
            int startOffset = childColumns[0].appendBytes(bytes, 0, bytes.length);
            OffHeap.putInt(null, offsets + 4L * appendIndex, startOffset + bytes.length);
            appendIndex++;
        }
    }

    public void appendBinaryAndOffset(byte[][] batch, boolean isNullable) {
        reserve(appendIndex + batch.length);
        for (byte[] v : batch) {
            byte[] bytes = v;
            if (bytes == null) {
                putNull(appendIndex);
                bytes = new byte[0];
            }
            int startOffset = childColumns[0].appendBytes(bytes, 0, bytes.length);
            if (startOffset + bytes.length < 0) {
                throw new RuntimeException("Binary overflow, offset=" + startOffset + ", length=" + bytes.length);
            }
            OffHeap.putInt(null, offsets + 4L * appendIndex, startOffset + bytes.length);
            appendIndex++;
        }
    }

    public byte[] getBytesWithOffset(int rowId) {
        long endOffsetAddress = offsets + 4L * rowId;
        int startOffset = rowId == 0 ? 0 : OffHeap.getInt(null, endOffsetAddress - 4);
        int endOffset = OffHeap.getInt(null, endOffsetAddress);
        return childColumns[0].getBytes(startOffset, endOffset - startOffset);
    }

    public String getStringWithOffset(int rowId) {
        byte[] bytes = getBytesWithOffset(rowId);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String[] getStringColumn(int start, int end) {
        String[] result = new String[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                result[i - start] = getStringWithOffset(i);
            }
        }
        return result;
    }

    public int appendArray(List<ColumnValue> values) {
        int length = values.size();
        int startOffset = childColumns[0].appendIndex;
        for (ColumnValue v : values) {
            childColumns[0].appendValue(v);
        }
        reserve(appendIndex + 1);
        if (startOffset + length < 0) {
            throw new RuntimeException("Array overflow, offset=" + startOffset + ", length=" + length);
        }
        OffHeap.putLong(null, offsets + 8L * appendIndex, startOffset + length);
        return appendIndex++;
    }

    public void appendArray(List<Object>[] batch, boolean isNullable) {
        reserve(appendIndex + batch.length);
        int offset = childColumns[0].appendIndex;
        for (List<Object> v : batch) {
            if (v == null) {
                putNull(appendIndex);
            } else {
                offset += v.size();
            }
            OffHeap.putLong(null, offsets + 8L * appendIndex, offset);
            appendIndex++;
        }
        Object[] nested = newObjectContainerArray(childColumns[0].getColumnTyp(), offset - childColumns[0].appendIndex);
        int index = 0;
        for (List<Object> v : batch) {
            if (v != null) {
                for (Object o : v) {
                    nested[index++] = o;
                }
            }
        }
        childColumns[0].appendObjectColumn(nested, isNullable);
    }

    public ArrayList<Object> getArray(int rowId) {
        int startOffset = getArrayEndOffset(rowId - 1);
        int endOffset = getArrayEndOffset(rowId);
        ArrayList<Object> result = Lists.newArrayListWithExpectedSize(endOffset - startOffset);
        Collections.addAll(result, childColumns[0].getObjectColumn(startOffset, endOffset));
        return result;
    }

    public ArrayList<Object>[] getArrayColumn(int start, int end) {
        ArrayList<Object>[] result = new ArrayList[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                result[i - start] = getArray(i);
            }
        }
        return result;
    }

    public int appendMap(List<ColumnValue> keys, List<ColumnValue> values) {
        int length = keys.size();
        int startOffset = childColumns[0].appendIndex;
        for (ColumnValue k : keys) {
            childColumns[0].appendValue(k);
        }
        for (ColumnValue v : values) {
            childColumns[1].appendValue(v);
        }
        reserve(appendIndex + 1);
        if (startOffset + length < 0) {
            throw new RuntimeException("Map overflow, offset=" + startOffset + ", length=" + length);
        }
        OffHeap.putLong(null, offsets + 8L * appendIndex, startOffset + length);
        return appendIndex++;
    }

    public void appendMap(Map<Object, Object>[] batch, boolean isNullable) {
        reserve(appendIndex + batch.length);
        int offset = childColumns[0].appendIndex;
        for (Map<Object, Object> v : batch) {
            if (v == null) {
                putNull(appendIndex);
            } else {
                offset += v.size();
            }
            OffHeap.putLong(null, offsets + 8L * appendIndex, offset);
            appendIndex++;
        }
        Object[] keys = newObjectContainerArray(childColumns[0].getColumnTyp(), offset - childColumns[0].appendIndex);
        Object[] values = newObjectContainerArray(childColumns[1].getColumnTyp(), offset - childColumns[0].appendIndex);
        int index = 0;
        for (Map<Object, Object> v : batch) {
            if (v != null) {
                for (Map.Entry<Object, Object> entry : v.entrySet()) {
                    keys[index] = entry.getKey();
                    values[index] = entry.getValue();
                    index++;
                }
            }
        }
        childColumns[0].appendObjectColumn(keys, isNullable);
        childColumns[1].appendObjectColumn(values, isNullable);
    }

    public HashMap<Object, Object> getMap(int rowId) {
        int startOffset = getArrayEndOffset(rowId - 1);
        int endOffset = getArrayEndOffset(rowId);
        Object[] keys = childColumns[0].getObjectColumn(startOffset, endOffset);
        Object[] values = childColumns[1].getObjectColumn(startOffset, endOffset);
        HashMap<Object, Object> result = new HashMap<>(keys.length);
        for (int i = 0; i < keys.length; ++i) {
            result.put(keys[i], values[i]);
        }
        return result;
    }

    public HashMap<Object, Object>[] getMapColumn(int start, int end) {
        HashMap<Object, Object>[] result = new HashMap[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                result[i - start] = getMap(i);
            }
        }
        return result;
    }

    public int appendStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        if (values == null) {
            for (int i : structFieldIndex) {
                childColumns[i].appendValue(null);
            }
        } else {
            for (int i = 0; i < structFieldIndex.size(); ++i) {
                childColumns[structFieldIndex.get(i)].appendValue(values.get(i));
            }
        }
        reserve(appendIndex + 1);
        return appendIndex++;
    }

    public void appendStruct(Map<String, Object>[] batch, boolean isNullable) {
        reserve(appendIndex + batch.length);
        Object[][] columnData = new Object[childColumns.length][];
        for (int j = 0; j < childColumns.length; ++j) {
            columnData[j] = newObjectContainerArray(childColumns[j].getColumnTyp(), batch.length);
        }
        int index = 0;
        for (Map<String, Object> v : batch) {
            if (v == null) {
                putNull(appendIndex);
                for (int j = 0; j < childColumns.length; ++j) {
                    columnData[j][index] = null;
                }
            } else {
                for (int j = 0; j < childColumns.length; ++j) {
                    columnData[j][index] = v.get(childColumns[j].getColumnTyp().name());
                }
            }
            index++;
            appendIndex++;
        }
        for (int j = 0; j < childColumns.length; ++j) {
            childColumns[j].appendObjectColumn(columnData[j], isNullable);
        }
    }

    public HashMap<String, Object> getStruct(int rowId) {
        HashMap<String, Object> result = new HashMap<>();
        for (VectorColumn column : childColumns) {
            result.put(column.getColumnTyp().name(), column.getObjectColumn(rowId, rowId + 1)[0]);
        }
        return result;
    }

    public HashMap<String, Object>[] getStructColumn(int start, int end) {
        HashMap<String, Object>[] result = new HashMap[end - start];
        for (int i = start; i < end; ++i) {
            if (!isNullAt(i)) {
                result[i - start] = getStruct(i);
            }
        }
        return result;
    }

    public void updateMeta(VectorColumn meta) {
        if (columnType.isUnsupported()) {
            meta.appendLong(0);
        } else if (columnType.isStringType()) {
            meta.appendLong(nullMap);
            meta.appendLong(offsets);
            meta.appendLong(childColumns[0].data);
        } else if (columnType.isComplexType()) {
            meta.appendLong(nullMap);
            if (columnType.isArray() || columnType.isMap()) {
                meta.appendLong(offsets);
            }
            for (VectorColumn c : childColumns) {
                c.updateMeta(meta);
            }
        } else {
            meta.appendLong(nullMap);
            meta.appendLong(data);
        }
    }

    public void appendNativeValue(NativeColumnValue o) {
        ColumnType.Type typeValue = columnType.getType();
        if (o == null || o.isNull()) {
            appendNull(typeValue);
            return;
        }
        NativeValue nativeValue = o.getNativeValue(typeValue);
        if (nativeValue != null && columnType.isStringType()) {
            int byteLength = nativeValue.length;
            VectorColumn bytesColumn = childColumns[0];
            int startOffset = bytesColumn.appendIndex;
            bytesColumn.reserve(startOffset + byteLength);
            OffHeap.copyMemory(nativeValue.baseObject, nativeValue.offset,
                    null, bytesColumn.data + startOffset, byteLength);
            bytesColumn.appendIndex += byteLength;
            OffHeap.putInt(null, offsets + 4L * appendIndex, startOffset + byteLength);
            appendIndex++;
        } else {
            // can't get native value, fall back to materialized value
            appendValue((ColumnValue) o);
        }
    }

    public Object[] newObjectContainerArray(int size) {
        return newObjectContainerArray(columnType.getType(), size);
    }

    public Object[] newObjectContainerArray(ColumnType.Type type, int size) {
        switch (type) {
            case BOOLEAN:
                return new Boolean[size];
            case TINYINT:
                return new Byte[size];
            case SMALLINT:
                return new Short[size];
            case INT:
                return new Integer[size];
            case BIGINT:
                return new Long[size];
            case LARGEINT:
                return new BigInteger[size];
            case FLOAT:
                return new Float[size];
            case DOUBLE:
                return new Double[size];
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new BigDecimal[size];
            case DATE:
            case DATEV2:
                return new LocalDate[size];
            case DATETIME:
            case DATETIMEV2:
                return new LocalDateTime[size];
            case CHAR:
            case VARCHAR:
            case STRING:
                return new String[size];
            case ARRAY:
                return new ArrayList[size];
            case MAP:
            case STRUCT:
                return new HashMap[size];
            default:
                throw new RuntimeException("Unknown type value: " + type);
        }
    }

    public void appendObjectColumn(Object[] batch, boolean isNullable) {
        switch (columnType.getType()) {
            case BOOLEAN:
                appendBoolean((Boolean[]) batch, isNullable);
                break;
            case TINYINT:
                appendByte((Byte[]) batch, isNullable);
                break;
            case SMALLINT:
                appendShort((Short[]) batch, isNullable);
                break;
            case INT:
                appendInt((Integer[]) batch, isNullable);
                break;
            case BIGINT:
                appendLong((Long[]) batch, isNullable);
                break;
            case LARGEINT:
                appendBigInteger((BigInteger[]) batch, isNullable);
                break;
            case FLOAT:
                appendFloat((Float[]) batch, isNullable);
                break;
            case DOUBLE:
                appendDouble((Double[]) batch, isNullable);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                appendDecimal((BigDecimal[]) batch, isNullable);
                break;
            case DATE:
            case DATEV2:
                appendDate((LocalDate[]) batch, isNullable);
                break;
            case DATETIME:
            case DATETIMEV2:
                appendDateTime((LocalDateTime[]) batch, isNullable);
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
                if (batch instanceof String[]) {
                    appendStringAndOffset((String[]) batch, isNullable);
                } else {
                    appendBinaryAndOffset((byte[][]) batch, isNullable);
                }
                break;
            case ARRAY:
                appendArray((List<Object>[]) batch, isNullable);
                break;
            case MAP:
                appendMap((Map<Object, Object>[]) batch, isNullable);
                break;
            case STRUCT:
                appendStruct((Map<String, Object>[]) batch, isNullable);
                break;
            default:
                throw new RuntimeException("Unknown type value: " + columnType.getType());
        }
    }

    public Object[] getObjectColumn(int start, int end) {
        switch (columnType.getType()) {
            case BOOLEAN:
                return getBooleanColumn(start, end);
            case TINYINT:
                return getByteColumn(start, end);
            case SMALLINT:
                return getShortColumn(start, end);
            case INT:
                return getIntColumn(start, end);
            case BIGINT:
                return getLongColumn(start, end);
            case LARGEINT:
                return getBigIntegerColumn(start, end);
            case FLOAT:
                return getFloatColumn(start, end);
            case DOUBLE:
                return getDoubleColumn(start, end);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return getDecimalColumn(start, end);
            case DATE:
            case DATEV2:
                return getDateColumn(start, end);
            case DATETIME:
            case DATETIMEV2:
                return getDateTimeColumn(start, end);
            case CHAR:
            case VARCHAR:
            case STRING:
                return getStringColumn(start, end);
            case ARRAY:
                return getArrayColumn(start, end);
            case MAP:
                return getMapColumn(start, end);
            case STRUCT:
                return getStructColumn(start, end);
            default:
                throw new RuntimeException("Unknown type value: " + columnType.getType());
        }
    }

    public void appendValue(ColumnValue o) {
        ColumnType.Type typeValue = columnType.getType();
        if (o == null || o.isNull()) {
            appendNull(typeValue);
            return;
        }

        switch (typeValue) {
            case BOOLEAN:
                appendBoolean(o.getBoolean());
                break;
            case TINYINT:
                appendByte(o.getByte());
                break;
            case SMALLINT:
                appendShort(o.getShort());
                break;
            case INT:
                appendInt(o.getInt());
                break;
            case BIGINT:
                appendLong(o.getLong());
                break;
            case LARGEINT:
                appendBigInteger(o.getBigInteger());
                break;
            case FLOAT:
                appendFloat(o.getFloat());
                break;
            case DOUBLE:
                appendDouble(o.getDouble());
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                appendDecimal(o.getDecimal());
                break;
            case DATE:
            case DATEV2:
                appendDate(o.getDate());
                break;
            case DATETIME:
            case DATETIMEV2:
                appendDateTime(o.getDateTime());
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
                if (o.canGetStringAsBytes()) {
                    appendBytesAndOffset(o.getStringAsBytes());
                } else {
                    appendStringAndOffset(o.getString());
                }
                break;
            case BINARY:
                appendBytesAndOffset(o.getBytes());
                break;
            case ARRAY: {
                List<ColumnValue> values = new ArrayList<>();
                o.unpackArray(values);
                appendArray(values);
                break;
            }
            case MAP: {
                List<ColumnValue> keys = new ArrayList<>();
                List<ColumnValue> values = new ArrayList<>();
                o.unpackMap(keys, values);
                appendMap(keys, values);
                break;
            }
            case STRUCT: {
                List<ColumnValue> values = new ArrayList<>();
                o.unpackStruct(structFieldIndex, values);
                appendStruct(structFieldIndex, values);
                break;
            }
            default:
                throw new RuntimeException("Unknown type value: " + typeValue);
        }
    }

    // for test only.
    public void dump(StringBuilder sb, int i) {
        if (isNullAt(i)) {
            sb.append("NULL");
            return;
        }

        ColumnType.Type typeValue = columnType.getType();
        switch (typeValue) {
            case BOOLEAN:
                sb.append(getBoolean(i));
                break;
            case TINYINT:
                sb.append(getByte(i));
                break;
            case SMALLINT:
                sb.append(getShort(i));
                break;
            case INT:
                sb.append(getInt(i));
                break;
            case BIGINT:
                sb.append(getLong(i));
                break;
            case LARGEINT:
                sb.append(getBigInteger(i));
                break;
            case FLOAT:
                sb.append(getFloat(i));
                break;
            case DOUBLE:
                sb.append(getDouble(i));
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                sb.append(getDecimal(i));
                break;
            case DATE:
            case DATEV2:
                sb.append(getDate(i));
                break;
            case DATETIME:
            case DATETIMEV2:
                sb.append(getDateTime(i));
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
                sb.append(getStringWithOffset(i));
                break;
            case ARRAY: {
                int begin = getArrayEndOffset(i - 1);
                int end = getArrayEndOffset(i);
                sb.append("[");
                for (int rowId = begin; rowId < end; rowId++) {
                    if (rowId != begin) {
                        sb.append(",");
                    }
                    childColumns[0].dump(sb, rowId);
                }
                sb.append("]");
                break;
            }
            case MAP: {
                int begin = getArrayEndOffset(i - 1);
                int end = getArrayEndOffset(i);
                sb.append("{");
                VectorColumn key = childColumns[0];
                VectorColumn value = childColumns[1];
                for (int rowId = begin; rowId < end; rowId++) {
                    if (rowId != begin) {
                        sb.append(",");
                    }
                    if (key.columnType.isStringType()) {
                        sb.append("\"");
                    }
                    key.dump(sb, rowId);
                    if (key.columnType.isStringType()) {
                        sb.append("\"");
                    }
                    sb.append(":");
                    if (value.columnType.isStringType()) {
                        sb.append("\"");
                    }
                    value.dump(sb, rowId);
                    if (value.columnType.isStringType()) {
                        sb.append("\"");
                    }
                }
                sb.append("}");
                break;
            }
            case STRUCT: {
                sb.append("{");
                for (int fieldIndex = 0; fieldIndex < childColumns.length; ++fieldIndex) {
                    VectorColumn child = childColumns[fieldIndex];
                    if (fieldIndex != 0) {
                        sb.append(",");
                    }
                    sb.append("\"").append(child.columnType.getName()).append("\":");
                    if (child.columnType.isStringType()) {
                        sb.append("\"");
                    }
                    child.dump(sb, i);
                    if (child.columnType.isStringType()) {
                        sb.append("\"");
                    }
                }
                sb.append("}");
                break;
            }
            default:
                throw new RuntimeException("Unknown type value: " + typeValue);
        }
    }
}
