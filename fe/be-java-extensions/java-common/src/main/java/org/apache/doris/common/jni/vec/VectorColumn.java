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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public VectorColumn(ColumnType columnType, int capacity) {
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
    public VectorColumn(long address, int capacity, ColumnType columnType) {
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

    // restore block column
    public VectorColumn(ColumnType columnType, int numRows, long columnMetaAddress) {
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
            for (int i = 0; i < numRows; ++i) {
                if (isNullAt(i)) {
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

    public boolean isNullAt(int rowId) {
        if (nullMap == 0) {
            return false;
        } else {
            return OffHeap.getByte(null, nullMap + rowId) == 1;
        }
    }

    public boolean hasNull() {
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
            case DATEV2:
                return appendDate(LocalDate.MIN);
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

    private void putBoolean(int rowId, boolean value) {
        OffHeap.putByte(null, data + rowId, (byte) ((value) ? 1 : 0));
    }

    public boolean getBoolean(int rowId) {
        return OffHeap.getByte(null, data + rowId) == 1;
    }

    public int appendByte(byte v) {
        reserve(appendIndex + 1);
        putByte(appendIndex, v);
        return appendIndex++;
    }

    public void putByte(int rowId, byte value) {
        OffHeap.putByte(null, data + (long) rowId, value);
    }

    public byte getByte(int rowId) {
        return OffHeap.getByte(null, data + (long) rowId);
    }

    public int appendShort(short v) {
        reserve(appendIndex + 1);
        putShort(appendIndex, v);
        return appendIndex++;
    }

    private void putShort(int rowId, short value) {
        OffHeap.putShort(null, data + 2L * rowId, value);
    }

    public short getShort(int rowId) {
        return OffHeap.getShort(null, data + 2L * rowId);
    }

    public int appendInt(int v) {
        reserve(appendIndex + 1);
        putInt(appendIndex, v);
        return appendIndex++;
    }

    private void putInt(int rowId, int value) {
        OffHeap.putInt(null, data + 4L * rowId, value);
    }

    public int getInt(int rowId) {
        return OffHeap.getInt(null, data + 4L * rowId);
    }

    public int appendFloat(float v) {
        reserve(appendIndex + 1);
        putFloat(appendIndex, v);
        return appendIndex++;
    }

    private void putFloat(int rowId, float value) {
        OffHeap.putFloat(null, data + rowId * 4L, value);
    }

    public float getFloat(int rowId) {
        return OffHeap.getFloat(null, data + rowId * 4L);
    }

    public int appendLong(long v) {
        reserve(appendIndex + 1);
        putLong(appendIndex, v);
        return appendIndex++;
    }

    private void putLong(int rowId, long value) {
        OffHeap.putLong(null, data + 8L * rowId, value);
    }

    public long getLong(int rowId) {
        return OffHeap.getLong(null, data + 8L * rowId);
    }

    public int appendDouble(double v) {
        reserve(appendIndex + 1);
        putDouble(appendIndex, v);
        return appendIndex++;
    }

    private void putDouble(int rowId, double value) {
        OffHeap.putDouble(null, data + rowId * 8L, value);
    }

    public double getDouble(int rowId) {
        return OffHeap.getDouble(null, data + rowId * 8L);
    }

    public int appendBigInteger(BigInteger v) {
        reserve(appendIndex + 1);
        putBigInteger(appendIndex, v);
        return appendIndex++;
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

    public int appendDecimal(BigDecimal v) {
        reserve(appendIndex + 1);
        putDecimal(appendIndex, v);
        return appendIndex++;
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

    public int appendDate(LocalDate v) {
        reserve(appendIndex + 1);
        putDate(appendIndex, v);
        return appendIndex++;
    }

    private void putDate(int rowId, LocalDate v) {
        int date = TypeNativeBytes.convertToDateV2(v.getYear(), v.getMonthValue(), v.getDayOfMonth());
        OffHeap.putInt(null, data + rowId * 4L, date);
    }

    public LocalDate getDate(int rowId) {
        int date = OffHeap.getInt(null, data + rowId * 4L);
        return TypeNativeBytes.convertToJavaDate(date);
    }

    public int appendDateTime(LocalDateTime v) {
        reserve(appendIndex + 1);
        putDateTime(appendIndex, v);
        return appendIndex++;
    }

    public LocalDateTime getDateTime(int rowId) {
        long time = OffHeap.getLong(null, data + rowId * 8L);
        return TypeNativeBytes.convertToJavaDateTime(time);
    }

    private void putDateTime(int rowId, LocalDateTime v) {
        long time = TypeNativeBytes.convertToDateTimeV2(v.getYear(), v.getMonthValue(), v.getDayOfMonth(), v.getHour(),
                v.getMinute(), v.getSecond(), v.getNano() / 1000);
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
        OffHeap.putInt(null, offsets + 4L * appendIndex, startOffset + length);
        return appendIndex++;
    }

    public int appendStringAndOffset(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        return appendBytesAndOffset(bytes, 0, bytes.length);
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

    public int appendArray(List<ColumnValue> values) {
        int length = values.size();
        int startOffset = childColumns[0].appendIndex;
        for (ColumnValue v : values) {
            childColumns[0].appendValue(v);
        }
        reserve(appendIndex + 1);
        OffHeap.putLong(null, offsets + 8L * appendIndex, startOffset + length);
        return appendIndex++;
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
        OffHeap.putInt(null, offsets + 8L * appendIndex, startOffset + length);
        return appendIndex++;
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
            case DATEV2:
                appendDate(o.getDate());
                break;
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
            case DATEV2:
                sb.append(getDate(i));
                break;
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
