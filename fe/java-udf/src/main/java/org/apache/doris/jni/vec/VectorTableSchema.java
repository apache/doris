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

import org.apache.doris.jni.utils.OffHeap;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.annotations.VisibleForTesting;

import java.nio.charset.StandardCharsets;

public class VectorTableSchema {

    // Upper limit for the maximum capacity for this column.
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 15;
    private final String[] fields;
    private final TPrimitiveType[] schemaTypes;
    private final Integer fieldSize;
    // Data address
    private long offsets;
    private int capacity;
    private int schemaLength;
    private long schemaOffset;

    public VectorTableSchema(String[] fields, TPrimitiveType[] schemaTypes) {
        this.fields = fields;
        this.schemaTypes = schemaTypes;
        this.fieldSize = fields.length;
        this.offsets = 0;
        this.capacity = 0;
        String tableSchema = fillSchema();
        appendString(tableSchema);
    }

    private long reserveCapacity(int newCapacity) {
        long oldOffsetSize = capacity * 4L;
        long newOffsetSize = newCapacity * 4L;
        this.offsets = OffHeap.reallocateMemory(offsets, oldOffsetSize, newOffsetSize);
        capacity = newCapacity;
        return offsets;
    }

    /**
     * Fill schema.
     * Format like: field1,field2,field3#type1,type2,type3
     */
    public String fillSchema() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldSize; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(fields[i]);
        }
        sb.append("#");
        for (int i = 0; i < fieldSize; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(schemaTypes[i].getValue());
        }
        return sb.toString();
    }

    private void appendString(String tableSchema) {
        byte[] bytes = tableSchema.getBytes(StandardCharsets.UTF_8);
        schemaLength = bytes.length;
        schemaOffset = reserve(schemaLength);
        appendBytes(bytes, 0, schemaLength, schemaOffset);
    }

    public void appendBytes(byte[] src, int offset, int length, long dstOffset) {
        putBytes(dstOffset, src, offset, length);
    }

    private void putBytes(long dstOffset, byte[] src, int offset, int length) {
        OffHeap.copyMemory(src, OffHeap.BYTE_ARRAY_OFFSET + offset, null, dstOffset, length);
    }

    private long reserve(int requiredCapacity) {
        int newCapacity = (int) Math.min(MAX_CAPACITY, requiredCapacity * 2L);
        return reserveCapacity(newCapacity);
    }

    public String getString(int rowId) {
        byte[] bytes = getBytes(rowId, schemaLength, schemaOffset);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private byte[] getBytes(int rowId, int length, long offsets) {
        byte[] array = new byte[length];
        OffHeap.copyMemory(null, offsets + rowId, array, OffHeap.BYTE_ARRAY_OFFSET, length);
        return array;
    }

    public void close() {
        OffHeap.freeMemory(schemaOffset);
    }

    @VisibleForTesting
    public String[] getFields() {
        return fields;
    }

    @VisibleForTesting
    public TPrimitiveType[] getSchemaTypes() {
        return schemaTypes;
    }

}
