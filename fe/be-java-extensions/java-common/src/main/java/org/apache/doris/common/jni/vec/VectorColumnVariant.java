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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Off-heap encoded Variant storage used by {@link VectorColumn}.
 *
 * <p>The layout mirrors ColumnVariantV2::EncodedDataView: a deduplicated metadata dictionary,
 * one metadata id per row, and one encoded value per row.</p>
 */
final class VectorColumnVariant {
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 15;
    private static final byte[] EMPTY_METADATA = new byte[] {1, 0, 0};
    private static final byte[] NULL_VALUE = new byte[] {0};

    private final Map<ByteArrayKey, Integer> metadataIds = new HashMap<>();

    private long metadataOffsets;
    private long metadataBytes;
    private long rowMetadataIds;
    private long valueOffsets;
    private long valueBytes;

    private int rowCapacity;
    private int metadataBytesCapacity;
    private int valueBytesCapacity;

    private int metadataCount;
    private int metadataBytesSize;
    private int rowCount;
    private int valueBytesSize;

    void reserveRows(int requiredCapacity) {
        if (requiredCapacity <= rowCapacity) {
            return;
        }
        int newCapacity = growCapacity(rowCapacity, requiredCapacity);
        long oldOffsetsSize = rowCapacity == 0 ? 0 : (rowCapacity + 1L) * Integer.BYTES;
        metadataOffsets = OffHeap.reallocateMemory(
                metadataOffsets, oldOffsetsSize,
                (newCapacity + 1L) * Integer.BYTES);
        rowMetadataIds = OffHeap.reallocateMemory(
                rowMetadataIds, (long) rowCapacity * Integer.BYTES,
                (long) newCapacity * Integer.BYTES);
        valueOffsets = OffHeap.reallocateMemory(
                valueOffsets, oldOffsetsSize,
                (newCapacity + 1L) * Integer.BYTES);
        if (rowCapacity == 0) {
            OffHeap.putInt(null, metadataOffsets, 0);
            OffHeap.putInt(null, valueOffsets, 0);
        }
        rowCapacity = newCapacity;
    }

    void append(byte[] metadata, byte[] value) {
        Objects.requireNonNull(metadata, "Variant metadata cannot be null");
        Objects.requireNonNull(value, "Variant value cannot be null");
        reserveRows(rowCount + 1);

        Integer metadataId = metadataIds.get(new ByteArrayKey(metadata));
        if (metadataId == null) {
            metadataId = appendMetadata(metadata);
        }
        OffHeap.putInt(null, rowMetadataIds + (long) rowCount * Integer.BYTES, metadataId);

        int requiredValueBytes = checkedSize("value", valueBytesSize, value.length);
        reserveValueBytes(requiredValueBytes);
        OffHeap.copyMemory(value, OffHeap.BYTE_ARRAY_OFFSET, null, valueBytes + valueBytesSize, value.length);
        valueBytesSize = requiredValueBytes;
        rowCount++;
        OffHeap.putInt(null, valueOffsets + (long) rowCount * Integer.BYTES, valueBytesSize);
    }

    void appendNull() {
        append(EMPTY_METADATA, NULL_VALUE);
    }

    void updateMeta(VectorColumn meta) {
        meta.appendLong(metadataCount);
        meta.appendLong(metadataOffsets);
        meta.appendLong(metadataBytes);
        meta.appendLong(rowMetadataIds);
        meta.appendLong(valueOffsets);
        meta.appendLong(valueBytes);
    }

    void reset() {
        metadataIds.clear();
        metadataCount = 0;
        metadataBytesSize = 0;
        rowCount = 0;
        valueBytesSize = 0;
        if (rowCapacity > 0) {
            OffHeap.putInt(null, metadataOffsets, 0);
            OffHeap.putInt(null, valueOffsets, 0);
        }
    }

    void close() {
        free(metadataOffsets);
        free(metadataBytes);
        free(rowMetadataIds);
        free(valueOffsets);
        free(valueBytes);
        metadataOffsets = 0;
        metadataBytes = 0;
        rowMetadataIds = 0;
        valueOffsets = 0;
        valueBytes = 0;
        rowCapacity = 0;
        metadataBytesCapacity = 0;
        valueBytesCapacity = 0;
        reset();
    }

    private int appendMetadata(byte[] metadata) {
        int requiredMetadataBytes = checkedSize("metadata", metadataBytesSize, metadata.length);
        reserveMetadataBytes(requiredMetadataBytes);
        OffHeap.copyMemory(
                metadata, OffHeap.BYTE_ARRAY_OFFSET, null, metadataBytes + metadataBytesSize, metadata.length);
        metadataBytesSize = requiredMetadataBytes;
        metadataCount++;
        OffHeap.putInt(
                null, metadataOffsets + (long) metadataCount * Integer.BYTES, metadataBytesSize);

        int metadataId = metadataCount - 1;
        metadataIds.put(new ByteArrayKey(Arrays.copyOf(metadata, metadata.length)), metadataId);
        return metadataId;
    }

    private void reserveMetadataBytes(int requiredCapacity) {
        if (requiredCapacity <= metadataBytesCapacity) {
            return;
        }
        int newCapacity = growCapacity(metadataBytesCapacity, requiredCapacity);
        metadataBytes = OffHeap.reallocateMemory(metadataBytes, metadataBytesCapacity, newCapacity);
        metadataBytesCapacity = newCapacity;
    }

    private void reserveValueBytes(int requiredCapacity) {
        if (requiredCapacity <= valueBytesCapacity) {
            return;
        }
        int newCapacity = growCapacity(valueBytesCapacity, requiredCapacity);
        valueBytes = OffHeap.reallocateMemory(valueBytes, valueBytesCapacity, newCapacity);
        valueBytesCapacity = newCapacity;
    }

    private static int checkedSize(String component, int currentSize, int appendedSize) {
        long requiredSize = (long) currentSize + appendedSize;
        if (requiredSize > MAX_CAPACITY) {
            throw new RuntimeException("Variant " + component + " buffer exceeds the Java JNI size limit");
        }
        return (int) requiredSize;
    }

    private static int growCapacity(int currentCapacity, int requiredCapacity) {
        long doubledCapacity = Math.max(1L, currentCapacity * 2L);
        int newCapacity = (int) Math.min(MAX_CAPACITY, Math.max(doubledCapacity, requiredCapacity));
        if (newCapacity < requiredCapacity) {
            throw new RuntimeException("Cannot reserve enough bytes for Variant JNI data");
        }
        return newCapacity;
    }

    private static void free(long address) {
        if (address != 0) {
            OffHeap.freeMemory(address);
        }
    }

    private static final class ByteArrayKey {
        private final byte[] bytes;
        private final int hashCode;

        private ByteArrayKey(byte[] bytes) {
            this.bytes = bytes;
            this.hashCode = Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ByteArrayKey
                    && Arrays.equals(bytes, ((ByteArrayKey) other).bytes);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
