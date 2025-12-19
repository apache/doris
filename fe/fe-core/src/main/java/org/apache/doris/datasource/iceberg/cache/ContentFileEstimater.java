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

package org.apache.doris.datasource.iceberg.cache;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.StructLike;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Utility to estimate the JVM weight of Iceberg {@link ContentFile} objects.
 */
public final class ContentFileEstimater {
    private static final long LIST_BASE_WEIGHT = 48L;
    private static final long OBJECT_REFERENCE_WEIGHT = 8L;
    private static final long CONTENT_FILE_BASE_WEIGHT = 256L;
    private static final long STRING_BASE_WEIGHT = 40L;
    private static final long CHAR_BYTES = 2L;
    private static final long BYTE_BUFFER_BASE_WEIGHT = 16L;
    private static final long MAP_BASE_WEIGHT = 48L;
    private static final long MAP_ENTRY_OVERHEAD = 24L;
    private static final long LONG_OBJECT_WEIGHT = 24L;
    private static final long INT_OBJECT_WEIGHT = 16L;
    private static final long PARTITION_BASE_WEIGHT = 48L;
    private static final long PARTITION_VALUE_BASE_WEIGHT = 8L;

    private ContentFileEstimater() {
    }

    public static long estimate(List<? extends ContentFile<?>> files) {
        return listReferenceWeight(files) + estimateContentFilesWeight(files);
    }

    private static long listReferenceWeight(List<?> files) {
        if (files == null || files.isEmpty()) {
            return 0L;
        }
        return LIST_BASE_WEIGHT + (long) files.size() * OBJECT_REFERENCE_WEIGHT;
    }

    private static long estimateContentFilesWeight(List<? extends ContentFile<?>> files) {
        long total = 0L;
        if (files == null) {
            return 0L;
        }
        for (ContentFile<?> file : files) {
            total += estimateContentFileWeight(file);
        }
        return total;
    }

    private static long estimateContentFileWeight(ContentFile<?> file) {
        if (file == null) {
            return 0L;
        }

        long weight = CONTENT_FILE_BASE_WEIGHT;
        weight += charSequenceWeight(file.path());
        weight += stringWeight(file.manifestLocation());
        weight += byteBufferWeight(file.keyMetadata());
        weight += partitionWeight(file.partition());

        weight += numericMapWeight(file.columnSizes());
        weight += numericMapWeight(file.valueCounts());
        weight += numericMapWeight(file.nullValueCounts());
        weight += numericMapWeight(file.nanValueCounts());
        weight += byteBufferMapWeight(file.lowerBounds());
        weight += byteBufferMapWeight(file.upperBounds());

        weight += listWeight(file.splitOffsets(), LONG_OBJECT_WEIGHT);
        weight += listWeight(file.equalityFieldIds(), INT_OBJECT_WEIGHT);

        weight += optionalLongWeight(file.pos());
        weight += optionalLongWeight(file.dataSequenceNumber());
        weight += optionalLongWeight(file.fileSequenceNumber());
        weight += optionalLongWeight(file.firstRowId());
        weight += optionalIntWeight(file.sortOrderId());

        if (file instanceof DeleteFile) {
            DeleteFile deleteFile = (DeleteFile) file;
            weight += stringWeight(deleteFile.referencedDataFile());
            weight += optionalLongWeight(deleteFile.contentOffset());
            weight += optionalLongWeight(deleteFile.contentSizeInBytes());
        }

        return weight;
    }

    private static long listWeight(List<? extends Number> list, long elementWeight) {
        if (list == null || list.isEmpty()) {
            return 0L;
        }
        return LIST_BASE_WEIGHT + (long) list.size() * (OBJECT_REFERENCE_WEIGHT + elementWeight);
    }

    private static long numericMapWeight(Map<Integer, Long> map) {
        if (map == null || map.isEmpty()) {
            return 0L;
        }
        return MAP_BASE_WEIGHT + (long) map.size() * (MAP_ENTRY_OVERHEAD + LONG_OBJECT_WEIGHT);
    }

    private static long byteBufferMapWeight(Map<Integer, ByteBuffer> map) {
        if (map == null || map.isEmpty()) {
            return 0L;
        }
        long weight = MAP_BASE_WEIGHT + (long) map.size() * MAP_ENTRY_OVERHEAD;
        for (ByteBuffer buffer : map.values()) {
            weight += byteBufferWeight(buffer);
        }
        return weight;
    }

    private static long partitionWeight(StructLike partition) {
        if (partition == null) {
            return 0L;
        }
        long weight = PARTITION_BASE_WEIGHT + (long) partition.size() * PARTITION_VALUE_BASE_WEIGHT;
        for (int i = 0; i < partition.size(); i++) {
            Object value = partition.get(i, Object.class);
            weight += estimateValueWeight(value);
        }
        return weight;
    }

    private static long estimateValueWeight(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof CharSequence) {
            return charSequenceWeight((CharSequence) value);
        } else if (value instanceof byte[]) {
            return BYTE_BUFFER_BASE_WEIGHT + ((byte[]) value).length;
        } else if (value instanceof ByteBuffer) {
            return byteBufferWeight((ByteBuffer) value);
        } else if (value instanceof Long || value instanceof Double) {
            return LONG_OBJECT_WEIGHT;
        } else if (value instanceof Integer || value instanceof Float) {
            return INT_OBJECT_WEIGHT;
        } else if (value instanceof Short || value instanceof Character) {
            return 4L;
        } else if (value instanceof Boolean) {
            return 1L;
        }
        return OBJECT_REFERENCE_WEIGHT;
    }

    private static long charSequenceWeight(CharSequence value) {
        if (value == null) {
            return 0L;
        }
        return STRING_BASE_WEIGHT + (long) value.length() * CHAR_BYTES;
    }

    private static long stringWeight(String value) {
        if (value == null) {
            return 0L;
        }
        return STRING_BASE_WEIGHT + (long) value.length() * CHAR_BYTES;
    }

    private static long byteBufferWeight(ByteBuffer buffer) {
        if (buffer == null) {
            return 0L;
        }
        return BYTE_BUFFER_BASE_WEIGHT + buffer.remaining();
    }

    private static long optionalLongWeight(Long value) {
        return value == null ? 0L : LONG_OBJECT_WEIGHT;
    }

    private static long optionalIntWeight(Integer value) {
        return value == null ? 0L : INT_OBJECT_WEIGHT;
    }
}
