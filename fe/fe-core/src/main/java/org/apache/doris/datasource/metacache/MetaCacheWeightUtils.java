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

package org.apache.doris.datasource.metacache;

import org.apache.doris.datasource.NameMapping;

/**
 * Overflow-safe helpers for approximate external metadata cache weights.
 *
 * <p>Weights are byte-like units, not exact retained heap sizes: structural constants are
 * rounded upward from offline calibration and do not model the exact layout of the active JVM
 * or of third-party SDK classes. {@code max-weight} is therefore an estimated retained-cache
 * admission budget rather than a precise heap limit; soft value references remain the safety
 * net for residual under-estimation.
 */
public final class MetaCacheWeightUtils {
    private static final long NAME_MAPPING_BASE_BYTES = 64L;
    // Rounded-up structural overheads shared by all estimators.
    private static final long STRING_OBJECT_BYTES = 48L;
    private static final long ARRAY_BASE_BYTES = 24L;
    private static final long OBJECT_REFERENCE_BYTES = 8L;
    private static final long HASH_MAP_OBJECT_BYTES = 64L;
    private static final long HASH_MAP_ENTRY_BYTES = 64L;

    private MetaCacheWeightUtils() {
    }

    /**
     * Approximate retained bytes of a String: object plus backing array, one byte per Latin-1
     * character and two bytes otherwise. Determined by scanning the value once, so callers pay
     * O(length) exactly like the loader that produced the string.
     */
    public static long estimatedStringBytes(String value) {
        if (value == null) {
            return 0L;
        }
        return saturatedAdd(STRING_OBJECT_BYTES, estimatedStringPayloadBytes(value));
    }

    /** Character payload of a String without the object/array overhead. */
    public static long estimatedStringPayloadBytes(String value) {
        if (value == null) {
            return 0L;
        }
        return saturatedMultiply(value.length(), isLatin1(value) ? 1L : 2L);
    }

    /** Approximate retained character data of any CharSequence. */
    public static long estimatedCharSequenceBytes(CharSequence value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof String) {
            return estimatedStringBytes((String) value);
        }
        return saturatedAdd(STRING_OBJECT_BYTES,
                saturatedMultiply(value.length(), 2L));
    }

    /** Approximate size of a retained byte array. */
    public static long estimatedByteArrayBytes(long length) {
        if (length < 0L) {
            return Long.MAX_VALUE;
        }
        return saturatedAdd(ARRAY_BASE_BYTES, length);
    }

    /** Approximate size of an object-reference array. */
    public static long estimatedObjectArrayBytes(long length) {
        if (length < 0L) {
            return Long.MAX_VALUE;
        }
        return saturatedAdd(ARRAY_BASE_BYTES,
                saturatedMultiply(length, OBJECT_REFERENCE_BYTES));
    }

    /** Incremental payload of an int array whose header is accounted elsewhere. */
    public static long estimatedIntArrayPayloadBytes(long length) {
        if (length < 0L) {
            return Long.MAX_VALUE;
        }
        return saturatedMultiply(length, Integer.BYTES);
    }

    /**
     * Approximate size of a java.util.HashMap holding {@code entries} mappings: the map object,
     * its table and one node per entry; keys and values are charged separately.
     */
    public static long estimatedHashMapBytes(long entries) {
        if (entries <= 0L) {
            return HASH_MAP_OBJECT_BYTES;
        }
        return saturatedAdd(HASH_MAP_OBJECT_BYTES,
                saturatedMultiply(entries, HASH_MAP_ENTRY_BYTES));
    }

    /**
     * Pass-through for rounded structural constants. Historic call sites scaled a
     * compressed-reference constant to the active VM layout; the constants are now rounded up
     * far enough to cover any supported layout, so no adjustment is applied.
     */
    public static long estimatedObjectBytes(long approximateBytes) {
        return approximateBytes < 0L ? Long.MAX_VALUE : approximateBytes;
    }

    /** Estimate the fixed set of names retained by a cache key. */
    public static long estimatedNameMappingBytes(NameMapping nameMapping) {
        if (nameMapping == null) {
            return 0L;
        }
        long bytes = NAME_MAPPING_BASE_BYTES;
        bytes = saturatedAdd(bytes, estimatedStringBytes(nameMapping.getLocalDbName()));
        bytes = saturatedAdd(bytes, estimatedStringBytes(nameMapping.getLocalTblName()));
        bytes = saturatedAdd(bytes, estimatedStringBytes(nameMapping.getRemoteDbName()));
        return saturatedAdd(bytes, estimatedStringBytes(nameMapping.getRemoteTblName()));
    }

    public static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    public static long saturatedMultiply(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    private static boolean isLatin1(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) > 0xFF) {
                return false;
            }
        }
        return true;
    }
}
