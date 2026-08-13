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

/** Constant-time helpers for conservative external metadata cache weights. */
public final class MetaCacheWeightUtils {
    private static final long STRING_BASE_BYTES = 40L;
    private static final long STRING_BYTES_PER_CHARACTER = 2L;
    private static final long NAME_MAPPING_BASE_BYTES = 64L;

    private MetaCacheWeightUtils() {
    }

    /**
     * Estimate a String without inspecting its contents. Two bytes per character deliberately
     * avoids depending on CompactStrings or VM-private layout details.
     */
    public static long estimatedStringBytes(String value) {
        return estimatedCharSequenceBytes(value);
    }

    /** Estimate retained character data without materializing a String copy. */
    public static long estimatedCharSequenceBytes(CharSequence value) {
        return value == null ? 0L : saturatedAdd(
                STRING_BASE_BYTES, saturatedMultiply(value.length(), STRING_BYTES_PER_CHARACTER));
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
}
