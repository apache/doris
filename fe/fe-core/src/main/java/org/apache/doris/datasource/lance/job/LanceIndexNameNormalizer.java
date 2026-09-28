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

package org.apache.doris.datasource.lance.job;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Logical index name normalization v1, the only definition: the UTF-8 result
 * of Java {@code toLowerCase(Locale.ROOT)}. Both the display name and the
 * normalized bytes are persisted on the job; Doris preserves display case,
 * rejects new case-only duplicates, and fails mutation on ambiguous external
 * case-only collisions. No normalization migration or mixed-version fence
 * protocol exists.
 */
public final class LanceIndexNameNormalizer {
    /** Bound on the persisted logical index name, aligned with the external string bound. */
    public static final int MAX_INDEX_NAME_BYTES = 1024;

    private LanceIndexNameNormalizer() {
    }

    /**
     * Normalization v1. The result is the identity bytes of the same-name fence key.
     */
    public static String normalize(String displayName) {
        if (displayName == null) {
            throw new IllegalArgumentException("index name must not be null");
        }
        return displayName.toLowerCase(Locale.ROOT);
    }

    /**
     * True when two display names differ only by case under normalization v1.
     */
    public static boolean isCaseOnlyDuplicate(String displayA, String displayB) {
        if (displayA == null || displayB == null) {
            return false;
        }
        return !displayA.equals(displayB) && normalize(displayA).equals(normalize(displayB));
    }

    /**
     * Validate the persisted display name: non-empty and within the UTF-8 byte bound.
     */
    public static void validateDisplayName(String displayName) {
        if (displayName == null || displayName.isEmpty()) {
            throw new IllegalArgumentException("index name must not be null or empty");
        }
        if (displayName.getBytes(StandardCharsets.UTF_8).length > MAX_INDEX_NAME_BYTES) {
            throw new IllegalArgumentException("index name exceeds " + MAX_INDEX_NAME_BYTES + " UTF-8 bytes");
        }
    }
}
