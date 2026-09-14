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

import java.util.Objects;

/**
 * Immutable result value returned by {@link MetaCacheSizeEstimator}; this class is not an
 * estimator implementation. An incomplete result carries no usable byte count and must fail
 * cache admission closed.
 */
public final class MetaCacheSizeEstimate {
    private final long bytes;
    private final boolean complete;
    private final String incompleteReason;

    private MetaCacheSizeEstimate(long bytes, boolean complete, String incompleteReason) {
        this.bytes = bytes;
        this.complete = complete;
        this.incompleteReason = Objects.requireNonNull(incompleteReason, "incompleteReason");
    }

    public static MetaCacheSizeEstimate complete(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("cache size estimate can not be negative: " + bytes);
        }
        return new MetaCacheSizeEstimate(bytes, true, "");
    }

    public static MetaCacheSizeEstimate incomplete(String reason) {
        String safeReason = Objects.requireNonNull(reason, "reason").trim();
        if (safeReason.isEmpty()) {
            throw new IllegalArgumentException("incomplete cache size estimate requires a reason");
        }
        return new MetaCacheSizeEstimate(0L, false, safeReason);
    }

    public long getBytes() {
        return bytes;
    }

    public boolean isComplete() {
        return complete;
    }

    public String getIncompleteReason() {
        return incompleteReason;
    }
}
