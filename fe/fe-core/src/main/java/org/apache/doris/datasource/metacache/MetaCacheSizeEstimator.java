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
import java.util.function.Supplier;

/**
 * Supplies the admission weight of one key/value pair.
 *
 * <p>The callback runs after load and before admission. Implementations must use already available
 * shape counters and constant-time collection sizes; they must not walk object graphs, perform IO,
 * materialize lazy SDK state, or copy payloads. Caffeine's weigher reads only the admitted
 * reservation record, so cache hits and eviction remain O(1).
 */
@FunctionalInterface
public interface MetaCacheSizeEstimator<K, V> {
    MetaCacheSizeEstimate estimate(K key, V value);

    /** Convert preparation failures into fail-closed incomplete estimates. */
    static MetaCacheSizeEstimate estimateSafely(
            String failureReason, Supplier<MetaCacheSizeEstimate> estimation) {
        Objects.requireNonNull(failureReason, "failureReason");
        Objects.requireNonNull(estimation, "estimation");
        try {
            return Objects.requireNonNull(estimation.get(), "size estimate");
        } catch (RuntimeException e) {
            return MetaCacheSizeEstimate.incomplete(failureReason + ":" + e.getClass().getName());
        }
    }
}
