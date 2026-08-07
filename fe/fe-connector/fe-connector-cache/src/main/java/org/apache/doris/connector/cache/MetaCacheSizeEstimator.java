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

package org.apache.doris.connector.cache;

/**
 * Estimates the retained bytes owned by one connector metadata cache entry.
 *
 * <p>Implementations should be type-specific, deterministic, non-blocking, and normally O(1) at cache admission.
 * Large object graphs should precompute their owned size when the value is built instead of being traversed again
 * whenever Caffeine inserts, replaces, refreshes, or computes an entry.
 */
@FunctionalInterface
public interface MetaCacheSizeEstimator<K, V> {
    long estimateBytes(K key, V value);
}
