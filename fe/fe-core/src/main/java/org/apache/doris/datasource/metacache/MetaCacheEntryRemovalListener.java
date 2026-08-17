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

import javax.annotation.Nullable;

/**
 * Receives a value that left the entry through the normal removal path (capacity or weight
 * eviction, expiry, soft-value collection, peer reclaim, explicit invalidation). Replacements are
 * reported through {@link MetaCacheEntryReplacementListener} instead. The callback runs
 * asynchronously after the removal, so it must be fenced by the removed value itself (its
 * generation) rather than by whatever the entry currently publishes; {@code removedValue} is
 * null when the value was already collected.
 */
@FunctionalInterface
public interface MetaCacheEntryRemovalListener<K, V> {
    void onRemoval(K key, @Nullable V removedValue);
}
