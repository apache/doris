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
 * Caffeine-free callback invoked when one value stops being owned by the metadata cache.
 *
 * <p>This includes eviction/invalidation as well as a disabled-cache load or a load/refresh whose publication is
 * rejected by a concurrent invalidation. In the latter cases the value was never visible through the cache, and
 * the callback receives {@link MetaCacheRemovalReason#EXPLICIT} so resource-owning values can release the cache
 * reference they reserved before attempting publication.
 */
@FunctionalInterface
public interface MetaCacheRemovalListener<K, V> {
    void onRemoval(K key, V value, MetaCacheRemovalReason reason);
}
