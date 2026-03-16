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

import org.apache.doris.common.Pair;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.RemovalListener;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

/**
 * Bridge factory for legacy {@link MetaCache} users.
 */
public class LegacyMetaCacheFactory {
    private final ExecutorService refreshExecutor;

    public LegacyMetaCacheFactory(ExecutorService refreshExecutor) {
        this.refreshExecutor = refreshExecutor;
    }

    public <T> MetaCache<T> build(String name,
            OptionalLong expireAfterAccessSec, OptionalLong refreshAfterWriteSec, long maxSize,
            CacheLoader<String, List<Pair<String, String>>> namesCacheLoader,
            CacheLoader<String, Optional<T>> metaObjCacheLoader,
            RemovalListener<String, Optional<T>> removalListener) {
        return new MetaCache<>(
                name, refreshExecutor, expireAfterAccessSec, refreshAfterWriteSec,
                maxSize, namesCacheLoader, metaObjCacheLoader, removalListener);
    }
}
