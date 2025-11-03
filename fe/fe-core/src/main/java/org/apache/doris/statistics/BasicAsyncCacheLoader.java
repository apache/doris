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

package org.apache.doris.statistics;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public abstract class BasicAsyncCacheLoader<K, V> implements AsyncCacheLoader<K, V> {

    private static final Logger LOG = LogManager.getLogger(BasicAsyncCacheLoader.class);

    @Override
    public @NonNull CompletableFuture<V> asyncLoad(
            @NonNull K key,
            @NonNull Executor executor) {
        CompletableFuture<V> future = CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            try {
                return doLoad(key);
            } finally {
                long endTime = System.currentTimeMillis();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Load async cache [{}] cost time ms:{}", key, endTime - startTime);
                }
            }
        }, executor);
        return future;
    }

    protected abstract V doLoad(K k);
}
