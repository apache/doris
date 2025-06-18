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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/plugin/trino-hive/src/main/java/io/trino/plugin/hive/fs/TransactionScopeCachingDirectoryListerFactory.java
// and modified by Doris

package org.apache.doris.fs;

import org.apache.doris.common.EvictableCacheBuilder;
import org.apache.doris.fs.TransactionScopeCachingDirectoryLister.FetchingValueHolder;

import com.google.common.cache.Cache;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionScopeCachingDirectoryListerFactory {
    //TODO use a cache key based on Path & SchemaTableName and iterate over the cache keys
    // to deal more efficiently with cache invalidation scenarios for partitioned tables.
    // private final Optional<Cache<TransactionDirectoryListingCacheKey, FetchingValueHolder>> cache;

    private final Optional<Cache<TransactionDirectoryListingCacheKey, FetchingValueHolder>> cache;

    private final AtomicLong nextTransactionId = new AtomicLong();

    public TransactionScopeCachingDirectoryListerFactory(long maxSize) {
        if (maxSize > 0) {
            EvictableCacheBuilder<TransactionDirectoryListingCacheKey, FetchingValueHolder> cacheBuilder =
                    EvictableCacheBuilder.newBuilder()
                            .maximumWeight(maxSize)
                            .weigher((key, value) ->
                                    Math.toIntExact(value.getCacheFileCount()));
            this.cache = Optional.of(cacheBuilder.build());
        } else {
            cache = Optional.empty();
        }
    }

    public DirectoryLister get(DirectoryLister delegate) {
        return cache
                .map(cache -> (DirectoryLister) new TransactionScopeCachingDirectoryLister(delegate,
                        nextTransactionId.getAndIncrement(), cache))
                .orElse(delegate);
    }
}
