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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.DefaultConfHandler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * FE-local cache manager for materialized view cache.
 */
public class MTMVCacheManager {

    // Guards updateConfig() against concurrent put/invalidate so mutations issued during
    // a swap are not lost in the retired instance and cannot resurrect an invalidated entry.
    private final Object swapLock = new Object();
    private volatile Cache<Key, MTMVCache> caches;

    public MTMVCacheManager() {
        caches = build(Config.mtmv_cache_manage_num, Config.expire_mtmv_cache_in_fe_second);
    }

    public MTMVCache getIfPresent(long mtmvId, boolean guarded) {
        return caches.getIfPresent(new Key(mtmvId, guarded));
    }

    public void put(long mtmvId, boolean guarded, MTMVCache cache) {
        if (cache == null) {
            return;
        }
        synchronized (swapLock) {
            caches.put(new Key(mtmvId, guarded), cache);
        }
    }

    public void invalidate(long mtmvId) {
        synchronized (swapLock) {
            caches.invalidate(new Key(mtmvId, true));
            caches.invalidate(new Key(mtmvId, false));
        }
    }

    public void invalidateAll() {
        synchronized (swapLock) {
            caches.invalidateAll();
        }
    }

    public long size() {
        return caches.estimatedSize();
    }

    public Snapshot snapshot() {
        CacheStats s = caches.stats();
        return new Snapshot(caches.estimatedSize(), s.hitCount(), s.missCount(),
                s.evictionCount(), s.loadFailureCount(), s.hitRate());
    }

    /**
     * Snapshot for SHOW PROC '/mtmv_cache/hot'. Ordered by most-recently-accessed first when
     * expireAfterAccess is enabled; falls back to iteration order with idleMs=-1 otherwise.
     */
    public List<HotEntry> hotEntries(int limit) {
        if (limit <= 0) {
            return Collections.emptyList();
        }
        return caches.policy().expireAfterAccess()
                .map(exp -> exp.youngest(limit).keySet().stream()
                        .map(k -> new HotEntry(k.mtmvId, k.guarded,
                                exp.ageOf(k, TimeUnit.MILLISECONDS).orElse(-1L)))
                        .collect(Collectors.toList()))
                .orElseGet(() -> caches.asMap().keySet().stream()
                        .limit(limit)
                        .map(k -> new HotEntry(k.mtmvId, k.guarded, -1L))
                        .collect(Collectors.toList()));
    }

    public void updateConfig() {
        Cache<Key, MTMVCache> fresh = build(Config.mtmv_cache_manage_num, Config.expire_mtmv_cache_in_fe_second);
        synchronized (swapLock) {
            fresh.putAll(caches.asMap());
            fresh.cleanUp();
            caches = fresh;
        }
    }

    public static synchronized void reloadConfig() {
        Env env = Env.getCurrentEnv();
        if (env == null) {
            return;
        }
        MTMVCacheManager manager = env.getMtmvCacheManager();
        if (manager == null) {
            return;
        }
        manager.updateConfig();
    }

    private static Cache<Key, MTMVCache> build(int maxSize, long expireAfterAccessSeconds) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder().softValues().recordStats();
        if (maxSize > 0) {
            builder.maximumSize(maxSize);
        }
        if (expireAfterAccessSeconds > 0) {
            builder.expireAfterAccess(Duration.ofSeconds(expireAfterAccessSeconds));
        }
        return builder.build();
    }

    // NOTE: referenced by Config.mtmv_cache_manage_num.callbackClassString and
    // Config.expire_mtmv_cache_in_fe_second.callbackClassString.
    public static class UpdateConfig extends DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            MTMVCacheManager.reloadConfig();
        }
    }

    /** Stable composite key so it is immune to BaseTableInfo hashCode drift. */
    public static final class Key {
        public final long mtmvId;
        public final boolean guarded;

        public Key(long mtmvId, boolean guarded) {
            this.mtmvId = mtmvId;
            this.guarded = guarded;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Key that = (Key) o;
            return mtmvId == that.mtmvId && guarded == that.guarded;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mtmvId, guarded);
        }
    }

    public static final class HotEntry {
        public final long mtmvId;
        public final boolean guarded;
        public final long idleMs;

        public HotEntry(long mtmvId, boolean guarded, long idleMs) {
            this.mtmvId = mtmvId;
            this.guarded = guarded;
            this.idleMs = idleMs;
        }
    }

    public static final class Snapshot {
        public final long size;
        public final long hitCount;
        public final long missCount;
        public final long evictionCount;
        public final long loadFailureCount;
        public final double hitRate;

        public Snapshot(long size, long hitCount, long missCount, long evictionCount,
                long loadFailureCount, double hitRate) {
            this.size = size;
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.evictionCount = evictionCount;
            this.loadFailureCount = loadFailureCount;
            this.hitRate = hitRate;
        }
    }

    @VisibleForTesting
    public Cache<Key, MTMVCache> getCachesForTest() {
        return caches;
    }
}
