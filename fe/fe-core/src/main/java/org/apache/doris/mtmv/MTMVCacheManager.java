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

    private final Object swapLock = new Object();
    private volatile Cache<Key, MTMVCache> caches;

    public MTMVCacheManager() {
        caches = build(Config.mtmv_cache_manage_num, Config.expire_mtmv_cache_in_fe_second);
    }

    public MTMVCache getIfPresent(long mtmvId, int guardMask) {
        return caches.getIfPresent(new Key(mtmvId, guardMask));
    }

    public void put(long mtmvId, int guardMask, MTMVCache cache) {
        Objects.requireNonNull(cache, "mtmv cache to publish must not be null");
        synchronized (swapLock) {
            caches.put(new Key(mtmvId, guardMask), cache);
        }
    }

    public void invalidate(long mtmvId) {
        synchronized (swapLock) {
            // Every guard mask is an independent entry for the same MV, so drop them all.
            caches.asMap().keySet().removeIf(key -> key.mtmvId == mtmvId);
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

    /** False when the live maximum is 0, i.e. every put would be discarded immediately. */
    public boolean isEnabled() {
        return caches.policy().eviction().map(eviction -> eviction.getMaximum() > 0).orElse(true);
    }

    public Snapshot snapshot() {
        Cache<Key, MTMVCache> current = caches;
        CacheStats s = current.stats();
        return new Snapshot(current.estimatedSize(), s.hitCount(), s.missCount(),
                s.evictionCount(), s.hitRate());
    }

    /**
     * Snapshot for SHOW PROC '/mtmv_cache/hot'. Ordered by most-recently-accessed first when
     * expireAfterAccess is enabled; falls back to iteration order with idleMs=-1 otherwise.
     */
    public List<HotEntry> hotEntries(int limit) {
        if (limit <= 0) {
            return Collections.emptyList();
        }
        Cache<Key, MTMVCache> current = caches;
        return current.policy().expireAfterAccess()
                .map(exp -> exp.youngest(stream -> stream
                        .limit(limit)
                        .map(entry -> {
                            Key k = entry.getKey();
                            long expireMs = exp.getExpiresAfter(TimeUnit.MILLISECONDS);
                            long idleMs = Math.max(expireMs - entry.expiresAfter().toMillis(), 0L);
                            return new HotEntry(k.mtmvId, k.guardMask, idleMs);
                        })
                        .collect(Collectors.toList())))
                .orElseGet(() -> current.asMap().keySet().stream()
                        .limit(limit)
                        .map(k -> new HotEntry(k.mtmvId, k.guardMask, -1L))
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
        env.getMtmvCacheManager().updateConfig();
    }

    private static Cache<Key, MTMVCache> build(int maxSize, long expireAfterAccessSeconds) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder().softValues().recordStats()
                .maximumSize(Math.max(maxSize, 0));
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
        /** The session-variable dependency families guarded in this cache, see SessionVarGuardRewriter.GUARD_*. */
        public final int guardMask;

        public Key(long mtmvId, int guardMask) {
            this.mtmvId = mtmvId;
            this.guardMask = guardMask;
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
            return mtmvId == that.mtmvId && guardMask == that.guardMask;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mtmvId, guardMask);
        }
    }

    public static final class HotEntry {
        public final long mtmvId;
        public final int guardMask;
        public final long idleMs;

        public HotEntry(long mtmvId, int guardMask, long idleMs) {
            this.mtmvId = mtmvId;
            this.guardMask = guardMask;
            this.idleMs = idleMs;
        }
    }

    public static final class Snapshot {
        public final long size;
        public final long hitCount;
        public final long missCount;
        public final long evictionCount;
        public final double hitRate;

        public Snapshot(long size, long hitCount, long missCount, long evictionCount, double hitRate) {
            this.size = size;
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.evictionCount = evictionCount;
            this.hitRate = hitRate;
        }
    }

    @VisibleForTesting
    public Cache<Key, MTMVCache> getCachesForTest() {
        return caches;
    }
}
