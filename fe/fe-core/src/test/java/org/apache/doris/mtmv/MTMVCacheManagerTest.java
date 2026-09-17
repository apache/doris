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

import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mtmv.MTMVCacheManager.HotEntry;
import org.apache.doris.mtmv.MTMVCacheManager.Key;
import org.apache.doris.mtmv.MTMVCacheManager.Snapshot;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MTMVCacheManagerTest {

    @Test
    public void testPutGetInvalidate() {
        MTMVCacheManager manager = new MTMVCacheManager();
        MTMVCache cacheGuarded = Mockito.mock(MTMVCache.class);
        MTMVCache cacheUnguarded = Mockito.mock(MTMVCache.class);
        manager.put(1L, true, cacheGuarded);
        manager.put(1L, false, cacheUnguarded);
        Assertions.assertSame(cacheGuarded, manager.getIfPresent(1L, true));
        Assertions.assertSame(cacheUnguarded, manager.getIfPresent(1L, false));
        Assertions.assertEquals(2L, manager.size());

        manager.invalidate(1L);
        Assertions.assertNull(manager.getIfPresent(1L, true));
        Assertions.assertNull(manager.getIfPresent(1L, false));
        Assertions.assertEquals(0L, manager.size());
    }

    @Test
    public void testPutRejectsNull() {
        MTMVCacheManager manager = new MTMVCacheManager();
        Assertions.assertThrows(NullPointerException.class, () -> manager.put(1L, true, null));
    }

    @Test
    public void testDifferentMtmvsAreIndependent() {
        MTMVCacheManager manager = new MTMVCacheManager();
        MTMVCache c1 = Mockito.mock(MTMVCache.class);
        MTMVCache c2 = Mockito.mock(MTMVCache.class);
        manager.put(1L, true, c1);
        manager.put(2L, true, c2);
        manager.invalidate(1L);
        Assertions.assertNull(manager.getIfPresent(1L, true));
        Assertions.assertSame(c2, manager.getIfPresent(2L, true));
    }

    @Test
    public void testSnapshotReportsHitAndMiss() {
        MTMVCacheManager manager = new MTMVCacheManager();
        MTMVCache c1 = Mockito.mock(MTMVCache.class);
        manager.put(1L, true, c1);
        manager.getIfPresent(1L, true);
        manager.getIfPresent(1L, false);
        Snapshot snap = manager.snapshot();
        Assertions.assertEquals(1L, snap.size);
        Assertions.assertTrue(snap.hitCount >= 1);
        Assertions.assertTrue(snap.missCount >= 1);
    }

    @Test
    public void testHotEntriesHonorsLimit() {
        MTMVCacheManager manager = new MTMVCacheManager();
        MTMVCache c = Mockito.mock(MTMVCache.class);
        for (int i = 0; i < 5; i++) {
            manager.put(i, true, c);
        }
        List<HotEntry> hot = manager.hotEntries(3);
        Assertions.assertEquals(3, hot.size());
        for (HotEntry e : hot) {
            Assertions.assertTrue(e.idleMs >= 0,
                    "idleMs should be >= 0 when expireAfterAccess is set, got " + e.idleMs);
        }
    }

    @Test
    public void testHotEntriesEmptyForZeroOrNegativeLimit() {
        MTMVCacheManager manager = new MTMVCacheManager();
        MTMVCache c = Mockito.mock(MTMVCache.class);
        manager.put(1L, true, c);
        Assertions.assertTrue(manager.hotEntries(0).isEmpty());
        Assertions.assertTrue(manager.hotEntries(-1).isEmpty());
    }

    @Test
    public void testInvalidateAll() {
        MTMVCacheManager manager = new MTMVCacheManager();
        MTMVCache c = Mockito.mock(MTMVCache.class);
        manager.put(1L, true, c);
        manager.put(2L, false, c);
        manager.invalidateAll();
        Assertions.assertEquals(0L, manager.size());
    }

    // updateConfig() can swap the field between the two reads.
    @Test
    public void testSnapshotReadsOneCacheInstance() {
        int originalMaxSize = Config.mtmv_cache_manage_num;
        try {
            MTMVCacheManager manager = new MTMVCacheManager();
            Cache<Key, MTMVCache> original = mockCache();
            Mockito.when(original.estimatedSize()).thenReturn(7L);
            Mockito.when(original.asMap()).thenReturn(new ConcurrentHashMap<>());
            Mockito.when(original.stats()).thenAnswer(invocation -> {
                // The swap lands while snapshot() is between its reads.
                Config.mtmv_cache_manage_num = 0;
                manager.updateConfig();
                return CacheStats.of(3L, 1L, 0L, 0L, 0L, 2L, 0L);
            });
            Deencapsulation.setField(manager, "caches", original);

            Snapshot snap = manager.snapshot();

            Assertions.assertEquals(7L, snap.size);
            Assertions.assertEquals(3L, snap.hitCount);
            Assertions.assertEquals(1L, snap.missCount);
            Assertions.assertEquals(2L, snap.evictionCount);
        } finally {
            Config.mtmv_cache_manage_num = originalMaxSize;
        }
    }

    @Test
    public void testHotEntriesReadsOneCacheInstance() {
        int originalMaxSize = Config.mtmv_cache_manage_num;
        try {
            MTMVCacheManager manager = new MTMVCacheManager();
            Cache<Key, MTMVCache> original = mockCache();
            Policy<Key, MTMVCache> policy = mockPolicy();
            Mockito.when(policy.expireAfterAccess()).thenReturn(Optional.empty());
            Mockito.when(original.asMap()).thenReturn(
                    new ConcurrentHashMap<>(Collections.singletonMap(new Key(1L, true),
                            Mockito.mock(MTMVCache.class))));
            Mockito.when(original.policy()).thenAnswer(invocation -> {
                // Swapping to a disabled cache would leave the fresh instance empty.
                Config.mtmv_cache_manage_num = 0;
                manager.updateConfig();
                return policy;
            });
            Deencapsulation.setField(manager, "caches", original);

            List<HotEntry> hot = manager.hotEntries(10);

            Assertions.assertEquals(1, hot.size());
            Assertions.assertEquals(1L, hot.get(0).mtmvId);
        } finally {
            Config.mtmv_cache_manage_num = originalMaxSize;
        }
    }

    @Test
    public void testIsEnabledFollowsLiveMaxSize() {
        int originalMaxSize = Config.mtmv_cache_manage_num;
        try {
            Config.mtmv_cache_manage_num = 10;
            MTMVCacheManager manager = new MTMVCacheManager();
            Assertions.assertTrue(manager.isEnabled());

            Config.mtmv_cache_manage_num = 0;
            manager.updateConfig();
            Assertions.assertFalse(manager.isEnabled());
        } finally {
            Config.mtmv_cache_manage_num = originalMaxSize;
        }
    }

    @SuppressWarnings("unchecked")
    private static Cache<Key, MTMVCache> mockCache() {
        return Mockito.mock(Cache.class);
    }

    @SuppressWarnings("unchecked")
    private static Policy<Key, MTMVCache> mockPolicy() {
        return Mockito.mock(Policy.class);
    }

    @Test
    public void testZeroMaxSizeDisablesCacheInsteadOfUnbounding() {
        int originalMaxSize = Config.mtmv_cache_manage_num;
        try {
            Config.mtmv_cache_manage_num = 0;
            MTMVCacheManager manager = new MTMVCacheManager();
            for (int i = 0; i < 5; i++) {
                manager.put(i, true, Mockito.mock(MTMVCache.class));
            }
            manager.getCachesForTest().cleanUp();
            Assertions.assertEquals(0L, manager.size());
        } finally {
            Config.mtmv_cache_manage_num = originalMaxSize;
        }
    }

    @Test
    public void testUpdateConfigShrinksToNewMaxSize() {
        int originalMaxSize = Config.mtmv_cache_manage_num;
        try {
            Config.mtmv_cache_manage_num = 10;
            MTMVCacheManager manager = new MTMVCacheManager();
            for (int i = 0; i < 10; i++) {
                manager.put(i, true, Mockito.mock(MTMVCache.class));
            }
            manager.getCachesForTest().cleanUp();
            Assertions.assertEquals(10L, manager.size());

            Config.mtmv_cache_manage_num = 2;
            manager.updateConfig();
            Assertions.assertTrue(manager.size() <= 2L, "expected shrink to 2, got " + manager.size());

            Config.mtmv_cache_manage_num = 0;
            manager.updateConfig();
            Assertions.assertEquals(0L, manager.size());
            manager.put(99L, true, Mockito.mock(MTMVCache.class));
            manager.getCachesForTest().cleanUp();
            Assertions.assertEquals(0L, manager.size());
        } finally {
            Config.mtmv_cache_manage_num = originalMaxSize;
        }
    }

    @Test
    public void testKeyEqualityAndHash() {
        Key k1 = new Key(42L, true);
        Key k2 = new Key(42L, true);
        Key k3 = new Key(42L, false);
        Assertions.assertEquals(k1, k2);
        Assertions.assertEquals(k1.hashCode(), k2.hashCode());
        Assertions.assertNotEquals(k1, k3);
    }
}
