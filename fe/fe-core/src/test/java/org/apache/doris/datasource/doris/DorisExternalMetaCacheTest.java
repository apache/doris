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

package org.apache.doris.datasource.doris;

import org.apache.doris.datasource.metacache.MetaCacheEntry;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DorisExternalMetaCacheTest {

    @Test
    public void testInvalidateBackendCacheUsesSingletonEntryKey() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DorisExternalMetaCache cache = new DorisExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            MetaCacheEntry<String, ImmutableMap<Long, org.apache.doris.system.Backend>> backendsEntry = cache.entry(
                    catalogId,
                    DorisExternalMetaCache.ENTRY_BACKENDS,
                    String.class,
                    DorisExternalMetaCacheTestSupport.backendMapClass());
            backendsEntry.put("backends", ImmutableMap.of());
            Assert.assertNotNull(backendsEntry.getIfPresent("backends"));

            cache.invalidateBackendCache(catalogId);

            Assert.assertNull(backendsEntry.getIfPresent("backends"));
        } finally {
            executor.shutdownNow();
        }
    }

    private static final class DorisExternalMetaCacheTestSupport {
        @SuppressWarnings("unchecked")
        private static Class<ImmutableMap<Long, org.apache.doris.system.Backend>> backendMapClass() {
            return (Class<ImmutableMap<Long, org.apache.doris.system.Backend>>) (Class<?>) ImmutableMap.class;
        }
    }
}
