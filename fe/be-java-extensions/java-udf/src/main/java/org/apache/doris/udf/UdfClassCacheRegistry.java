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

package org.apache.doris.udf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What a statically loaded function compiled to, kept for the life of the process and keyed by the
 * function's signature.
 *
 * <p>Entries are inserted on first use and removed only by {@link #invalidate}, which DROP FUNCTION
 * reaches through {@code UdfExecutorFactory.invalidate}. There is deliberately no time-based
 * eviction; it used to exist and caused two things:
 *
 * <ol>
 *   <li>closing a {@code URLClassLoader} while another thread was still loading classes from it
 *       produced NoClassDefFoundError;</li>
 *   <li>rebuilding a fresh loader on every eviction left several coexisting loaders for one
 *       function, which broke lazy class resolution and reflective lookups inside user code.</li>
 * </ol>
 */
final class UdfClassCacheRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(UdfClassCacheRegistry.class);

    private static final Map<String, UdfClassCache> CACHES = new ConcurrentHashMap<>();

    private UdfClassCacheRegistry() {
    }

    /** What is cached for this signature, or null. A miss is ordinary; see {@link #publish}. */
    static UdfClassCache get(String functionSignature) {
        return CACHES.get(functionSignature);
    }

    /**
     * Publishes a freshly compiled cache, atomically.
     *
     * <p>A miss in {@link #get} is not only reachable after {@link #invalidate}: two first-time
     * loads of the same signature can both miss and both compile. Whoever loses that race gets its
     * own cache closed here - it has not been handed to any executor yet, so closing its loader
     * cannot affect anyone - and must switch to the returned one.
     *
     * @return the cache actually held after this call: {@code cache} if it won, otherwise the
     *         already published one, which the caller must use instead
     */
    static UdfClassCache publish(String functionSignature, UdfClassCache cache) {
        LOG.info("Cache UDF for: {}", functionSignature);
        UdfClassCache existing = CACHES.putIfAbsent(functionSignature, cache);
        if (existing == null) {
            return cache;
        }
        try {
            cache.close();
        } catch (Exception e) {
            LOG.warn("Failed to close redundant UdfClassCache for " + functionSignature, e);
        }
        return existing;
    }

    /**
     * Drops what was cached for one function, because it has been dropped.
     *
     * <p>The loader is closed immediately. A query still holding this cache will fail with
     * NoClassDefFoundError on its next lazy class resolution, which is the accepted meaning of DROP
     * FUNCTION: the function is gone and queries against it are expected to fail.
     */
    static void invalidate(String functionSignature) {
        UdfClassCache removed = CACHES.remove(functionSignature);
        if (removed == null) {
            // Ordinary: DROP FUNCTION is broadcast to every plugin, and most functions were never
            // loaded statically in the first place.
            return;
        }
        LOG.info("Dropping cached UDF for: {}", functionSignature);
        try {
            removed.close();
        } catch (Exception e) {
            LOG.warn("Failed to close UdfClassCache for " + functionSignature, e);
        }
    }
}
