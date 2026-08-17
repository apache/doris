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
 * function's id.
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

    /**
     * The key one function's compiled classes live under, built the same way wherever it is needed.
     *
     * <p>FE's function id, not the signature. The signature is {@code name(argTypes)} with no
     * database in it, so {@code db1.f(INT)} and {@code db2.f(INT)} would share one compiled class -
     * whichever ran first, and dropping either would drop both. It is also spelled differently on
     * the two paths that reach this class: FE appends {@code "..."} for a variadic function when it
     * describes one to execute and does not when it drops one, so a variadic function's entry could
     * never be invalidated at all and DROP + CREATE went on running the old code until the BE
     * restarted. The id has neither problem: unique in the cluster, carried on every request that
     * executes the function, and freshly allocated by CREATE FUNCTION - so even a missed
     * invalidation cannot make a re-created function hit its predecessor's classes.
     *
     * <p>The signature is still the key when there is no id, which is not reachable through
     * CREATE FUNCTION (every function is given one) and keeps the old behaviour if it ever is.
     */
    static String cacheKey(long functionId, String functionSignature) {
        return functionId > 0 ? "id=" + functionId : "signature=" + functionSignature;
    }

    /** What is cached under this key, or null. A miss is ordinary; see {@link #publish}. */
    static UdfClassCache get(String functionKey) {
        return CACHES.get(functionKey);
    }

    /**
     * Publishes a freshly compiled cache, atomically.
     *
     * <p>A miss in {@link #get} is not only reachable after {@link #invalidate}: two first-time
     * loads of the same function can both miss and both compile. Whoever loses that race gets its
     * own cache closed here - it has not been handed to any executor yet, so closing its loader
     * cannot affect anyone - and must switch to the returned one.
     *
     * @return the cache actually held after this call: {@code cache} if it won, otherwise the
     *         already published one, which the caller must use instead
     */
    static UdfClassCache publish(String functionKey, UdfClassCache cache) {
        LOG.info("Cache UDF for: {}", functionKey);
        UdfClassCache existing = CACHES.putIfAbsent(functionKey, cache);
        if (existing == null) {
            return cache;
        }
        try {
            cache.close();
        } catch (Exception e) {
            LOG.warn("Failed to close redundant UdfClassCache for " + functionKey, e);
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
    static void invalidate(String functionKey) {
        UdfClassCache removed = CACHES.remove(functionKey);
        if (removed == null) {
            // Ordinary: DROP FUNCTION is broadcast to every plugin, and most functions were never
            // loaded statically in the first place.
            return;
        }
        LOG.info("Dropping cached UDF for: {}", functionKey);
        try {
            removed.close();
        } catch (Exception e) {
            LOG.warn("Failed to close UdfClassCache for " + functionKey, e);
        }
    }
}
