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

package org.apache.doris.connector.jdbc.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.UUID;

/**
 * Unit tests for the driver-classloader keep-alive cache in {@link JdbcConnectorClient}.
 *
 * <p>The cache holds exactly one classloader per distinct driver URL and never evicts it, so that
 * repeated catalog create/close/recreate cycles for the same driver reuse a single classloader
 * instead of building (and DriverManager-pinning) a fresh one each time. Reintroducing per-close
 * eviction reopened the Metaspace leak behind external-regression OOM 986696, so these tests exist
 * to lock the keep-alive semantics in place.
 */
class JdbcConnectorClientClassLoaderTest {

    private static URL uniqueDriverUrl() throws Exception {
        return new URL("file:///tmp/doris-test-driver-" + UUID.randomUUID() + ".jar");
    }

    @Test
    void sameDriverUrlReturnsSameCachedLoader() throws Exception {
        URL url = uniqueDriverUrl();
        ClassLoader first = JdbcConnectorClient.getOrCreateDriverClassLoader(url);
        ClassLoader second = JdbcConnectorClient.getOrCreateDriverClassLoader(url);
        Assertions.assertSame(first, second,
                "The same driver URL must resolve to the one cached classloader");
    }

    @Test
    void distinctDriverUrlsGetDistinctLoaders() throws Exception {
        ClassLoader a = JdbcConnectorClient.getOrCreateDriverClassLoader(uniqueDriverUrl());
        ClassLoader b = JdbcConnectorClient.getOrCreateDriverClassLoader(uniqueDriverUrl());
        Assertions.assertNotSame(a, b,
                "Different driver URLs must get their own classloaders");
    }

    @Test
    void churningSameDriverUrlReusesOneLoaderNoMetaspaceLeak() throws Exception {
        URL url = uniqueDriverUrl();
        int before = JdbcConnectorClient.classLoaderCacheSize();
        // Simulate many CREATE CATALOG -> DROP CATALOG -> CREATE CATALOG cycles for the same driver:
        // each cycle looks the driver classloader up again. Keep-alive means the cache (and thus the
        // number of live, DriverManager-pinned driver classloaders) grows by exactly one, not one per
        // cycle -- which is the whole point of the fix.
        ClassLoader firstLoader = JdbcConnectorClient.getOrCreateDriverClassLoader(url);
        for (int i = 0; i < 20; i++) {
            ClassLoader loader = JdbcConnectorClient.getOrCreateDriverClassLoader(url);
            Assertions.assertSame(firstLoader, loader,
                    "Every cycle for the same driver URL must reuse the first classloader");
        }
        int after = JdbcConnectorClient.classLoaderCacheSize();
        Assertions.assertEquals(before + 1, after,
                "20 create/recreate cycles for one driver URL must add exactly one cached classloader");
    }
}
