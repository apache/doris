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

package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

/**
 * Verifies the DORIS-PATCH in this module's shadowed {@link FileSystem}:
 * {@code doris.fs.cache.key} participates in the FileSystem.CACHE key so that
 * catalogs/TVFs with different credentials never share a cached instance.
 */
public class DorisFileSystemCacheKeyTest {

    private static final URI LOCAL = URI.create("file:///");

    @AfterEach
    public void cleanup() throws IOException {
        FileSystem.closeAll();
    }

    @Test
    public void testPatchedKeyClassIsLoaded() throws Exception {
        // If the vanilla hadoop-common FileSystem shadowed this module's copy,
        // fail loudly instead of letting the behavior tests mislead.
        Class<?> keyClass = Class.forName("org.apache.hadoop.fs.FileSystem$Cache$Key");
        Assertions.assertDoesNotThrow(() -> keyClass.getDeclaredField("dorisCacheKey"),
                "patched FileSystem.Cache.Key (DORIS-PATCH) is not on the classpath");
    }

    @Test
    public void testDefaultBehaviorUnchanged() throws IOException {
        // Without doris.fs.cache.key, caching must behave exactly like vanilla hadoop:
        // same URI + same UGI -> same instance.
        FileSystem fs1 = FileSystem.get(LOCAL, new Configuration(false));
        FileSystem fs2 = FileSystem.get(LOCAL, new Configuration(false));
        Assertions.assertSame(fs1, fs2);
    }

    @Test
    public void testDifferentCacheKeysGetDistinctInstances() throws IOException {
        Configuration confA = new Configuration(false);
        confA.set("doris.fs.cache.key", "fingerprint-catalog-a");
        Configuration confB = new Configuration(false);
        confB.set("doris.fs.cache.key", "fingerprint-catalog-b");

        FileSystem fsDefault = FileSystem.get(LOCAL, new Configuration(false));
        FileSystem fsA = FileSystem.get(LOCAL, confA);
        FileSystem fsB = FileSystem.get(LOCAL, confB);

        Assertions.assertNotSame(fsA, fsB);
        Assertions.assertNotSame(fsDefault, fsA);
        Assertions.assertNotSame(fsDefault, fsB);
    }

    @Test
    public void testSameCacheKeySharesInstance() throws IOException {
        // Two different Configuration objects carrying the same fingerprint must
        // still hit the same cache entry (this is the whole point: identity of the
        // conf object must not matter, only the fingerprint).
        Configuration conf1 = new Configuration(false);
        conf1.set("doris.fs.cache.key", "fingerprint-catalog-a");
        Configuration conf2 = new Configuration(false);
        conf2.set("doris.fs.cache.key", "fingerprint-catalog-a");

        FileSystem fs1 = FileSystem.get(LOCAL, conf1);
        FileSystem fs2 = FileSystem.get(LOCAL, conf2);
        Assertions.assertSame(fs1, fs2);
    }
}
