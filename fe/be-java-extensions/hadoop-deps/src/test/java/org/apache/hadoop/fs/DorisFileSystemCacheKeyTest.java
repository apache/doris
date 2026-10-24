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
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

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
    public void testPerSchemeKeyWins() throws IOException {
        // The per-scheme property is what FE actually writes: a Configuration merged from several
        // storage definitions carries one entry per scheme, so each scheme must read its own.
        Configuration confA = new Configuration(false);
        confA.set("doris.fs.cache.key.file", "per-scheme-a");
        confA.set("doris.fs.cache.key", "generic-shared");
        Configuration confB = new Configuration(false);
        confB.set("doris.fs.cache.key.file", "per-scheme-b");
        confB.set("doris.fs.cache.key", "generic-shared");

        // Same generic key, different per-scheme key -> must NOT share.
        Assertions.assertNotSame(FileSystem.get(LOCAL, confA), FileSystem.get(LOCAL, confB));
    }

    @Test
    public void testUnrelatedSchemeKeyIsIgnored() throws IOException {
        // A key published for another scheme (the HDFS half of a mixed catalog) must not affect
        // how file:// is cached -- that is the whole point of not sharing one property name.
        Configuration conf1 = new Configuration(false);
        conf1.set("doris.fs.cache.key.file", "same");
        conf1.set("doris.fs.cache.key.hdfs", "hdfs-creds-1");
        Configuration conf2 = new Configuration(false);
        conf2.set("doris.fs.cache.key.file", "same");
        conf2.set("doris.fs.cache.key.hdfs", "hdfs-creds-2");

        Assertions.assertSame(FileSystem.get(LOCAL, conf1), FileSystem.get(LOCAL, conf2));
    }

    @Test
    public void testGenericKeyStillHonoredWhenNoPerSchemeEntry() throws IOException {
        Configuration confA = new Configuration(false);
        confA.set("doris.fs.cache.key", "generic-a");
        Configuration confB = new Configuration(false);
        confB.set("doris.fs.cache.key", "generic-b");

        Assertions.assertNotSame(FileSystem.get(LOCAL, confA), FileSystem.get(LOCAL, confB));
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

    @Test
    public void testServiceScanIgnoresTheThreadContextClassLoader() throws Exception {
        // SERVICE_FILE_SYSTEMS is static and latches after the first scan, and FE resolves
        // org.apache.hadoop.* parent-first so that this class -- and therefore that static --
        // is shared by the kernel and every plugin. Under the vanilla no-arg ServiceLoader.load
        // the scan would follow whichever thread got there first, and Doris routinely pins the
        // context loader to a plugin around provider calls. A plugin classloader is
        // child-exclusive for resources, so one bundling hadoop-common without hadoop-hdfs-client
        // (the hive connector) would freeze the registry with no hdfs entry and break hdfs://
        // for the whole process. The DORIS-PATCH binds the scan to this class's own loader.
        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        Field loadedFlag = FileSystem.class.getDeclaredField("FILE_SYSTEMS_LOADED");
        loadedFlag.setAccessible(true);
        Field registryField = FileSystem.class.getDeclaredField("SERVICE_FILE_SYSTEMS");
        registryField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Class<? extends FileSystem>> registry =
                (Map<String, Class<? extends FileSystem>>) registryField.get(null);
        Map<String, Class<? extends FileSystem>> snapshot = new HashMap<>(registry);
        boolean wasLoaded = (boolean) loadedFlag.get(null);
        try {
            // A loader that can serve no service file at all: on vanilla this is what the scan
            // would see, and har:// would then be unresolvable.
            Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[0], null));
            registry.clear();
            loadedFlag.set(null, false);

            // har has no fs.har.impl default, so it can only come from the service scan.
            Assertions.assertEquals(HarFileSystem.class,
                    FileSystem.getFileSystemClass("har", new Configuration(false)));
        } finally {
            Thread.currentThread().setContextClassLoader(saved);
            registry.clear();
            registry.putAll(snapshot);
            loadedFlag.set(null, wasLoaded);
        }
    }
    @Test
    public void testTvfPolicyIsIndependentFromUserCacheFingerprints() throws IOException {
        Configuration plain = new Configuration(false);
        Configuration first = new Configuration(false);
        first.set(FileSystem.TvfGuard.POLICY_KEY, "first-policy");
        Configuration second = new Configuration(false);
        second.set(FileSystem.TvfGuard.POLICY_KEY, "second-policy");
        first.set("doris.fs.cache.key", "same-user-value");
        second.set("doris.fs.cache.key", "same-user-value");
        FileSystem.Cache.Key a = new FileSystem.Cache.Key(LOCAL, first);
        FileSystem.Cache.Key b = new FileSystem.Cache.Key(LOCAL, second);
        Assertions.assertNotEquals(a, b);
        Assertions.assertNotEquals(a, new FileSystem.Cache.Key(LOCAL, plain));
    }

    @Test
    public void testGuardedViewFsAuthorityCaseIsAnExactCacheDimension() throws IOException {
        Configuration guarded = new Configuration(false);
        guarded.set(FileSystem.TvfGuard.POLICY_KEY, "one-policy-with-both-tables");
        Assertions.assertNotEquals(new FileSystem.Cache.Key(URI.create("viewfs://Cluster"), guarded),
                new FileSystem.Cache.Key(URI.create("viewfs://cluster"), guarded));
        Configuration plain = new Configuration(false);
        Assertions.assertEquals(new FileSystem.Cache.Key(URI.create("viewfs://Cluster"), plain),
                new FileSystem.Cache.Key(URI.create("viewfs://cluster"), plain));
    }


    @Test
    public void testActualSnapshotProfilePartitionsCacheBeforeLookup() throws IOException {
        // A credential helper may initialize local FS while its guarded data route is remote.
        Configuration first = nativeRequest();
        first.set("remote.cluster.setting", "first");
        Configuration second = nativeRequest();
        second.set("remote.cluster.setting", "second");
        FileSystem a = FileSystem.get(LOCAL, first);
        FileSystem b = FileSystem.get(LOCAL, second);
        Assertions.assertNotSame(a, b);
        Assertions.assertNotEquals(a.getConf().getRaw(FileSystem.TvfGuard.CONTEXT_KEY),
                b.getConf().getRaw(FileSystem.TvfGuard.CONTEXT_KEY));
        Configuration nextPath = new Configuration(first);
        nextPath.set(FileSystem.TvfGuard.PATH_KEY, "/data/next.csv");
        Assertions.assertSame(a, FileSystem.get(LOCAL, nextPath));
        FileSystem.TvfGuard.clearNativeContext();
    }

    @Test
    public void testRawConfigurationCodeUnitsRemainDistinctCacheProfiles() throws IOException {
        java.util.Set<FileSystem> instances = new java.util.HashSet<>();
        for (String value : new String[] {"?", String.valueOf((char) 0xD800), String.valueOf((char) 0xDC00)}) {
            Configuration conf = nativeRequest();
            conf.set("unrelated.raw.setting", value);
            FileSystem fs = FileSystem.get(LOCAL, conf);
            Assertions.assertEquals(value, fs.getConf().getRaw("unrelated.raw.setting"));
            Assertions.assertTrue(instances.add(fs), "different raw settings must not reuse a guarded filesystem");
        }
        FileSystem.TvfGuard.clearNativeContext();
    }

    private static Configuration nativeRequest() {
        Configuration conf = new Configuration(false);
        conf.set(FileSystem.TvfGuard.POLICY_KEY, FileSystem.TvfGuard.NATIVE_POLICY);
        conf.set(FileSystem.TvfGuard.ROOT_KEY, "hdfs://runtime");
        conf.set(FileSystem.TvfGuard.PATH_KEY, "/data/file.csv");
        return conf;
    }

}
