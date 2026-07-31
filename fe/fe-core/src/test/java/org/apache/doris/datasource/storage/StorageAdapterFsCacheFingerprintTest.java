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

package org.apache.doris.datasource.storage;

import org.apache.doris.filesystem.properties.FsCacheKeys;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class StorageAdapterFsCacheFingerprintTest {

    private static final String HDFS_KEY = FsCacheKeys.fsCacheKeyProperty("hdfs");
    private static final String S3A_KEY = FsCacheKeys.fsCacheKeyProperty("s3a");

    private static StorageAdapter hdfs(String user) {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "hdfs://test/1.orc");
        props.put("hadoop.username", user);
        return StorageAdapter.of(props);
    }

    private static StorageAdapter s3(String accessKey) {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        props.put("s3.access_key", accessKey);
        props.put("s3.secret_key", "secret");
        return StorageAdapter.of(props);
    }

    @Test
    public void testFingerprintStableForSameDefinition() {
        Assertions.assertEquals(hdfs("userA").getFsCacheFingerprint(), hdfs("userA").getFsCacheFingerprint());
    }

    @Test
    public void testFingerprintDiffersAcrossCredentials() {
        Assertions.assertNotEquals(hdfs("userA").getFsCacheFingerprint(), hdfs("userB").getFsCacheFingerprint());
    }

    @Test
    public void testRawHadoopOverrideChangesTheHdfsFingerprint() {
        // Every consumer finishes its Configuration by overlaying the catalog's raw fs./dfs./hadoop.
        // keys on top of the storage's derived map (IcebergCatalogFactory.buildHadoopConfiguration,
        // HudiScanPlanProvider.buildHadoopConf, HdfsProperties.extractUserOverriddenHdfsConfig), and
        // none of them is a bound alias. Two definitions pointing the same nameservice at different
        // namenodes must therefore not share a cached FileSystem.
        Map<String, String> nn1 = new HashMap<>();
        nn1.put("uri", "hdfs://ns/1.orc");
        nn1.put("hadoop.username", "userA");
        nn1.put("dfs.namenode.rpc-address.ns.nn1", "hostA:8020");

        Map<String, String> nn2 = new HashMap<>(nn1);
        nn2.put("dfs.namenode.rpc-address.ns.nn1", "hostB:8020");

        Assertions.assertNotEquals(StorageAdapter.of(nn1).getFsCacheFingerprint(),
                StorageAdapter.of(nn2).getFsCacheFingerprint());
        // Same definition twice still hits the cache: the fingerprint stays a pure function.
        Assertions.assertEquals(StorageAdapter.of(nn1).getFsCacheFingerprint(),
                StorageAdapter.of(new HashMap<>(nn1)).getFsCacheFingerprint());
    }

    @Test
    public void testRawHadoopCredentialOverrideChangesTheS3Fingerprint() {
        // The S3 arm has no raw passthrough of its own — the connectors overlay fs.s3a.* onto the
        // Configuration afterwards — so a definition carrying its credentials that way is
        // indistinguishable by the bound aliases alone.
        Map<String, String> ak1 = new HashMap<>();
        ak1.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        ak1.put("fs.s3a.access.key", "ak1");
        ak1.put("fs.s3a.secret.key", "sk1");

        Map<String, String> ak2 = new HashMap<>(ak1);
        ak2.put("fs.s3a.access.key", "ak2");

        Assertions.assertNotEquals(StorageAdapter.of(ak1).getFsCacheFingerprint(),
                StorageAdapter.of(ak2).getFsCacheFingerprint());
    }

    @Test
    public void testSmuggledSeparatorsCannotForgeAnotherDefinitionsFingerprint() {
        // The identity is hashed with length-framed entries, not a "\nkey=value" join: names and
        // values are both user-supplied, so a single value carrying embedded newlines could
        // otherwise reproduce the encoding of several separate ones. Hadoop ignores fs.ignored
        // (one opaque multiline value) but honors the explicit fs.s3a.* credentials below, so the
        // two definitions open different clients and must not collide on one cache entry.
        Map<String, String> smuggled = new HashMap<>();
        smuggled.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        smuggled.put("fs.ignored", "\nfs.s3a.access.key=AK\nfs.s3a.secret.key=SK");

        Map<String, String> real = new HashMap<>();
        real.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        real.put("fs.ignored", "");
        real.put("fs.s3a.access.key", "AK");
        real.put("fs.s3a.secret.key", "SK");

        Assertions.assertNotEquals(StorageAdapter.of(smuggled).getFsCacheFingerprint(),
                StorageAdapter.of(real).getFsCacheFingerprint());
    }

    @Test
    public void testUnrelatedPropertiesDoNotChangeTheFingerprint() {
        // The identity is confined to what configures a FileSystem. Anything else sharing the
        // property map (table/format options here) must keep the cache hit.
        Map<String, String> base = new HashMap<>();
        base.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        base.put("s3.access_key", "ak");
        base.put("s3.secret_key", "sk");

        Map<String, String> withExtras = new HashMap<>(base);
        withExtras.put("column_separator", ",");
        withExtras.put("format", "parquet");

        Assertions.assertEquals(StorageAdapter.of(base).getFsCacheFingerprint(),
                StorageAdapter.of(withExtras).getFsCacheFingerprint());
    }

    @Test
    public void testBackendConfigCarriesPerSchemeFingerprint() {
        StorageAdapter sp = hdfs("userA");
        Map<String, String> beProps = sp.getBackendConfigProperties();
        Assertions.assertEquals(sp.getFsCacheFingerprint(), beProps.get(HDFS_KEY));
        // viewfs is the HDFS provider's other addressable scheme; it must carry the same value.
        Assertions.assertEquals(sp.getFsCacheFingerprint(),
                beProps.get(FsCacheKeys.fsCacheKeyProperty("viewfs")));
        // Never the shared, scheme-less name: that is what makes merging lossless.
        Assertions.assertNull(beProps.get(FsCacheKeys.FS_CACHE_KEY_PROPERTY));
    }

    @Test
    public void testS3FingerprintIsPublishedUnderTheSchemeItIsOpenedWith() {
        // Doris normalizes cos://, oss:// and friends to s3://, so a fingerprint published only
        // under the dialect's own scheme would never be read back.
        Map<String, String> beProps = s3("ak1").getBackendConfigProperties();
        Assertions.assertEquals(s3("ak1").getFsCacheFingerprint(), beProps.get(S3A_KEY));
        Assertions.assertEquals(s3("ak1").getFsCacheFingerprint(),
                beProps.get(FsCacheKeys.fsCacheKeyProperty("s3")));
    }

    @Test
    public void testMergingTwoStoragesKeepsBothFingerprints() {
        // The reason the key is per scheme: every consumer merges storages with putAll, and no
        // merge site is expected to know about this mechanism.
        StorageAdapter hdfsStorage = hdfs("userA");
        StorageAdapter s3Storage = s3("ak1");

        Map<String, String> merged = new HashMap<>();
        merged.putAll(hdfsStorage.getBackendConfigProperties());
        merged.putAll(s3Storage.getBackendConfigProperties());

        Assertions.assertEquals(hdfsStorage.getFsCacheFingerprint(), merged.get(HDFS_KEY));
        Assertions.assertEquals(s3Storage.getFsCacheFingerprint(), merged.get(S3A_KEY));

        // Changing only the S3 credentials must not disturb the HDFS entry, and vice versa —
        // under one shared key this is exactly the case that used to collapse.
        Map<String, String> other = new HashMap<>();
        other.putAll(hdfsStorage.getBackendConfigProperties());
        other.putAll(s3("ak2").getBackendConfigProperties());
        Assertions.assertEquals(merged.get(HDFS_KEY), other.get(HDFS_KEY));
        Assertions.assertNotEquals(merged.get(S3A_KEY), other.get(S3A_KEY));
    }

    @Test
    public void testBackendConfigIsADefensiveCopy() {
        // OutFileClause used to inject fs.defaultFS by mutating this map; the copy keeps such a
        // caller from poisoning the adapter's cached map (and, for Broker/Local/Http, its raw props).
        StorageAdapter sp = hdfs("userA");
        sp.getBackendConfigProperties().put("injected.key", "injected");
        Assertions.assertFalse(sp.getBackendConfigProperties().containsKey("injected.key"));
    }

    @Test
    public void testNoBlanketDisableCacheByDefault() {
        // The patched FileSystem keys its cache by doris.fs.cache.key.<scheme>, so Doris no longer
        // forces fs.<schema>.impl.disable.cache anywhere in the BE-bound map.
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        Map<String, String> beProps = StorageAdapter.of(props).getBackendConfigProperties();
        Assertions.assertNull(beProps.get("fs.s3a.impl.disable.cache"));
        Assertions.assertNull(beProps.get("fs.s3.impl.disable.cache"));
        Assertions.assertNotNull(beProps.get(S3A_KEY));
    }

    @Test
    public void testPatchedFileSystemShadowIsActive() throws Exception {
        // fe-core depends on hadoop-deps, whose jar ships a patched org.apache.hadoop.fs.FileSystem
        // and is loaded ahead of hadoop-common (declared first in the pom; start_fe.sh prepends it
        // at runtime). The fingerprint only isolates credentials if that patched Cache.Key wins.
        Class<?> keyClass = Class.forName("org.apache.hadoop.fs.FileSystem$Cache$Key");
        Assertions.assertDoesNotThrow(() -> keyClass.getDeclaredField("dorisCacheKey"),
                "patched FileSystem.Cache.Key (DORIS-PATCH) is not on the FE classpath");
    }
}
